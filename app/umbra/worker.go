// Package umbra provides the shadow-traffic worker used by Xray-umbra.
//
// The worker computes the umbra (shadow) byte volume as
//
//	shadow = real * (factor - 1.0)
//
// and delivers it to the client over the same connection, packed as mux.cool
// frames addressed to a session the client never opened. Stock mux clients
// discard such frames on arrival, so the shadow bytes reach the wire and the
// traffic counters without surfacing in the user's application stream.
package umbra

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/stats"
)

// Config is the common shape of the per-protocol UmbraConfig messages.
type Config interface {
	GetEnabled() bool
	GetFactor() float64
}

// Active reports whether cfg requests shadow delivery: enabled with a factor
// that produces shadow traffic.
func Active(cfg Config) bool {
	return cfg != nil && cfg.GetEnabled() && ActiveFactor(cfg.GetFactor())
}

// Gate returns a non-nil error when connections on an umbra inbound must be
// rejected: umbra is disabled, or enabled with a factor that cannot work. A
// nil config means no umbra on the inbound and allows traffic.
func Gate(cfg Config) error {
	if cfg == nil {
		return nil
	}
	if !cfg.GetEnabled() {
		return errors.New("umbra: inbound disabled")
	}
	if !ActiveFactor(cfg.GetFactor()) {
		return errors.New("umbra: factor must be greater than 1.0 and at most 2.0")
	}
	return nil
}

// MaxFactor is the largest factor accepted by config validation.
const MaxFactor = 2.0

const (
	// shadowSessionID addresses frames that stock mux clients discard on
	// arrival: session IDs are allocated from 1 upwards, so 0 is never live.
	shadowSessionID = 0
	// shadowFrameSize mirrors the mux stream chunk size.
	shadowFrameSize = 8 * 1024
)

// ShadowBytes returns the number of shadow bytes that correspond to deltaReal
// real bytes for the given factor. A factor <= 1.0 yields no shadow traffic.
func ShadowBytes(deltaReal int64, factor float64) int64 {
	if factor <= 1.0 || deltaReal <= 0 {
		return 0
	}
	return int64(math.Round(float64(deltaReal) * (factor - 1.0)))
}

// ValidFactor reports whether factor is within the allowed range (1.0-2.0).
func ValidFactor(factor float64) bool {
	return factor >= 1.0 && factor <= MaxFactor
}

// ActiveFactor reports whether factor requests shadow traffic (> 1.0).
func ActiveFactor(factor float64) bool {
	return factor > 1.0 && factor <= MaxFactor
}

// Counter counts the real bytes of a single connection, uplink and downlink
// together. It implements stats.Counter so it can be attached to the link
// wrappers used by the transport.
type Counter struct {
	value atomic.Int64
}

// Value implements stats.Counter.
func (c *Counter) Value() int64 {
	return c.value.Load()
}

// Set implements stats.Counter.
func (c *Counter) Set(v int64) int64 {
	return c.value.Swap(v)
}

// Add implements stats.Counter.
func (c *Counter) Add(v int64) int64 {
	return c.value.Add(v) - v
}

// ShadowWriter delivers shadow bytes as discardable mux frames on a writer
// shared with real mux traffic. Each frame goes out in a single
// WriteMultiBuffer call, so it interleaves atomically with real frames.
type ShadowWriter struct {
	writer buf.Writer
}

// NewShadowWriter creates a ShadowWriter writing to the given link writer.
func NewShadowWriter(writer buf.Writer) *ShadowWriter {
	return &ShadowWriter{writer: writer}
}

// Deliver writes n shadow bytes as mux frames that the client discards.
func (w *ShadowWriter) Deliver(n int64) error {
	for n > 0 {
		size := n
		if size > shadowFrameSize {
			size = shadowFrameSize
		}
		meta := mux.FrameMetadata{
			SessionID:     shadowSessionID,
			SessionStatus: mux.SessionStatusEnd,
			Option:        mux.OptionData,
		}
		frame := buf.New()
		if err := meta.WriteTo(frame); err != nil {
			frame.Release()
			return err
		}
		if _, err := serial.WriteUint16(frame, uint16(size)); err != nil {
			frame.Release()
			return err
		}
		if err := w.writer.WriteMultiBuffer(buf.MultiBuffer{frame, buf.FromBytes(make([]byte, size))}); err != nil {
			return err
		}
		n -= size
	}
	return nil
}

// LockedWriter serializes WriteMultiBuffer calls so shadow frames can share a
// response writer with real traffic without splitting a frame.
type LockedWriter struct {
	sync.Mutex
	writer buf.Writer
}

// NewLockedWriter wraps writer with a write lock.
func NewLockedWriter(writer buf.Writer) *LockedWriter {
	return &LockedWriter{writer: writer}
}

// WriteMultiBuffer implements buf.Writer.
func (w *LockedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	w.Lock()
	defer w.Unlock()
	return w.writer.WriteMultiBuffer(mb)
}

// Close implements common.Closable.
func (w *LockedWriter) Close() error {
	w.Lock()
	defer w.Unlock()
	return common.Close(w.writer)
}

// Interrupt implements common.Interruptible.
func (w *LockedWriter) Interrupt() {
	common.Interrupt(w.writer)
}

// Count wraps a link's reader and writer with the connection's real-byte
// counter and returns the pair to use for dispatch.
func Count(real *Counter, reader buf.Reader, writer buf.Writer) (buf.Reader, buf.Writer) {
	return &buf.TimeoutWrapperReader{Reader: reader, Counter: real}, &dispatcher.SizeStatWriter{Counter: real, Writer: writer}
}

// StartWorker registers the user-visible counters for one connection and runs
// the shadow worker in the background. real counts the connection's real
// bytes; inject receives the shadow frames and must be the writer that carries
// mux frames toward the client, shared with real traffic and lock-wrapped.
func StartWorker(ctx context.Context, factor float64, sm stats.Manager, real *Counter, inject buf.Writer) {
	inbound := session.InboundFromContext(ctx)
	if sm == nil || inbound == nil || inbound.User == nil || len(inbound.User.Email) == 0 {
		return
	}
	// Real uplink/downlink user counters are attached by the dispatcher's link
	// wrappers; shadow frames bypass that chain, so the downlink counter is
	// updated here to keep the user counter equal to real + shadow. The umbra
	// uplink counter is registered for panels but stays zero: shadow only
	// travels downlink.
	email := inbound.User.Email
	userDownlink, err := sm.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>downlink")
	if err != nil {
		return
	}
	if _, err := sm.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>umbra>>>uplink"); err != nil {
		return
	}
	umbraDownlink, err := sm.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>umbra>>>downlink")
	if err != nil {
		return
	}

	sink := inject
	if userDownlink != nil {
		sink = &dispatcher.SizeStatWriter{Counter: userDownlink, Writer: inject}
	}
	worker := &Worker{
		Factor:        factor,
		Real:          real,
		Writer:        NewShadowWriter(sink),
		ShadowCounter: umbraDownlink,
	}
	go Run(ctx, time.Second, worker.Tick)
}

// Worker tracks one connection's real byte volume and injects the matching
// shadow volume on every tick.
type Worker struct {
	Factor        float64
	Real          *Counter
	Writer        *ShadowWriter
	ShadowCounter stats.Counter // optional, receives the delivered shadow volume
	last          int64
}

// Tick injects the shadow volume for the real bytes accumulated since the last
// tick. It reports false when delivery failed and ticking should stop.
func (w *Worker) Tick() bool {
	current := w.Real.Value()
	delta := current - w.last
	w.last = current
	shadow := ShadowBytes(delta, w.Factor)
	if shadow <= 0 {
		return true
	}
	if err := w.Writer.Deliver(shadow); err != nil {
		return false
	}
	if w.ShadowCounter != nil {
		w.ShadowCounter.Add(shadow)
	}
	return true
}

// Run drives step on a fixed interval until ctx is cancelled or step returns
// false, which signals that the connection is down and ticking should stop.
func Run(ctx context.Context, interval time.Duration, step func() bool) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if step != nil && !step() {
				return
			}
		}
	}
}
