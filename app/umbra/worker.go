// Package umbra provides the shadow-traffic machinery used by Xray-umbra.
//
// For every real byte of a connection the delivery injects
//
//	shadow = real * (factor - 1.0)
//
// bytes toward the client, packed as mux.cool frames addressed to a session
// the client never opened. Stock mux clients discard such frames on arrival,
// so the shadow bytes reach the wire and the traffic counters without
// surfacing in the user's application stream. Shadow volume is metered
// proportionally to the real traffic, so the delivered ratio stays exact.
package umbra

import (
	"context"
	"math"
	"sync"
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

// MaxFactor is the largest factor accepted by config validation.
const MaxFactor = 2.0

const (
	// shadowSessionID addresses frames that stock mux clients discard on
	// arrival: session IDs are allocated from 1 upwards, so 0 is never live.
	shadowSessionID = 0
	// shadowFrameSize mirrors the mux stream chunk size.
	shadowFrameSize = 8 * 1024
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

// ValidFactor reports whether factor is within the allowed range (1.0-2.0).
func ValidFactor(factor float64) bool {
	return factor >= 1.0 && factor <= MaxFactor
}

// ActiveFactor reports whether factor requests shadow traffic (> 1.0).
func ActiveFactor(factor float64) bool {
	return factor > 1.0 && factor <= MaxFactor
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

// ShadowWriter delivers shadow bytes as discardable mux frames on a writer
// shared with real mux traffic. Each frame goes out in a single
// WriteMultiBuffer call, so it interleaves atomically with real frames. The
// delivered volume is counted per frame into Counter, so the accounting stays
// exact even when a connection closes mid-delivery.
type ShadowWriter struct {
	writer  buf.Writer
	Counter stats.Counter // optional
}

// NewShadowWriter creates a ShadowWriter writing to the given link writer.
func NewShadowWriter(writer buf.Writer, counter stats.Counter) *ShadowWriter {
	return &ShadowWriter{writer: writer, Counter: counter}
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
		if w.Counter != nil {
			w.Counter.Add(size)
		}
		n -= size
	}
	return nil
}

// Delivery meters one connection's real bytes and delivers the matching shadow
// volume. The reader and writer wrappers report every real byte they pass; the
// shadow for it is then injected to the sink immediately, so the delivered
// ratio stays at the configured factor. Shadow delivery problems never break
// the real stream: they only stop further injection.
type Delivery struct {
	factor float64
	sink   *ShadowWriter

	mu     sync.Mutex
	real   int64
	shadow int64
	dead   bool

	userDownlink  stats.Counter
	umbraDownlink stats.Counter
}

// NewDelivery creates a delivery for one connection. The user is taken from
// the inbound session so the counters match the connection's owner.
func NewDelivery(ctx context.Context, factor float64, sm stats.Manager) *Delivery {
	d := &Delivery{factor: factor}
	if sm == nil {
		return d
	}
	inbound := session.InboundFromContext(ctx)
	if inbound == nil || inbound.User == nil || len(inbound.User.Email) == 0 {
		return d
	}
	// Real uplink/downlink user counters are attached by the dispatcher's link
	// wrappers; shadow frames bypass that chain, so the downlink counter is
	// updated here to keep the user counter equal to real + shadow. The umbra
	// uplink counter is registered for panels but stays zero: shadow only
	// travels downlink.
	email := inbound.User.Email
	if c, err := sm.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>downlink"); err == nil {
		d.userDownlink = c
	}
	if _, err := sm.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>umbra>>>uplink"); err != nil {
		return d
	}
	if c, err := sm.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>umbra>>>downlink"); err == nil {
		d.umbraDownlink = c
	}
	return d
}

// SetSink attaches the writer that carries mux frames toward the client and
// flushes the shadow owed so far. It may be called late on protocols whose
// response writer is created after the request is dispatched.
func (d *Delivery) SetSink(writer buf.Writer) {
	sink := writer
	if d.userDownlink != nil {
		sink = &dispatcher.SizeStatWriter{Counter: d.userDownlink, Writer: writer}
	}
	d.mu.Lock()
	d.sink = NewShadowWriter(sink, d.umbraDownlink)
	d.mu.Unlock()
	_ = d.flush()
}

// WrapReader counts the real bytes read from the link and returns the reader
// to use for dispatch.
func (d *Delivery) WrapReader(reader buf.Reader) buf.Reader {
	return &meteredReader{reader: reader, delivery: d}
}

// WrapWriter counts the real bytes written to the link and returns the writer
// to use for dispatch.
func (d *Delivery) WrapWriter(writer buf.Writer) buf.Writer {
	return &meteredWriter{writer: writer, delivery: d}
}

func (d *Delivery) addReal(n int64) {
	d.mu.Lock()
	d.real += n
	d.mu.Unlock()
	_ = d.flush()
}

func (d *Delivery) flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sink == nil || d.dead {
		return nil
	}
	owed := int64(math.Round(float64(d.real)*(d.factor-1.0))) - d.shadow
	if owed <= 0 {
		return nil
	}
	if err := d.sink.Deliver(owed); err != nil {
		d.dead = true
		return nil
	}
	d.shadow += owed
	return nil
}

type meteredReader struct {
	reader   buf.Reader
	delivery *Delivery
}

func (r *meteredReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	if n := int64(mb.Len()); n > 0 {
		r.delivery.addReal(n)
	}
	return mb, err
}

// ReadMultiBufferTimeout implements buf.TimeoutReader so the reader stays
// usable where the dispatcher expects one.
func (r *meteredReader) ReadMultiBufferTimeout(d time.Duration) (buf.MultiBuffer, error) {
	if tr, ok := r.reader.(buf.TimeoutReader); ok {
		mb, err := tr.ReadMultiBufferTimeout(d)
		if n := int64(mb.Len()); n > 0 {
			r.delivery.addReal(n)
		}
		return mb, err
	}
	return r.ReadMultiBuffer()
}

type meteredWriter struct {
	writer   buf.Writer
	delivery *Delivery
}

func (w *meteredWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	n := int64(mb.Len())
	err := w.writer.WriteMultiBuffer(mb)
	if err == nil && n > 0 {
		w.delivery.addReal(n)
	}
	return err
}
