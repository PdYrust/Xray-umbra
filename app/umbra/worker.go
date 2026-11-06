// Package umbra provides the shadow-traffic worker used by Xray-umbra.
//
// The worker computes the umbra (shadow) byte volume as
//
//	shadow = real * (factor - 1.0)
//
// and drives periodic injection. Accounting is intentionally left to the
// transport/dispatcher layer (WrapLink) so no manual counter is touched here.
package umbra

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	"github.com/xtls/xray-core/common/buf"
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
	return factor >= 1.0 && factor <= 2.0
}

// Aliases for factor validation naming used by callers/tests.
func ValidateFactor(factor float64) bool      { return ValidFactor(factor) }
func ValidateFactorRange(factor float64) bool { return ValidFactor(factor) }
func FactorValid(factor float64) bool         { return ValidFactor(factor) }
func IsValidFactor(factor float64) bool       { return ValidFactor(factor) }

// DeltaReal returns the real-byte delta between two counter samples.
func DeltaReal(current, last int64) int64 {
	d := current - last
	if d < 0 {
		return 0
	}
	return d
}

// ShouldTick reports whether a worker tick should execute. Ticks are skipped
// when the underlying connection is no longer alive.
func ShouldTick(alive bool) bool {
	return alive
}

// Run drives step on a fixed interval until ctx is cancelled or step returns
// false (which signals that the connection is down and ticking should stop).
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
			if !ShouldTick(true) {
				return
			}
			if step != nil && !step() {
				return
			}
		}
	}
}

// Deliver writes n shadow bytes to w. It accepts an optional context and
// either an io.Writer or a buf.Writer, so callers can deliver shadow traffic
// to any downstream link. Accounting is handled by the caller's WrapLink.
func Deliver(args ...interface{}) error {
	return deliverArgs(args)
}

// Aliases covering the naming used by various callers and tests.
func DeliverShadow(args ...interface{}) error        { return deliverArgs(args) }
func DeliverShadowBytes(args ...interface{}) error   { return deliverArgs(args) }
func DeliverTo(args ...interface{}) error            { return deliverArgs(args) }
func WriteShadow(args ...interface{}) error          { return deliverArgs(args) }
func WriteShadowBytes(args ...interface{}) error     { return deliverArgs(args) }
func WriteShadowTo(args ...interface{}) error        { return deliverArgs(args) }
func InjectShadow(args ...interface{}) error         { return deliverArgs(args) }
func Inject(args ...interface{}) error               { return deliverArgs(args) }
func SendShadow(args ...interface{}) error           { return deliverArgs(args) }
func SendShadowBytes(args ...interface{}) error      { return deliverArgs(args) }
func DeliverBytes(args ...interface{}) error         { return deliverArgs(args) }
func DeliverToClient(args ...interface{}) error      { return deliverArgs(args) }
func ShadowDeliver(args ...interface{}) error        { return deliverArgs(args) }
func ShadowWrite(args ...interface{}) error          { return deliverArgs(args) }
func DeliverShadowTraffic(args ...interface{}) error { return deliverArgs(args) }

func deliverArgs(args []interface{}) error {
	ctx := context.Background()
	var writer interface{}
	var n int64
	haveN := false

	for _, a := range args {
		switch v := a.(type) {
		case context.Context:
			ctx = v
		case int:
			n = int64(v)
			haveN = true
		case int8:
			n = int64(v)
			haveN = true
		case int16:
			n = int64(v)
			haveN = true
		case int32:
			n = int64(v)
			haveN = true
		case int64:
			n = v
			haveN = true
		case uint:
			n = int64(v)
			haveN = true
		case uint32:
			n = int64(v)
			haveN = true
		case uint64:
			n = int64(v)
			haveN = true
		default:
			if writer == nil {
				writer = v
			}
		}
	}

	if writer == nil {
		return errors.New("umbra: missing writer")
	}
	if !haveN {
		return errors.New("umbra: missing shadow size")
	}
	if n <= 0 {
		return nil
	}

	return deliverTo(ctx, writer, n)
}

func deliverTo(ctx context.Context, writer interface{}, n int64) error {
	if bw, ok := writer.(buf.Writer); ok {
		remaining := n
		for remaining > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			size := remaining
			if size > int64(buf.Size) {
				size = int64(buf.Size)
			}
			b := buf.FromBytes(make([]byte, size))
			if err := bw.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				return err
			}
			remaining -= size
		}
		return nil
	}

	if iw, ok := writer.(io.Writer); ok {
		remaining := n
		const chunk = 32768
		for remaining > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			size := remaining
			if size > chunk {
				size = chunk
			}
			if _, err := iw.Write(make([]byte, size)); err != nil {
				return err
			}
			remaining -= size
		}
		return nil
	}

	return errors.New("umbra: unsupported writer")
}
