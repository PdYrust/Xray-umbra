package umbra

import (
	"bytes"
	"context"
	"io"
	"math"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/serial"
)

func TestShadowBytes(t *testing.T) {
	cases := []struct {
		delta  int64
		factor float64
		want   int64
	}{
		{100, 1.2, 20},
		{100, 1.0, 0},
		{100, 0.8, 0},
		{0, 1.5, 0},
		{-5, 1.5, 0},
		{3, 1.5, 2}, // 1.5 rounds to 2
	}
	for _, c := range cases {
		if got := ShadowBytes(c.delta, c.factor); got != c.want {
			t.Fatalf("ShadowBytes(%d, %v) = %d, want %d", c.delta, c.factor, got, c.want)
		}
	}
}

func TestFactorValidation(t *testing.T) {
	for factor, valid := range map[float64]bool{
		1.0: true, 1.2: true, 2.0: true,
		0.9: false, 2.01: false, 0: false, 20: false, math.NaN(): false,
	} {
		if got := ValidFactor(factor); got != valid {
			t.Fatalf("ValidFactor(%v) = %v, want %v", factor, got, valid)
		}
	}
	for factor, active := range map[float64]bool{
		1.2: true, 2.0: true,
		1.0: false, 0.9: false, 2.01: false, 0: false,
	} {
		if got := ActiveFactor(factor); got != active {
			t.Fatalf("ActiveFactor(%v) = %v, want %v", factor, got, active)
		}
	}
}

type collectWriter struct {
	frames [][]byte
}

func (w *collectWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	var raw bytes.Buffer
	for _, b := range mb {
		raw.Write(b.Bytes())
	}
	w.frames = append(w.frames, raw.Bytes())
	return nil
}

func (w *collectWriter) Close() error { return nil }

// parseFrame decodes one shadow frame: metadata, then a chunk size prefix and
// the payload, mirroring how a mux client reads a data frame.
func parseFrame(t *testing.T, raw []byte) (mux.FrameMetadata, []byte) {
	t.Helper()
	reader := bytes.NewReader(raw)
	meta := mux.FrameMetadata{}
	if err := meta.Unmarshal(reader, false); err != nil {
		t.Fatalf("failed to unmarshal frame metadata: %v", err)
	}
	size, err := serial.ReadUint16(reader)
	if err != nil {
		t.Fatalf("failed to read payload size: %v", err)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}
	return meta, payload
}

func TestShadowWriterDeliversDiscardableFrames(t *testing.T) {
	w := &collectWriter{}
	sw := NewShadowWriter(w)
	total := int64(shadowFrameSize*2 + 100)
	if err := sw.Deliver(total); err != nil {
		t.Fatal(err)
	}
	if len(w.frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(w.frames))
	}

	seen := int64(0)
	for i, raw := range w.frames {
		meta, payload := parseFrame(t, raw)
		if meta.SessionID != shadowSessionID {
			t.Fatalf("frame %d: session %d, want %d", i, meta.SessionID, shadowSessionID)
		}
		if meta.SessionStatus != mux.SessionStatusEnd {
			t.Fatalf("frame %d: status %d, want End", i, meta.SessionStatus)
		}
		if !meta.Option.Has(mux.OptionData) {
			t.Fatalf("frame %d: missing data option", i)
		}
		seen += int64(len(payload))
	}
	if seen != total {
		t.Fatalf("delivered %d bytes, want %d", seen, total)
	}
}

func TestShadowWriterEmptyIsNoop(t *testing.T) {
	w := &collectWriter{}
	if err := NewShadowWriter(w).Deliver(0); err != nil {
		t.Fatal(err)
	}
	if len(w.frames) != 0 {
		t.Fatalf("got %d frames, want 0", len(w.frames))
	}
}

func TestWorkerTickInjectsDeltaOnly(t *testing.T) {
	w := &collectWriter{}
	real := &Counter{}
	shadow := &Counter{}
	worker := &Worker{Factor: 2.0, Real: real, Writer: NewShadowWriter(w), ShadowCounter: shadow}

	real.Add(100)
	if !worker.Tick() {
		t.Fatal("tick failed")
	}
	if shadow.Value() != 100 {
		t.Fatalf("shadow counter = %d, want 100", shadow.Value())
	}

	// No new real bytes: no new shadow.
	if !worker.Tick() {
		t.Fatal("tick failed")
	}
	if shadow.Value() != 100 {
		t.Fatalf("shadow counter = %d, want 100 after idle tick", shadow.Value())
	}

	// History from before the worker started must not be re-injected: only
	// the delta since the previous tick counts.
	real.Add(50)
	if !worker.Tick() {
		t.Fatal("tick failed")
	}
	if shadow.Value() != 150 {
		t.Fatalf("shadow counter = %d, want 150", shadow.Value())
	}
	if len(w.frames) != 2 {
		t.Fatalf("got %d frames, want 2", len(w.frames))
	}
}

func TestWorkerTickStopsOnWriteError(t *testing.T) {
	worker := &Worker{Factor: 2.0, Real: &Counter{}, Writer: NewShadowWriter(&failingWriter{})}
	worker.Real.Add(10)
	if worker.Tick() {
		t.Fatal("tick should stop after a delivery error")
	}
}

type failingWriter struct{}

func (w *failingWriter) WriteMultiBuffer(buf.MultiBuffer) error { return io.ErrClosedPipe }
func (w *failingWriter) Close() error                           { return nil }

func TestRunStopsWhenStepReturnsFalse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	calls := 0
	Run(ctx, 10*time.Millisecond, func() bool {
		calls++
		return calls < 3
	})

	if calls != 3 {
		t.Fatalf("expected 3 ticks, got %d", calls)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		Run(ctx, 10*time.Millisecond, func() bool { return true })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop on cancelled context")
	}
}
