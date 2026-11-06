package umbra

import (
	"bytes"
	"context"
	"io"
	"math"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/serial"
)

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

// Counter is a minimal stats.Counter for tests.
type Counter struct {
	value int64
}

func (c *Counter) Value() int64      { return c.value }
func (c *Counter) Set(v int64) int64 { old := c.value; c.value = v; return old }
func (c *Counter) Add(v int64) int64 { old := c.value; c.value += v; return old }

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
	counted := &Counter{}
	sw := NewShadowWriter(w, counted)
	total := int64(shadowFrameSize*2 + 100)
	if err := sw.Deliver(total); err != nil {
		t.Fatal(err)
	}
	if len(w.frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(w.frames))
	}
	if counted.Value() != total {
		t.Fatalf("counter = %d, want %d", counted.Value(), total)
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
	if err := NewShadowWriter(w, nil).Deliver(0); err != nil {
		t.Fatal(err)
	}
	if len(w.frames) != 0 {
		t.Fatalf("got %d frames, want 0", len(w.frames))
	}
}

type failingWriter struct{}

func (w *failingWriter) WriteMultiBuffer(buf.MultiBuffer) error { return io.ErrClosedPipe }
func (w *failingWriter) Close() error                           { return nil }

type sliceReader struct {
	mb buf.MultiBuffer
}

func (r *sliceReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if len(r.mb) == 0 {
		return nil, io.EOF
	}
	mb := r.mb
	r.mb = nil
	return mb, nil
}

func readAll(t *testing.T, r buf.Reader) int64 {
	t.Helper()
	var total int64
	for {
		mb, err := r.ReadMultiBuffer()
		total += int64(mb.Len())
		if err != nil {
			return total
		}
	}
}

// deliveredBytes sums the payload of all shadow frames collected so far.
func deliveredBytes(t *testing.T, w *collectWriter) int64 {
	t.Helper()
	var total int64
	for _, raw := range w.frames {
		_, payload := parseFrame(t, raw)
		total += int64(len(payload))
	}
	return total
}

func TestDeliveryInjectsProportionally(t *testing.T) {
	for _, factor := range []float64{1.2, 1.5, 2.0} {
		w := &collectWriter{}
		d := NewDelivery(context.Background(), factor, nil)
		d.SetSink(w)

		writer := d.WrapWriter(&collectWriter{})
		for i := 0; i < 5; i++ {
			if err := writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(make([]byte, 1000))}); err != nil {
				t.Fatal(err)
			}
		}
		reader := d.WrapReader(&sliceReader{mb: buf.MultiBuffer{buf.FromBytes(make([]byte, 2000))}})
		readAll(t, reader)

		real := int64(5*1000 + 2000)
		want := int64(math.Round(float64(real) * (factor - 1.0)))
		if got := deliveredBytes(t, w); got != want {
			t.Fatalf("factor %v: delivered %d shadow bytes, want %d", factor, got, want)
		}
	}
}

func TestDeliverySetSinkLateFlushesOwed(t *testing.T) {
	w := &collectWriter{}
	d := NewDelivery(context.Background(), 2.0, nil)
	writer := d.WrapWriter(&collectWriter{})
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(make([]byte, 4000))}); err != nil {
		t.Fatal(err)
	}
	if len(w.frames) != 0 {
		t.Fatal("no shadow may be delivered before the sink is attached")
	}

	d.SetSink(w)
	if got := deliveredBytes(t, w); got != 4000 {
		t.Fatalf("delivered %d shadow bytes, want 4000", got)
	}
}

func TestDeliverySinkErrorNeverBreaksRealTraffic(t *testing.T) {
	d := NewDelivery(context.Background(), 2.0, nil)
	d.SetSink(&failingWriter{})

	writer := d.WrapWriter(&collectWriter{})
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(make([]byte, 4000))}); err != nil {
		t.Fatalf("real write must succeed even when shadow delivery fails: %v", err)
	}
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(make([]byte, 4000))}); err != nil {
		t.Fatalf("real write must succeed after shadow delivery failed: %v", err)
	}
}
