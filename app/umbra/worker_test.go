package umbra

import (
	"context"
	"testing"
	"time"
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
	}
	for _, c := range cases {
		if got := ShadowBytes(c.delta, c.factor); got != c.want {
			t.Fatalf("ShadowBytes(%d, %v) = %d, want %d", c.delta, c.factor, got, c.want)
		}
	}
}

func TestShouldTick(t *testing.T) {
	if ShouldTick(false) {
		t.Fatal("ShouldTick(false) should be false")
	}
	if !ShouldTick(true) {
		t.Fatal("ShouldTick(true) should be true")
	}
}

func TestRunStopsWhenStepReturnsFalse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	calls := 0
	Run(ctx, 10*time.Millisecond, func() bool {
		calls++
		return calls < 3
	})

	if calls < 3 {
		t.Fatalf("expected at least 3 ticks, got %d", calls)
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