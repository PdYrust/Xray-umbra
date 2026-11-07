package umbra

import (
	"strings"
	"testing"
)

type fakeConfig struct {
	enabled bool
	factor  float64
}

func (c *fakeConfig) GetEnabled() bool   { return c.enabled }
func (c *fakeConfig) GetFactor() float64 { return c.factor }

func TestGate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"no config allows traffic", nil, ""},
		{"disabled rejects", &fakeConfig{enabled: false, factor: 1.2}, "disabled"},
		{"active allows", &fakeConfig{enabled: true, factor: 1.2}, ""},
		{"factor one is neutral", &fakeConfig{enabled: true, factor: 1.0}, ""},
		{"big factor allows", &fakeConfig{enabled: true, factor: 5.0}, ""},
		{"zero factor rejects", &fakeConfig{enabled: true}, "factor"},
		{"factor below one rejects", &fakeConfig{enabled: true, factor: 0.5}, "factor"},
	}
	for _, c := range cases {
		err := Gate(c.cfg)
		if c.wantErr == "" {
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", c.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Fatalf("%s: got %v, want error containing %q", c.name, err, c.wantErr)
		}
	}
}

func TestActive(t *testing.T) {
	for _, c := range []struct {
		cfg  Config
		want bool
	}{
		{nil, false},
		{&fakeConfig{enabled: false, factor: 1.2}, false},
		{&fakeConfig{enabled: true, factor: 1.2}, true},
		{&fakeConfig{enabled: true, factor: 5.0}, true},
		{&fakeConfig{enabled: true, factor: 1.0}, false},
	} {
		if got := Active(c.cfg); got != c.want {
			t.Fatalf("Active(%+v) = %v, want %v", c.cfg, got, c.want)
		}
	}
}
