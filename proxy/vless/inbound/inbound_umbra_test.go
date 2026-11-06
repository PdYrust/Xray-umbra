package inbound

import (
	"testing"

	"github.com/xtls/xray-core/app/umbra"
)

func TestHandlerUmbraGate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     umbra.Config
		allowed bool
	}{
		{"no config", nil, true},
		{"disabled", &UmbraConfig{Enabled: false, Factor: 1.2}, false},
		{"active", &UmbraConfig{Enabled: true, Factor: 1.2}, true},
		{"max factor", &UmbraConfig{Enabled: true, Factor: 2.0}, true},
		{"factor one is a bypass", &UmbraConfig{Enabled: true, Factor: 1.0}, false},
		{"factor out of range", &UmbraConfig{Enabled: true, Factor: 2.5}, false},
	}
	for _, c := range cases {
		h := &Handler{umbra: c.cfg}
		err := umbra.Gate(h.umbra)
		if got := err == nil; got != c.allowed {
			t.Fatalf("%s: gate allowed = %v, want %v (err: %v)", c.name, got, c.allowed, err)
		}
	}
}
