package inbound

import "testing"

func TestHandlerUmbraEnabled(t *testing.T) {
	h := &Handler{}
	if !h.umbraEnabled() {
		t.Fatal("handler without umbra config should be considered enabled")
	}

	h.umbra = &UmbraConfig{Enabled: false}
	if h.umbraEnabled() {
		t.Fatal("handler with umbra disabled should not be enabled")
	}

	h.umbra = &UmbraConfig{Enabled: true, Factor: 1.2, BlackholeTag: "umbra-sink"}
	if !h.umbraEnabled() {
		t.Fatal("handler with umbra enabled should be enabled")
	}
}