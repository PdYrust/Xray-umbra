package conf_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/proxy/vless/inbound"
	"github.com/xtls/xray-core/proxy/vless/outbound"
)

func TestVLessOutbound(t *testing.T) {
	creator := func() Buildable {
		return new(VLessOutboundConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"vnext": [{
					"address": "example.com",
					"port": 443,
					"users": [
						{
							"id": "27848739-7e62-4138-9fd3-098a63964b6b",
							"flow": "xtls-rprx-vision-udp443",
							"encryption": "none",
							"level": 0
						}
					]
				}]
			}`,
			Parser: loadJSON(creator),
			Output: &outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: &net.IPOrDomain{
						Address: &net.IPOrDomain_Domain{
							Domain: "example.com",
						},
					},
					Port: 443,
					User: &protocol.User{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:         "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow:       "xtls-rprx-vision-udp443",
							Encryption: "none",
						}),
						Level: 0,
					},
				},
			},
		},
		{
			Input: `{
				"address": "example.com",
				"port": 443,
				"id": "27848739-7e62-4138-9fd3-098a63964b6b",
				"flow": "xtls-rprx-vision-udp443",
				"encryption": "none",
				"level": 0
			}`,
			Parser: loadJSON(creator),
			Output: &outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: &net.IPOrDomain{
						Address: &net.IPOrDomain_Domain{
							Domain: "example.com",
						},
					},
					Port: 443,
					User: &protocol.User{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:         "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow:       "xtls-rprx-vision-udp443",
							Encryption: "none",
						}),
						Level: 0,
					},
				},
			},
		},
	})
}

func TestVLessInbound(t *testing.T) {
	creator := func() Buildable {
		return new(VLessInboundConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"clients": [
					{
						"id": "27848739-7e62-4138-9fd3-098a63964b6b",
						"flow": "xtls-rprx-vision",
						"level": 0,
						"email": "love@example.com"
					}
				],
				"decryption": "none",
				"fallbacks": [
					{
						"dest": 80
					},
					{
						"alpn": "h2",
						"dest": "@/dev/shm/domain.socket",
						"xver": 2
					},
					{
						"path": "/innerws",
						"dest": "serve-ws-none"
					}
				]
			}`,
			Parser: loadJSON(creator),
			Output: &inbound.Config{
				Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:   "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow: "xtls-rprx-vision",
						}),
						Level: 0,
						Email: "love@example.com",
					},
				},
				Decryption: "none",
				Fallbacks: []*inbound.Fallback{
					{
						Alpn: "",
						Path: "",
						Type: "tcp",
						Dest: "localhost:80",
						Xver: 0,
					},
					{
						Alpn: "h2",
						Path: "",
						Type: "unix",
						Dest: "@/dev/shm/domain.socket",
						Xver: 2,
					},
					{
						Alpn: "",
						Path: "/innerws",
						Type: "serve",
						Dest: "serve-ws-none",
						Xver: 0,
					},
				},
			},
		},
		{
			Input: `{
				"clients": [
					{
						"id": "27848739-7e62-4138-9fd3-098a63964b6b",
						"flow": "xtls-rprx-vision",
						"level": 0,
						"email": "love@example.com"
					}
				],
				"decryption": "none",
				"umbra": {
					"enabled": true,
					"factor": 1.2
				}
			}`,
			Parser: loadJSON(creator),
			Output: &inbound.Config{
				Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id:   "27848739-7e62-4138-9fd3-098a63964b6b",
							Flow: "xtls-rprx-vision",
						}),
						Level: 0,
						Email: "love@example.com",
					},
				},
				Decryption: "none",
				Umbra: &inbound.UmbraConfig{
					Enabled: true,
					Factor:  1.2,
				},
			},
		},
	})
}

func TestVLessInboundUmbraValidation(t *testing.T) {
	creator := func() Buildable {
		return new(VLessInboundConfig)
	}
	base := `{
		"clients": [{"id": "27848739-7e62-4138-9fd3-098a63964b6b", "email": "love@example.com"}],
		"decryption": "none",
		%s
	}`

	invalid := map[string]string{
		"factor below one":                `"umbra": {"enabled": true, "factor": 0.5}`,
		"factor zero":                     `"umbra": {"enabled": true, "factor": 0}`,
		"factor negative":                 `"umbra": {"enabled": true, "factor": -1}`,
		"factor below one while disabled": `"umbra": {"enabled": false, "factor": 0.5}`,
	}
	for name, umbra := range invalid {
		if _, err := loadJSON(creator)(fmt.Sprintf(base, umbra)); err == nil {
			t.Fatalf("%s: expected a config error", name)
		}
	}

	valid := map[string]string{
		"disabled without factor":      `"umbra": {"enabled": false}`,
		"disabled with factor":         `"umbra": {"enabled": false, "factor": 1.5}`,
		"active factor":                `"umbra": {"enabled": true, "factor": 2.0}`,
		"factor one while enabled":     `"umbra": {"enabled": true, "factor": 1.0}`,
		"factor missing while enabled": `"umbra": {"enabled": true}`,
		"big factor":                   `"umbra": {"enabled": true, "factor": 5.0}`,
		"very big factor":              `"umbra": {"enabled": true, "factor": 20}`,
	}
	for name, umbra := range valid {
		if _, err := loadJSON(creator)(fmt.Sprintf(base, umbra)); err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
	}
}

func TestInboundLevelUmbraRejected(t *testing.T) {
	build := func(t *testing.T, raw string) error {
		t.Helper()
		var cfg InboundDetourConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatalf("failed to parse inbound config: %v", err)
		}
		_, err := cfg.Build()
		return err
	}

	// "umbra" belongs inside "settings"; an inbound-level key must be
	// rejected rather than silently ignored.
	if err := build(t, `{
		"protocol": "vless",
		"port": 12345,
		"settings": {
			"clients": [{"id": "27848739-7e62-4138-9fd3-098a63964b6b", "email": "love@example.com"}],
			"decryption": "none"
		},
		"umbra": {"enabled": true, "factor": 1.2}
	}`); err == nil {
		t.Fatal("expected error for inbound-level umbra")
	}

	// Setting umbra in both places must fail as well.
	if err := build(t, `{
		"protocol": "vless",
		"port": 12345,
		"settings": {
			"clients": [{"id": "27848739-7e62-4138-9fd3-098a63964b6b", "email": "love@example.com"}],
			"decryption": "none",
			"umbra": {"enabled": true, "factor": 1.2}
		},
		"umbra": {"enabled": true, "factor": 1.2}
	}`); err == nil {
		t.Fatal("expected error for umbra set at both levels")
	}

	if err := build(t, `{
		"protocol": "vless",
		"port": 12345,
		"settings": {
			"clients": [{"id": "27848739-7e62-4138-9fd3-098a63964b6b", "email": "love@example.com"}],
			"decryption": "none",
			"umbra": {"enabled": true, "factor": 1.2}
		}
	}`); err != nil {
		t.Fatalf("settings-level umbra should build: %v", err)
	}
}
