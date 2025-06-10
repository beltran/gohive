package gohive

import (
	"testing"
)

func TestParseDSN_Valid(t *testing.T) {
	tests := []struct {
		name   string
		dsn    string
		expect DSN
	}{
		{
			name: "basic with db",
			dsn:  "hive://host:10000/db?auth=NONE&transport=binary",
			expect: DSN{
				Host:          "host",
				Port:          10000,
				Database:      "db",
				Auth:          "NONE",
				TransportMode: "binary",
				Service:       "hive",
			},
		},
		{
			name: "with username and password",
			dsn:  "hive://user:pass@host:10000/db?auth=PLAIN",
			expect: DSN{
				Username:      "user",
				Password:      "pass",
				Host:          "host",
				Port:          10000,
				Database:      "db",
				Auth:          "PLAIN",
				TransportMode: "binary",
				Service:       "hive",
			},
		},
		{
			name: "with ssl and skipverify",
			dsn:  "hive://host:10000/db?auth=NONE&sslcert=cert.pem&sslkey=key.pem&sslinsecureskipverify=true",
			expect: DSN{
				Host:            "host",
				Port:            10000,
				Database:        "db",
				Auth:            "NONE",
				SSLCertFile:     "cert.pem",
				SSLKeyFile:      "key.pem",
				SSLInsecureSkip: true,
				TransportMode:   "binary",
				Service:         "hive",
			},
		},
		{
			name: "with service and transport",
			dsn:  "hive://host:10000/db?auth=NONE&service=impala&transport=http",
			expect: DSN{
				Host:          "host",
				Port:          10000,
				Database:      "db",
				Auth:          "NONE",
				Service:       "impala",
				TransportMode: "http",
			},
		},
		{
			name: "missing port (should default to 10000)",
			dsn:  "hive://host/db?auth=NONE",
			expect: DSN{
				Host:          "host",
				Port:          10000,
				Database:      "db",
				Auth:          "NONE",
				TransportMode: "binary",
				Service:       "hive",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dsn, err := ParseDSN(tc.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if dsn.Host != tc.expect.Host || dsn.Port != tc.expect.Port || dsn.Database != tc.expect.Database || dsn.Auth != tc.expect.Auth || dsn.Username != tc.expect.Username || dsn.Password != tc.expect.Password || dsn.SSLCertFile != tc.expect.SSLCertFile || dsn.SSLKeyFile != tc.expect.SSLKeyFile || dsn.SSLInsecureSkip != tc.expect.SSLInsecureSkip || dsn.TransportMode != tc.expect.TransportMode || dsn.Service != tc.expect.Service {
				t.Errorf("got %+v, want %+v", dsn, tc.expect)
			}
		})
	}
}

func TestParseDSN_Invalid(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"missing scheme", "host:10000/db?auth=NONE"},
		{"missing db", "hive://host:10000?auth=NONE"},
		{"invalid port", "hive://host:abc/db?auth=NONE"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDSN(tc.dsn)
			if err == nil {
				t.Errorf("expected error for DSN: %s", tc.dsn)
			}
		})
	}
}
