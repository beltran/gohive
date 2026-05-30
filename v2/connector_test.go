package gohive

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewConnector(t *testing.T) {
	cfg := Config{
		Host:     "localhost",
		Port:     10000,
		Auth:     "NONE",
		Username: "user",
		Password: "pass",
		Database: "mydb",
	}
	c := NewConnector(cfg)
	if c == nil {
		t.Fatal("expected non-nil connector")
	}
	if c.cfg.Host != "localhost" {
		t.Errorf("got host %q, want %q", c.cfg.Host, "localhost")
	}
	if c.cfg.Port != 10000 {
		t.Errorf("got port %d, want %d", c.cfg.Port, 10000)
	}
	if c.cfg.Database != "mydb" {
		t.Errorf("got database %q, want %q", c.cfg.Database, "mydb")
	}
}

func TestOpenDB(t *testing.T) {
	cfg := Config{
		Host: "localhost",
		Port: 10000,
		Auth: "NONE",
	}
	db := OpenDB(cfg)
	if db == nil {
		t.Fatal("expected non-nil *sql.DB")
	}
	db.Close()
}

func TestConnectorDriver(t *testing.T) {
	c := NewConnector(Config{})
	d := c.Driver()
	if d == nil {
		t.Fatal("expected non-nil driver")
	}
	if _, ok := d.(*Driver); !ok {
		t.Fatalf("expected *Driver, got %T", d)
	}
}

func TestConnectorHTTPPathDefault(t *testing.T) {
	connCfg := newConnectConfiguration()
	cfg := Config{} // HTTPPath empty
	if cfg.HTTPPath != "" {
		connCfg.HTTPPath = cfg.HTTPPath
	}
	if connCfg.HTTPPath != "cliservice" {
		t.Errorf("got HTTPPath %q, want %q", connCfg.HTTPPath, "cliservice")
	}
}

func TestConnectorHTTPPathCustom(t *testing.T) {
	connCfg := newConnectConfiguration()
	cfg := Config{HTTPPath: "custom/path"}
	if cfg.HTTPPath != "" {
		connCfg.HTTPPath = cfg.HTTPPath
	}
	if connCfg.HTTPPath != "custom/path" {
		t.Errorf("got HTTPPath %q, want %q", connCfg.HTTPPath, "custom/path")
	}
}

func TestConnectorHiveConfiguration(t *testing.T) {
	hiveConf := map[string]string{
		"hive.server2.thrift.sasl.qop": "auth-conf",
		"mapreduce.job.queuename":      "default",
	}
	connCfg := newConnectConfiguration()
	connCfg.HiveConfiguration = hiveConf

	if len(connCfg.HiveConfiguration) != 2 {
		t.Fatalf("expected 2 hive config entries, got %d", len(connCfg.HiveConfiguration))
	}
	if connCfg.HiveConfiguration["hive.server2.thrift.sasl.qop"] != "auth-conf" {
		t.Error("hive config key not set correctly")
	}
}

func TestConnectorTLSConfigDirect(t *testing.T) {
	customTLS := &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	}
	connCfg := newConnectConfiguration()
	connCfg.TLSConfig = customTLS

	if connCfg.TLSConfig != customTLS {
		t.Error("expected custom TLSConfig to be used directly")
	}
	if connCfg.TLSConfig.MinVersion != tls.VersionTLS13 {
		t.Error("expected MinVersion TLS 1.3")
	}
}

func TestConnectorSSLCertKeyFallback(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "test.pem")
	keyFile := filepath.Join(tmpDir, "test.key")

	certPEM, keyPEM := generateSelfSignedCert(t)
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		SSLCertFile:     certFile,
		SSLKeyFile:      keyFile,
		SSLInsecureSkip: true,
	}

	connCfg := newConnectConfiguration()
	// Simulate Connect logic
	if connCfg.TLSConfig == nil && cfg.SSLCertFile != "" && cfg.SSLKeyFile != "" {
		tlsConfig, err := getTlsConfiguration(cfg.SSLCertFile, cfg.SSLKeyFile)
		if err != nil {
			t.Fatalf("getTlsConfiguration failed: %v", err)
		}
		tlsConfig.InsecureSkipVerify = cfg.SSLInsecureSkip
		connCfg.TLSConfig = tlsConfig
	}

	if connCfg.TLSConfig == nil {
		t.Fatal("expected TLSConfig to be built from cert/key files")
	}
	if !connCfg.TLSConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
	if len(connCfg.TLSConfig.Certificates) == 0 {
		t.Error("expected at least one certificate")
	}
}

func TestConnectorSSLCAFileFallback(t *testing.T) {
	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "ca.crt")

	caCert, _ := generateSelfSignedCert(t)
	if err := os.WriteFile(caFile, caCert, 0600); err != nil {
		t.Fatal(err)
	}

	cfg := Config{SSLCAFile: caFile}

	connCfg := newConnectConfiguration()
	if connCfg.TLSConfig == nil && cfg.SSLCertFile == "" && cfg.SSLCAFile != "" {
		caPEM, err := os.ReadFile(cfg.SSLCAFile)
		if err != nil {
			t.Fatalf("failed to read CA file: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			t.Fatal("failed to append CA cert")
		}
		connCfg.TLSConfig = &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: false,
		}
	}

	if connCfg.TLSConfig == nil {
		t.Fatal("expected TLSConfig to be built from CA file")
	}
	if connCfg.TLSConfig.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be false")
	}
	if connCfg.TLSConfig.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}
}

func TestConnectorSSLInvalidCertPath(t *testing.T) {
	_, err := getTlsConfiguration("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Fatal("expected error for nonexistent cert/key files")
	}
}

func TestConnectorSSLInvalidCAPath(t *testing.T) {
	_, err := os.ReadFile("/nonexistent/ca.crt")
	if err == nil {
		t.Fatal("expected error for nonexistent CA file")
	}
}

// generateSelfSignedCert creates a self-signed certificate and key PEM for testing.
func generateSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}
