package gohive

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"time"
)

// Config holds everything needed to connect to Hive/Impala via gohive v2.
type Config struct {
	Host              string
	Port              int
	Auth              string // "NONE", "KERBEROS", "NOSASL", etc.
	Username          string
	Password          string
	Database          string
	TransportMode     string // "binary" or "http"
	HTTPPath          string
	Service           string // Kerberos service name
	TLSConfig         *tls.Config
	SSLCertFile       string
	SSLKeyFile        string
	SSLCAFile         string
	SSLInsecureSkip   bool
	HiveConfiguration map[string]string
	ConnectTimeout    time.Duration // Timeout for establishing TCP connection
	HttpTimeout       time.Duration // Timeout for http calls
	SocketTimeout     time.Duration // Timeout for individual socket read/write operations. Keep this low (e.g. 2s) so context deadlines are respected promptly (see THRIFT-5233).
}

var _ driver.Connector = (*HiveConnector)(nil)

// HiveConnector implements driver.Connector using gohive v2 under the hood.
type HiveConnector struct {
	cfg Config
}

// NewConnector creates a new HiveConnector with the given config.
func NewConnector(cfg Config) *HiveConnector {
	return &HiveConnector{cfg: cfg}
}

// OpenDB is a convenience that returns a *sql.DB ready for use with GORM.
func OpenDB(cfg Config) *sql.DB {
	return sql.OpenDB(NewConnector(cfg))
}

// Connect establishes a new connection using gohive v2.
func (c *HiveConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connCfg := newConnectConfiguration()
	connCfg.Username = c.cfg.Username
	connCfg.Password = c.cfg.Password
	connCfg.Database = c.cfg.Database
	if c.cfg.TransportMode != "" {
		connCfg.TransportMode = c.cfg.TransportMode
	}
	if c.cfg.HTTPPath != "" {
		connCfg.HTTPPath = c.cfg.HTTPPath
	}
	connCfg.Service = c.cfg.Service
	if connCfg.Service == "" {
		connCfg.Service = "hive"
	}
	connCfg.TLSConfig = c.cfg.TLSConfig
	connCfg.HiveConfiguration = c.cfg.HiveConfiguration
	connCfg.ConnectTimeout = c.cfg.ConnectTimeout
	if c.cfg.SocketTimeout > 0 {
		connCfg.SocketTimeout = c.cfg.SocketTimeout
	} else {
		connCfg.SocketTimeout = 1 * time.Second // default: enables THRIFT-5233 context deadline retry
	}
	if c.cfg.HttpTimeout > 0 {
		connCfg.HttpTimeout = c.cfg.HttpTimeout
	}

	// Fallback: build TLS config from cert/key files if TLSConfig not provided directly
	if connCfg.TLSConfig == nil {

		if c.cfg.SSLCertFile != "" && c.cfg.SSLKeyFile != "" {
			tlsConfig, err := getTlsConfiguration(c.cfg.SSLCertFile, c.cfg.SSLKeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to configure SSL: %v", err)
			}
			tlsConfig.InsecureSkipVerify = c.cfg.SSLInsecureSkip
			connCfg.TLSConfig = tlsConfig
		} else if c.cfg.SSLCAFile != "" {

			caPEM, err := os.ReadFile(c.cfg.SSLCAFile)
			if err != nil {
				return nil, fmt.Errorf("invalid ca path: %s (%w)", c.cfg.SSLCAFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("invalid certification %q", c.cfg.SSLCAFile)
			}
			connCfg.TLSConfig = &tls.Config{
				RootCAs:            pool,
				InsecureSkipVerify: c.cfg.SSLInsecureSkip,
			}
		} else if c.cfg.SSLInsecureSkip {
			connCfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	conn, err := connect(ctx, c.cfg.Host, c.cfg.Port, c.cfg.Auth, connCfg)
	if err != nil {
		return nil, fmt.Errorf("gohive connect: %w", err)
	}
	return &sqlConnection{conn: conn}, nil
}

// Driver returns a placeholder driver (unused by sql.OpenDB).
func (c *HiveConnector) Driver() driver.Driver {
	return &Driver{}
}
