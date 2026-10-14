package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	gohive "github.com/ichsansaid/gohive/v2"
)

func main() {
	// Example 1: Using CA certificate only (server verification)
	exampleWithCAFile()

	// Example 2: Using client cert + key (mutual TLS)
	exampleWithCertAndKey()

	// Example 3: Using custom *tls.Config directly
	exampleWithCustomTLSConfig()
}

// exampleWithCAFile connects using a CA certificate to verify the server.
func exampleWithCAFile() {
	db := gohive.OpenDB(gohive.Config{
		Host:      "hs2.example.com",
		Port:      10000,
		Auth:      "NONE",
		Username:  "hive",
		Password:  "hive",
		Database:  "default",
		SSLCAFile: "/path/to/ca-cert.pem",
	})
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("CA cert example:", err)
	}
	fmt.Println("Connected with CA certificate")
}

// exampleWithCertAndKey connects using client certificate and key (mTLS).
func exampleWithCertAndKey() {
	db := gohive.OpenDB(gohive.Config{
		Host:            "hs2.example.com",
		Port:            10000,
		Auth:            "NONE",
		Username:        "hive",
		Password:        "hive",
		Database:        "default",
		SSLCertFile:     "/path/to/client-cert.pem",
		SSLKeyFile:      "/path/to/client-key.pem",
		SSLInsecureSkip: false,
	})
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("mTLS example:", err)
	}
	fmt.Println("Connected with client cert + key")
}

// exampleWithCustomTLSConfig connects using a user-provided *tls.Config.
// This gives full control over TLS settings.
func exampleWithCustomTLSConfig() {
	// Load CA cert
	caPEM, err := os.ReadFile("/path/to/ca-cert.pem")
	if err != nil {
		log.Fatal(err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		log.Fatal("failed to append CA cert")
	}

	// Load client cert + key
	clientCert, err := tls.LoadX509KeyPair("/path/to/client-cert.pem", "/path/to/client-key.pem")
	if err != nil {
		log.Fatal(err)
	}

	db := gohive.OpenDB(gohive.Config{
		Host:     "hs2.example.com",
		Port:     10000,
		Auth:     "NONE",
		Username: "hive",
		Password: "hive",
		Database: "default",
		TLSConfig: &tls.Config{
			RootCAs:            caPool,
			Certificates:       []tls.Certificate{clientCert},
			InsecureSkipVerify: false,
			MinVersion:         tls.VersionTLS12,
		},
	})
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("custom TLS example:", err)
	}
	fmt.Println("Connected with custom TLS config")
}
