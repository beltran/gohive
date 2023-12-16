package gohive

import (
	"log"
	"os"
	"testing"
	"fmt"
)

func TestConnectDefaultMeta(t *testing.T) {
	if "http" == os.Getenv("TRANSPORT") {
		t.Skip("we don't set the metastore for http in integration tests.");
	}
	client, err := OpenMetaStore("hm.example.com", 9083, getAuthForMeta())
	if err != nil {
		log.Fatal(err)
	}
	databases, err := client.GetAllDatabases()
	fmt.Println("databases", databases)
	client.Close()
}

func getAuthForMeta() string {
	auth := os.Getenv("AUTH")
	os.Setenv("KRB5CCNAME", "/tmp/krb5_gohive")
	if auth == "" {
		auth = "KERBEROS"
	}
	return auth
}
