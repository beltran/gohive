package gohive

import (
	"context"
	"log"
	"os"
	"testing"
	"reflect"
)

func TestConnectDefaultMeta(t *testing.T) {
	if "http" == os.Getenv("TRANSPORT") || "NONE" == os.Getenv("AUTH") {
		t.Skip("we don't set the metastore for http in integration tests.");
	}
	configuration := NewMetastoreConnectConfiguration()
	client, err := ConnectToMetastore("hm.example.com", 9083, getAuthForMeta(), configuration)
	if err != nil {
		log.Fatal(err)
	}
	databases, err := client.GetAllDatabases(context.Background())
	expected := []string{"default"}
	if !reflect.DeepEqual(databases, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, databases)
	}
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
