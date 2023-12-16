// +build all integration

package gohivemeta

import (
	"log"
	"os"
	"testing"
	"fmt"
)

func TestConnectDefault(t *testing.T) {
	auth := os.Getenv("AUTH")
	client, err := Open("hm.example.com", 9083, auth)
	if err != nil {
		log.Fatal(err)
	}
	databases, err := client.GetAllDatabases()
	fmt.Println("databases", databases)
	client.Close()
}

func getAuth() string {
	auth := os.Getenv("AUTH")
	os.Setenv("KRB5CCNAME", "/tmp/krb5_gohive")
	if auth == "" {
		auth = "KERBEROS"
	}
	return auth
}
