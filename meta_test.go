package gohive

import (
	"context"
	"github.com/beltran/gohive/hive_metastore"
	"log"
	"os"
	"fmt"
	"testing"
	"math/rand"
)

var lettersDb = []rune("abcdefghijklmnopqrstuvwxyz")

func randSeqDb(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = lettersDb[rand.Intn(len(lettersDb))]
	}
	return string(b)
}

var tableIdDb = 0
var randNameDb = randSeq(10)

func GetDatabaseName() string {
	tableName := fmt.Sprintf("db_pokes_%s%d", randNameDb, tableIdDb)
	tableIdDb+= 1
	return tableName
}

func TestConnectDefaultMeta(t *testing.T) {
	if os.Getenv("METASTORE_SKIP") == "1" {
		t.Skip("metastore not set.")
	}
	configuration := NewMetastoreConnectConfiguration()
	client, err := ConnectToMetastore("hm.example.com", 9083, getAuthForMeta(), configuration)
	if err != nil {
		log.Fatal(err)
	}
	client.Close()
}

func Contains(c []string, s string) bool {
	for _, v := range c {
		if v == s {
			return true
		}
	}
	return false
}

func TestDatabaseOperations(t *testing.T) {
	if os.Getenv("METASTORE_SKIP") == "1" {
		t.Skip("metastore not set.")
	}
	configuration := NewMetastoreConnectConfiguration()
	connection, err := ConnectToMetastore("hm.example.com", 9083, getAuthForMeta(), configuration)
	if err != nil {
		log.Fatal(err)
	}

	name := GetDatabaseName()

	database := hive_metastore.Database{
		Name:        name,
		LocationUri: "/"}
	err = connection.Client.CreateDatabase(context.Background(), &database)
	if err != nil {
		log.Fatal(err)
	}
	databases, err := connection.Client.GetAllDatabases(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if !Contains(databases, name) {
		t.Fatalf("%s not found, databases: %+v", name, databases)
	}
	err = connection.Client.DropDatabase(context.Background(), name, false, false)
	if err != nil {
		log.Fatal(err)
	}
	databases, err = connection.Client.GetAllDatabases(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	if Contains(databases, name) {
		t.Fatalf("%s should have been deleted, databases: %+v", name, databases)
	}
	connection.Close()
}

func getAuthForMeta() string {
	auth := os.Getenv("AUTH")
	os.Setenv("KRB5CCNAME", "/tmp/krb5_gohive")
	if auth == "" {
		auth = "KERBEROS"
	}
	return auth
}
