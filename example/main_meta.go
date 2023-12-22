package main

import (
	"context"
	"log"

	"github.com/beltran/gohive"
)

func main() {
	configuration := gohive.NewMetastoreConnectConfiguration()
	connection, err := gohive.ConnectToMetastore("hm.example.com", 9083, "KERBEROS", configuration)
        if err != nil {
                log.Fatal(err)
        }
        databases, err := connection.Client.GetAllDatabases(context.Background())
        log.Println("databases", databases)
        client_meta.Close()
}
