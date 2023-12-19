package main

import (
	"log"

	"github.com/beltran/gohive"
)

func main() {
	configuration := gohive.NewMetastoreConnectConfiguration()
	client_meta, err := gohive.ConnectToMetastore("hm.example.com", 9083, "KERBEROS", configuration)
        if err != nil {
                log.Fatal(err)
        }
        databases, err := client_meta.GetAllDatabases()
        log.Println("databases", databases)
        client_meta.Close()
}
