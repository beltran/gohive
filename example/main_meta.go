package main

import (
	"log"

	"github.com/beltran/gohive"
)

func main() {
	// Metastore client
	client_meta, err := gohive.OpenMetaStore("hm.example.com", 9083, "KERBEROS")
        if err != nil {
                log.Fatal(err)
        }
        databases, err := client_meta.GetAllDatabases()
        log.Println("databases", databases)
        client_meta.Close()
}
