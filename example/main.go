package main

import (
	"context"
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

	ctx := context.Background()
	configuration := gohive.NewConnectConfiguration()
	configuration.Service = "hive"
	configuration.FetchSize = 1000
	// Previously kinit should have done: kinit -kt ./secret.keytab hive/hs2.example.com@EXAMPLE.COM
	connection, errConn := gohive.Connect("hs2.example.com", 10000, "KERBEROS", configuration)
	if errConn != nil {
		log.Fatal(errConn)
	}
	cursor := connection.Cursor()

	cursor.Exec(ctx, "CREATE TABLE myTable (a INT, b STRING)")
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	cursor.Exec(ctx, "INSERT INTO myTable VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')")
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	cursor.Exec(ctx, "SELECT * FROM myTable")
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	var i int32
	var s string
	for cursor.HasMore(ctx) {
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		cursor.FetchOne(ctx, &i, &s)
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		log.Println(i, s)
	}

	cursor.Close()
	connection.Close()
}
