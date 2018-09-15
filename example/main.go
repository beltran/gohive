package main

import (
	"context"
	"log"

	"github.com/beltran/gohive"
)

func main() {
	async := false
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

	cursor.Execute(ctx, "CREATE TABLE myTable (a INT, b STRING)", async)
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	cursor.Execute(ctx, "INSERT INTO myTable VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')", async)
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	cursor.Execute(ctx, "SELECT * FROM myTable", async)
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
