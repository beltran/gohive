package main

import (
	"fmt"
	"log"

	gohive "github.com/beltran/gohive/v2"
)

func main() {
	// Using HiveConnector with programmatic config (no DSN string needed).
	// This is useful for GORM integration or when you already have
	// connection parameters as structured config.
	db := gohive.OpenDB(gohive.Config{
		Host:     "hs2.example.com",
		Port:     10000,
		Auth:     "KERBEROS",
		Database: "default",
	})
	defer db.Close()

	// Test the connection
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// Execute a query
	rows, err := db.Query("SELECT * FROM test_table LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal(err)
		}
		for i, col := range columns {
			fmt.Printf("%s: %v\n", col, values[i])
		}
		fmt.Println("---")
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
