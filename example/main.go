package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/beltran/gohive"
)

func main() {
	// Open a connection to Hive using the new SQL interface
	// Format: hive://username:password@host:port/database
	db, err := sql.Open("hive", "hive://username:password@localhost:10000/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Execute a query
	rows, err := db.Query("SELECT * FROM my_table LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	// Prepare a slice to hold the values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Iterate through the rows
	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			log.Fatal(err)
		}

		// Print the values
		for i, col := range columns {
			val := values[i]
			fmt.Printf("%s: %v\n", col, val)
		}
		fmt.Println("---")
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Execute a non-query statement
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)")
	if err != nil {
		log.Fatal(err)
	}
}
