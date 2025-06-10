package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/beltran/gohive"
)

func main() {
	// Open a connection to Hive using the new SQL interface
	// Format: hive://username:password@host:port/database?auth=KERBEROS&service=hive
	db, err := sql.Open("hive", "hive://hs2.example.com:10000/default?auth=KERBEROS&service=hive")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Create a test table
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)")
	if err != nil {
		log.Fatal(err)
	}

	// Insert some test data
	_, err = db.Exec("INSERT INTO test_table VALUES(1, 'test1'), (2, 'test2'), (3, 'test3')")
	if err != nil {
		log.Fatal(err)
	}

	// Execute a query
	rows, err := db.Query("SELECT * FROM test_table LIMIT 10")
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
}
