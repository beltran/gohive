//go:build all || integration
// +build all integration

package gohive

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func getTestTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func TestSQLDriverBasic(t *testing.T) {
	fmt.Println("Running SQL driver tests...")

	// Open a connection using the SQL interface
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Successfully connected to Hive!")
}

func TestSQLDriver(t *testing.T) {
	// Open a connection using the SQL interface
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLQuery(t *testing.T) {
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_table")

	// Create a test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING)", tableName))
	if err != nil {
		t.Fatalf("error creating table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert some test data
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2')", tableName))
	if err != nil {
		t.Fatalf("error inserting data: %v", err)
	}

	// Query the data
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("error querying data: %v", err)
	}
	defer rows.Close()

	// Check the results
	var id int
	var name string
	if !rows.Next() {
		t.Fatal("expected first row")
	}
	err = rows.Scan(&id, &name)
	if err != nil {
		t.Fatalf("error scanning first row: %v", err)
	}
	if id != 1 || name != "test1" {
		t.Errorf("got id=%d, name=%s, want id=1, name=test1", id, name)
	}

	if !rows.Next() {
		t.Fatal("expected second row")
	}
	err = rows.Scan(&id, &name)
	if err != nil {
		t.Fatalf("error scanning second row: %v", err)
	}
	if id != 2 || name != "test2" {
		t.Errorf("got id=%d, name=%s, want id=2, name=test2", id, name)
	}

	if rows.Next() {
		t.Fatal("unexpected third row")
	}
}

func TestSQLTypes(t *testing.T) {
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_types")

	// Create a table with various types
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			col_boolean BOOLEAN,
			col_tinyint TINYINT,
			col_smallint SMALLINT,
			col_int INT,
			col_bigint BIGINT,
			col_float FLOAT,
			col_double DOUBLE,
			col_decimal DECIMAL(10,2),
			col_string STRING,
			col_timestamp TIMESTAMP,
			col_date DATE,
			col_binary BINARY
		)
	`, tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert test data
	_, err = db.Exec(fmt.Sprintf(`
		INSERT INTO %s VALUES (
			true,
			1,
			2,
			3,
			4,
			5.5,
			6.6,
			7.77,
			'test',
			'2024-03-20 12:34:56',
			'2024-03-20',
			'binary'
		)
	`, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Query and verify the data
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected a row")
	}

	var (
		colBoolean   bool
		colTinyint   int8
		colSmallint  int16
		colInt       int32
		colBigint    int64
		colFloat     float32
		colDouble    float64
		colDecimal   string
		colString    string
		colTimestamp time.Time
		colDate      time.Time
		colBinary    []byte
	)

	err = rows.Scan(
		&colBoolean,
		&colTinyint,
		&colSmallint,
		&colInt,
		&colBigint,
		&colFloat,
		&colDouble,
		&colDecimal,
		&colString,
		&colTimestamp,
		&colDate,
		&colBinary,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the values
	if !colBoolean {
		t.Error("expected col_boolean to be true")
	}
	if colTinyint != 1 {
		t.Errorf("got col_tinyint=%d, want 1", colTinyint)
	}
	if colSmallint != 2 {
		t.Errorf("got col_smallint=%d, want 2", colSmallint)
	}
	if colInt != 3 {
		t.Errorf("got col_int=%d, want 3", colInt)
	}
	if colBigint != 4 {
		t.Errorf("got col_bigint=%d, want 4", colBigint)
	}
	if colFloat != 5.5 {
		t.Errorf("got col_float=%f, want 5.5", colFloat)
	}
	if colDouble != 6.6 {
		t.Errorf("got col_double=%f, want 6.6", colDouble)
	}
	if colDecimal != "7.77" {
		t.Errorf("got col_decimal=%s, want 7.77", colDecimal)
	}
	if colString != "test" {
		t.Errorf("got col_string=%s, want test", colString)
	}
	expectedTime := time.Date(2024, 3, 20, 12, 34, 56, 0, time.UTC)
	if !colTimestamp.Equal(expectedTime) {
		t.Errorf("got col_timestamp=%v, want %v", colTimestamp, expectedTime)
	}
	expectedDate := time.Date(2024, 3, 20, 0, 0, 0, 0, time.UTC)
	if !colDate.Equal(expectedDate) {
		t.Errorf("got col_date=%v, want %v", colDate, expectedDate)
	}
	if string(colBinary) != "binary" {
		t.Errorf("got col_binary=%s, want binary", string(colBinary))
	}
}

func TestSQLNullValues(t *testing.T) {
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_nulls")

	// Create a table with nullable columns
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT,
			name STRING
		)
	`, tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert a row with NULL values
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (NULL, NULL)", tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Query and verify NULL handling
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected a row")
	}

	var id sql.NullInt32
	var name sql.NullString

	err = rows.Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}

	if id.Valid {
		t.Error("expected id to be NULL")
	}
	if name.Valid {
		t.Error("expected name to be NULL")
	}
}

func TestSQLContext(t *testing.T) {
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_context")

	// Create a test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test context with Query
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// Test context with Exec
	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (3, 'test3')", tableName))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLPreparedStatement(t *testing.T) {
	db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_prepared")

	// Create a test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create a prepared statement
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Execute the prepared statement
	_, err = stmt.Exec(4, "test4")
	if err != nil {
		t.Fatal(err)
	}

	// Verify the inserted data
	var id int
	var name string
	err = db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE id = 4", tableName)).Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}
	if id != 4 || name != "test4" {
		t.Errorf("got id=%d, name=%s, want id=4, name=test4", id, name)
	}
}
