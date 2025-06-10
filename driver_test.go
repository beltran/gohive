//go:build all || integration
// +build all integration

package gohive

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func getTestTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func getSQLAuth() string {
	auth := os.Getenv("AUTH")
	if auth == "" {
		return "NONE"
	}
	return auth
}

func getSQLTransport() string {
	transport := os.Getenv("TRANSPORT")
	if transport == "" {
		return "binary"
	}
	return transport
}

func getSQLSsl() bool {
	ssl := os.Getenv("SSL")
	return ssl == "1"
}

func TestSQLDriverAuthNone(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test with NONE configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Username = "username"
	config.Password = "password"
	config.Database = "default"

	db, err := sql.Open("hive", fmt.Sprintf("hive://%s:%s@hs2.example.com:10000/%s?auth=NONE", config.Username, config.Password, config.Database))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverAuthKerberos(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "KERBEROS" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test with Kerberos configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/%s?auth=%s", config.Database, auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverAuthDigestMd5(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "DIGEST-MD5" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test with DIGEST-MD5 configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/%s?auth=%s", config.Database, auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverAuthPlain(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "PLAIN" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test with PLAIN configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/%s?auth=%s", config.Database, auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverAuthGssapi(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "GSSAPI" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test with GSSAPI configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/%s?auth=%s", config.Database, auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverInvalidHost(t *testing.T) {
	auth := getSQLAuth()
	// Test connection to a non-existent host
	connStr := fmt.Sprintf("hive://nonexistent.example.com:12345/default?auth=%s", auth)

	db, err := sql.Open("hive", connStr)
	if err != nil {
		if !strings.Contains(err.Error(), "no such host") {
			t.Errorf("Expected 'no such host' error from sql.Open, got: %v", err)
		}
		return
	}
	defer db.Close()

	// Attempt to ping should fail
	err = db.Ping()
	if err == nil {
		t.Fatal("Expected error when connecting to non-existent host, got nil")
	}
	if !strings.Contains(err.Error(), "no such host") {
		t.Errorf("Expected 'no such host' error from db.Ping, got: %v", err)
	}
}

func TestSQLDriver(t *testing.T) {
	auth := getSQLAuth()
	// Open a connection using the SQL interface
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
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
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
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
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
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
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_nulls")

	// Create a table with various nullable columns
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT,
			name STRING,
			ts TIMESTAMP,
			dt DATE,
			val DOUBLE
		)
	`, tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert a row with NULL values
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL)", tableName))
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

	var (
		id   sql.NullInt32
		name sql.NullString
		ts   sql.NullTime
		dt   sql.NullTime
		val  sql.NullFloat64
	)

	err = rows.Scan(&id, &name, &ts, &dt, &val)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all values are NULL
	if id.Valid {
		t.Error("expected id to be NULL")
	}
	if name.Valid {
		t.Error("expected name to be NULL")
	}
	if ts.Valid {
		t.Error("expected ts to be NULL")
	}
	if dt.Valid {
		t.Error("expected dt to be NULL")
	}
	if val.Valid {
		t.Error("expected val to be NULL")
	}
}

func TestSQLContext(t *testing.T) {
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
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
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
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

func TestSQLTimestampFormatting(t *testing.T) {
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_timestamp")

	// Create a test table with a timestamp column
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT, ts TIMESTAMP)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert a row using a prepared statement with a timestamp
	testTime := time.Date(2024, 3, 20, 12, 34, 56, 0, time.UTC)
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(1, testTime)
	if err != nil {
		t.Fatalf("Failed to insert timestamp: %v", err)
	}

	// Query the data back
	var id int
	var ts time.Time
	err = db.QueryRow(fmt.Sprintf("SELECT * FROM %s", tableName)).Scan(&id, &ts)
	if err != nil {
		t.Fatalf("Failed to query timestamp: %v", err)
	}

	// Verify the timestamp was stored correctly
	if !ts.Equal(testTime) {
		t.Errorf("Timestamp mismatch: got %v, want %v", ts, testTime)
	}
}

func TestSQLDriverNoCredentials(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test without credentials in DSN
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverDSNParsing(t *testing.T) {
	auth := getSQLAuth()
	testCases := []struct {
		name        string
		dsn         string
		expectError bool
	}{
		{
			name:        "missing hive:// prefix",
			dsn:         "username:password@hs2.example.com:10000/default?auth=NONE",
			expectError: true,
		},
		{
			name:        "missing database",
			dsn:         fmt.Sprintf("hive://hs2.example.com:10000?auth=%s", auth),
			expectError: true,
		},
		{
			name:        "missing port",
			dsn:         fmt.Sprintf("hive://hs2.example.com/default?auth=%s", auth),
			expectError: true,
		},
		{
			name:        "invalid port",
			dsn:         fmt.Sprintf("hive://hs2.example.com:invalid/default?auth=%s", auth),
			expectError: true,
		},
		{
			name:        "valid DSN",
			dsn:         fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth),
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sql.Open("hive", tc.dsn)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for DSN: %s", tc.dsn)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Unexpected error for DSN: %s, error: %v", tc.dsn, err)
			}
		})
	}
}

func TestSQLDriverQueryParams(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	// Test with multiple query parameters
	dsn := fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s&transport=binary&service=hive", auth)
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLDriverDatabaseOperations(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("test_table_%d", time.Now().UnixNano())
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Logf("Table created: %s", tableName)

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2')", tableName))
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	t.Logf("Data inserted into table: %s", tableName)

	// Before verifying table contents, add a query to check data
	checkRows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to check table data: %v", err)
	}
	defer checkRows.Close()

	var rowCount int
	if checkRows.Next() {
		err = checkRows.Scan(&rowCount)
		if err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}
	t.Logf("Current row count in table: %d", rowCount)

	// Verify table contents directly
	verifyRows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to verify table contents: %v", err)
	}
	defer verifyRows.Close()

	// Before iterating, log the column names
	cols, err := verifyRows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	t.Logf("Verification columns: %v", cols)

	// Log the result of verifyRows.Next() before scanning
	var verifyCount int
	for verifyRows.Next() {
		t.Logf("verifyRows.Next() returned true")
		var verifyId int
		var verifyName string
		err = verifyRows.Scan(&verifyId, &verifyName)
		if err != nil {
			t.Fatalf("Failed to scan verification row: %v", err)
		}
		verifyCount++
	}
	t.Logf("verifyRows.Next() returned false (end of rows)")

	if verifyCount != 2 {
		t.Errorf("Expected 2 rows in verification, got %d", verifyCount)
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	var id int
	var name string
	var count int
	for rows.Next() {
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	t.Logf("Actual row count: %d", count)
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	t.Logf("Query executed for table: %s", tableName)
}

func TestSQLDriverDataTypes(t *testing.T) {
	auth := getSQLAuth()
	// Create a unique table name for this test
	tableName := fmt.Sprintf("test_types_table_%d", time.Now().UnixNano())

	// Connect to the database
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a table with various data types
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id INT,
			name STRING,
			age SMALLINT,
			height FLOAT,
			weight DOUBLE,
			is_active BOOLEAN,
			created_at TIMESTAMP,
			birth_date DATE,
			notes VARCHAR(100),
			data BINARY
		)
	`, tableName)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	t.Logf("Table created: %s", tableName)

	// Insert test data
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s VALUES
		(1, 'John Doe', 30, 1.75, 70.5, true, '2024-03-20 10:00:00', '1990-01-01', 'Test notes', 'binary data')
	`, tableName)
	if _, err := db.Exec(insertSQL); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Query the data back
	querySQL := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.Query(querySQL)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	// Scan the row
	if !rows.Next() {
		t.Fatal("Expected one row but got none")
	}

	var id int
	var name string
	var age int16
	var height float32
	var weight float64
	var isActive bool
	var createdAt time.Time
	var birthDate time.Time
	var notes string
	var data []byte

	err = rows.Scan(&id, &name, &age, &height, &weight, &isActive, &createdAt, &birthDate, &notes, &data)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	// Verify the values
	if id != 1 {
		t.Errorf("Expected id=1, got %d", id)
	}
	if name != "John Doe" {
		t.Errorf("Expected name='John Doe', got '%s'", name)
	}
	if age != 30 {
		t.Errorf("Expected age=30, got %d", age)
	}
	if height != 1.75 {
		t.Errorf("Expected height=1.75, got %f", height)
	}
	if weight != 70.5 {
		t.Errorf("Expected weight=70.5, got %f", weight)
	}
	if !isActive {
		t.Error("Expected isActive=true")
	}
	expectedCreatedAt, _ := time.Parse("2006-01-02 15:04:05", "2024-03-20 10:00:00")
	if !createdAt.Equal(expectedCreatedAt) {
		t.Errorf("Expected createdAt='2024-03-20 10:00:00', got '%v'", createdAt)
	}
	expectedBirthDate, _ := time.Parse("2006-01-02", "1990-01-01")
	if !birthDate.Equal(expectedBirthDate) {
		t.Errorf("Expected birthDate='1990-01-01', got '%v'", birthDate)
	}
	if notes != "Test notes" {
		t.Errorf("Expected notes='Test notes', got '%s'", notes)
	}
	if string(data) != "binary data" {
		t.Errorf("Expected data='binary data', got '%s'", string(data))
	}

	// Clean up
	dropTableSQL := fmt.Sprintf("DROP TABLE %s", tableName)
	_, err = db.Exec(dropTableSQL)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

func TestSQLDriverNullValues(t *testing.T) {
	auth := getSQLAuth()
	// Create a unique table name for this test
	tableName := fmt.Sprintf("test_null_table_%d", time.Now().UnixNano())

	// Connect to the database
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a table with nullable columns
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id INT,
			name STRING,
			value DOUBLE,
			created_at TIMESTAMP
		)
	`, tableName)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	t.Logf("Table created: %s", tableName)

	// Insert a row with NULL values
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s VALUES
		(1, NULL, NULL, NULL)
	`, tableName)

	_, err = db.Exec(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	t.Logf("Data inserted into table: %s", tableName)

	// Query the data back
	querySQL := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := db.Query(querySQL)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	// Scan the row
	if !rows.Next() {
		t.Fatal("Expected one row but got none")
	}

	var id int
	var name sql.NullString
	var value sql.NullFloat64
	var createdAt sql.NullTime

	err = rows.Scan(&id, &name, &value, &createdAt)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	// Verify the values
	if id != 1 {
		t.Errorf("Expected id=1, got %d", id)
	}
	if name.Valid {
		t.Error("Expected name to be NULL")
	}
	if value.Valid {
		t.Error("Expected value to be NULL")
	}
	if createdAt.Valid {
		t.Error("Expected createdAt to be NULL")
	}

	// Clean up
	dropTableSQL := fmt.Sprintf("DROP TABLE %s", tableName)
	_, err = db.Exec(dropTableSQL)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

// Test for large result sets
func TestSQLDriverLargeResultSet(t *testing.T) {
	auth := getSQLAuth()
	tableName := fmt.Sprintf("test_large_table_%d", time.Now().UnixNano())
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	createTableSQL := fmt.Sprintf(`CREATE TABLE %s (id INT, value STRING)`, tableName)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d, 'val_%d')", i, i))
	}
	insertSQL := fmt.Sprintf(`INSERT INTO %s VALUES %s`, tableName, strings.Join(values, ","))
	_, err = db.Exec(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var value string
		err = rows.Scan(&id, &value)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}
	if count != 1000 {
		t.Errorf("Expected 1000 rows, got %d", count)
	}

	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

// Test for error handling
func TestSQLDriverErrorHandling(t *testing.T) {
	auth := getSQLAuth()
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("SELECT * FROM non_existent_table")
	if err == nil {
		t.Error("Expected error for non-existent table, got nil")
	}

	_, err = db.Exec("INVALID SQL SYNTAX")
	if err == nil {
		t.Error("Expected error for invalid SQL, got nil")
	}
}

// Test for connection reuse
func TestSQLDriverConnectionReuse(t *testing.T) {
	auth := getSQLAuth()
	for i := 0; i < 10; i++ {
		db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
		if err != nil {
			t.Fatalf("Failed to open connection on iteration %d: %v", i, err)
		}
		rows, err := db.Query("SELECT 1")
		if err != nil {
			t.Fatalf("Failed to query on iteration %d: %v", i, err)
		}
		rows.Close()
		db.Close()
	}
}

// Test for column name case sensitivity
func TestSQLDriverColumnNameCaseSensitivity(t *testing.T) {
	auth := getSQLAuth()
	tableName := fmt.Sprintf("test_case_table_%d", time.Now().UnixNano())
	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	createTableSQL := fmt.Sprintf(`CREATE TABLE %s (Id INT, Name STRING, eMail STRING)`, tableName)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (1, 'Alice', 'alice@example.com')`, tableName))
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	expected := []string{tableName + ".id", tableName + ".name", tableName + ".email"}
	for i, col := range expected {
		if cols[i] != col {
			t.Errorf("Expected column %s, got %s", col, cols[i])
		}
	}
	rows.Next()
	var id int
	var name, email string
	err = rows.Scan(&id, &name, &email)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}
	if name != "Alice" || email != "alice@example.com" {
		t.Errorf("Expected Alice/alice@example.com, got %s/%s", name, email)
	}
	_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

func TestSQLColumnTypeScanType(t *testing.T) {
	conn, cursor := makeConnection(t, 1000)
	defer closeAll(t, conn, cursor)

	// Create a table with all supported types
	tableName := getTestTableName("column_type_test")
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			col_boolean BOOLEAN,
			col_tinyint TINYINT,
			col_smallint SMALLINT,
			col_int INT,
			col_bigint BIGINT,
			col_float FLOAT,
			col_double DOUBLE,
			col_decimal DECIMAL(10,2),
			col_string STRING,
			col_varchar VARCHAR(50),
			col_char CHAR(10),
			col_timestamp TIMESTAMP,
			col_date DATE,
			col_binary BINARY,
			col_array ARRAY<STRING>,
			col_map MAP<STRING,STRING>,
			col_struct STRUCT<f1:STRING,f2:INT>,
			col_union UNIONTYPE<STRING,INT>
		)`, tableName)

	cursor.exec(context.Background(), createTableSQL)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Insert a test row
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s VALUES (
			true,
			127,
			32767,
			2147483647,
			9223372036854775807,
			3.14,
			3.14159265359,
			1234.56,
			'string value',
			'varchar value',
			'char value',
			'2024-03-20 12:34:56',
			'2024-03-20',
			'binary data',
			array('array', 'values'),
			map('key1', 'value1', 'key2', 'value2'),
			named_struct('f1', 'struct value', 'f2', 42),
			create_union(0, 'union value', 0)
		)`, tableName)

	cursor.exec(context.Background(), insertSQL)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Query the table
	querySQL := fmt.Sprintf("SELECT * FROM %s", tableName)
	cursor.exec(context.Background(), querySQL)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Get the rows interface
	rows := &rows{cursor: cursor}

	// Test each column's scan type
	expectedTypes := map[string]reflect.Type{
		"col_boolean":   reflect.TypeOf(false),
		"col_tinyint":   reflect.TypeOf(int8(0)),
		"col_smallint":  reflect.TypeOf(int16(0)),
		"col_int":       reflect.TypeOf(int32(0)),
		"col_bigint":    reflect.TypeOf(int64(0)),
		"col_float":     reflect.TypeOf(float32(0)),
		"col_double":    reflect.TypeOf(float64(0)),
		"col_decimal":   reflect.TypeOf(""),
		"col_string":    reflect.TypeOf(""),
		"col_varchar":   reflect.TypeOf(""),
		"col_char":      reflect.TypeOf(""),
		"col_timestamp": reflect.TypeOf(time.Time{}),
		"col_date":      reflect.TypeOf(time.Time{}),
		"col_binary":    reflect.TypeOf([]byte{}),
		"col_array":     reflect.TypeOf(""),
		"col_map":       reflect.TypeOf(""),
		"col_struct":    reflect.TypeOf(""),
		"col_union":     reflect.TypeOf(""),
	}

	// Get column names
	columns := rows.Columns()
	if len(columns) == 0 {
		t.Fatal("No columns returned")
	}

	// Create a map of fully qualified column names to their base names
	columnMap := make(map[string]string)
	for _, col := range columns {
		parts := strings.Split(col, ".")
		if len(parts) == 2 {
			columnMap[col] = parts[1]
		} else {
			columnMap[col] = col
		}
	}

	for i, colName := range columns {
		scanType := rows.ColumnTypeScanType(i)
		baseName := columnMap[colName]
		expectedType, exists := expectedTypes[baseName]
		if !exists {
			t.Errorf("Unexpected column name: %s", colName)
			continue
		}

		if scanType != expectedType {
			t.Errorf("Column %s: expected scan type %v, got %v", colName, expectedType, scanType)
		}
	}

	// Verify we tested all expected types
	for expectedCol := range expectedTypes {
		found := false
		for _, col := range columns {
			if columnMap[col] == expectedCol {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected column %s not found in result set", expectedCol)
		}
	}
}

func TestSQLDriverSpecialCharacters(t *testing.T) {
	// Create a new connection
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	// Create a test table
	tableName := fmt.Sprintf("test_special_table_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id INT,
			text STRING
		)
	`, tableName)
	_, err = db.Exec(createSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))

	// Test data with special characters
	data := []string{"こんにちは", "emoji: 😊", "quote\"test\""}

	// Insert test data
	insertSQL := "INSERT INTO " + tableName + " VALUES "
	values := make([]string, len(data))
	for i, text := range data {
		values[i] = fmt.Sprintf("(%d, '%s')", i, text)
	}
	insertSQL += strings.Join(values, ", ")
	t.Logf("Insert statement: %s", insertSQL)
	if _, err := db.Exec(insertSQL); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Verify the data
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to query test data: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id *int
		var text *string
		if err := rows.Scan(&id, &text); err != nil {
			t.Fatalf("Failed to scan special row: %v", err)
		}
		if id == nil {
			t.Errorf("Row %d: id is nil", count)
			continue
		}
		if text == nil {
			t.Errorf("Row %d: text is nil", *id)
			continue
		}
		t.Logf("Row %d: retrieved text: %q", *id, *text)
		if *text != data[*id] {
			t.Errorf("Expected '%s', got '%s'", data[*id], *text)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
	if count != len(data) {
		t.Errorf("Expected %d rows, got %d", len(data), count)
	}
}

func TestSQLStmtClose(t *testing.T) {
	// Create a new connection
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	// Create a prepared statement
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Close the statement
	err = stmt.Close()
	if err != nil {
		t.Fatalf("Failed to close statement: %v", err)
	}

	// Try to use the closed statement
	_, err = stmt.Query()
	if err == nil {
		t.Fatal("Expected error when using closed statement")
	}
}

func TestSQLColumnTypeDatabaseTypeName(t *testing.T) {
	conn, cursor := makeConnection(t, 1000)
	defer closeAll(t, conn, cursor)

	// Create a table with all supported types
	tableName := getTestTableName("column_type_test")
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			col_boolean BOOLEAN,
			col_tinyint TINYINT,
			col_smallint SMALLINT,
			col_int INT,
			col_bigint BIGINT,
			col_float FLOAT,
			col_double DOUBLE,
			col_decimal DECIMAL(10,2),
			col_string STRING,
			col_varchar VARCHAR(50),
			col_char CHAR(10),
			col_timestamp TIMESTAMP,
			col_date DATE,
			col_binary BINARY,
			col_array ARRAY<STRING>,
			col_map MAP<STRING,STRING>,
			col_struct STRUCT<f1:STRING,f2:INT>,
			col_union UNIONTYPE<STRING,INT>
		)`, tableName)

	cursor.exec(context.Background(), createTableSQL)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Insert a test row
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s VALUES (
			true,
			127,
			32767,
			2147483647,
			9223372036854775807,
			3.14,
			3.14159265359,
			1234.56,
			'string value',
			'varchar value',
			'char value',
			'2024-03-20 12:34:56',
			'2024-03-20',
			'binary data',
			array('array', 'values'),
			map('key1', 'value1', 'key2', 'value2'),
			named_struct('f1', 'struct value', 'f2', 42),
			create_union(0, 'union value', 0)
		)`, tableName)

	cursor.exec(context.Background(), insertSQL)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Query the table
	querySQL := fmt.Sprintf("SELECT * FROM %s", tableName)
	cursor.exec(context.Background(), querySQL)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Get the rows interface
	rows := &rows{cursor: cursor}

	// Test each column's database type name
	expectedTypes := map[string]string{
		"col_boolean":   "BOOLEAN",
		"col_tinyint":   "TINYINT",
		"col_smallint":  "SMALLINT",
		"col_int":       "INT",
		"col_bigint":    "BIGINT",
		"col_float":     "FLOAT",
		"col_double":    "DOUBLE",
		"col_decimal":   "DECIMAL",
		"col_string":    "STRING",
		"col_varchar":   "VARCHAR",
		"col_char":      "CHAR",
		"col_timestamp": "TIMESTAMP",
		"col_date":      "DATE",
		"col_binary":    "BINARY",
		"col_array":     "ARRAY",
		"col_map":       "MAP",
		"col_struct":    "STRUCT",
		"col_union":     "UNION",
	}

	// Get column names
	columns := rows.Columns()
	if len(columns) == 0 {
		t.Fatal("No columns returned")
	}

	// Create a map of fully qualified column names to their base names
	columnMap := make(map[string]string)
	for _, col := range columns {
		parts := strings.Split(col, ".")
		if len(parts) == 2 {
			columnMap[col] = parts[1]
		} else {
			columnMap[col] = col
		}
	}

	for i, colName := range columns {
		dbType := rows.ColumnTypeDatabaseTypeName(i)
		baseName := columnMap[colName]
		expectedType, exists := expectedTypes[baseName]
		if !exists {
			t.Errorf("Unexpected column name: %s", colName)
			continue
		}

		if dbType != expectedType {
			t.Errorf("Column %s: expected database type %s, got %s", colName, expectedType, dbType)
		}
	}

	// Verify we tested all expected types
	for expectedCol := range expectedTypes {
		found := false
		for _, col := range columns {
			if columnMap[col] == expectedCol {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected column %s not found in result set", expectedCol)
		}
	}
}

func TestSQLResultMethods(t *testing.T) {
	// Create a new connection
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" || ssl {
		t.Skip("not testing this combination")
	}

	db, err := sql.Open("hive", fmt.Sprintf("hive://hs2.example.com:10000/default?auth=%s", auth))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	// Create a test table with an ID column
	tableName := fmt.Sprintf("test_result_table_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id INT,
			value STRING
		)
	`, tableName)
	_, err = db.Exec(createSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))

	// Test LastInsertId
	insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (1, 'test')", tableName)
	result, err := db.Exec(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	lastID, err := result.LastInsertId()
	if err == nil {
		t.Logf("LastInsertId returned: %d", lastID)
	} else {
		t.Logf("LastInsertId error (expected): %v", err)
	}

	// Test RowsAffected
	updateSQL := fmt.Sprintf("UPDATE %s SET value = 'updated' WHERE id = 1", tableName)
	result, err = db.Exec(updateSQL)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err == nil {
		t.Logf("RowsAffected returned: %d", rowsAffected)
	} else {
		t.Logf("RowsAffected error (expected): %v", err)
	}
}
