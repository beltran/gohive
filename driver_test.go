//go:build all || integration
// +build all integration

package gohive

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
		transport = "binary"
	}
	return transport
}

func getSQLSsl() bool {
	ssl := os.Getenv("SSL")
	return ssl == "1"
}

// buildDSN constructs a DSN string with the given parameters
func buildDSN(host string, port int, database string, auth string, transport string, ssl bool, insecureSkipVerify bool) string {
	dsn := fmt.Sprintf("hive://%s:%d/%s?auth=%s&transport=%s", host, port, database, auth, transport)
	if ssl {
		dsn += "&sslcert=client.cer.pem&sslkey=client.cer.key"
		if insecureSkipVerify {
			dsn += "&sslinsecureskipverify=true"
		}
	}
	return dsn
}

// buildDSNWithCredentials constructs a DSN string with username and password
func buildDSNWithCredentials(username, password, host string, port int, database string, auth string, transport string, ssl bool, insecureSkipVerify bool) string {
	dsn := fmt.Sprintf("hive://%s:%s@%s:%d/%s?auth=%s&transport=%s", username, password, host, port, database, auth, transport)
	if ssl {
		dsn += "&sslcert=client.cer.pem&sslkey=client.cer.key"
		if insecureSkipVerify {
			dsn += "&sslinsecureskipverify=true"
		}
	}
	return dsn
}

func TestSQLDriverAuthNone(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "NONE" || transport != "binary" {
		t.Skip("not testing this combination")
	}

	// Test with NONE configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Username = "username"
	config.Password = "password"
	config.Database = "default"

	dsn := buildDSNWithCredentials(config.Username, config.Password, "hs2.example.com", 10000, config.Database, auth, transport, ssl, true)
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

func TestSQLDriverAuthKerberos(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "KERBEROS" || transport != "binary" {
		t.Skip("not testing this combination")
	}

	// Test with Kerberos configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	dsn := buildDSN("hs2.example.com", 10000, config.Database, auth, transport, ssl, true)
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

func TestSQLDriverAuthDigestMd5(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "DIGEST-MD5" || transport != "binary" {
		t.Skip("not testing this combination")
	}

	// Test with DIGEST-MD5 configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	dsn := buildDSN("hs2.example.com", 10000, config.Database, auth, transport, ssl, true)
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

func TestSQLDriverAuthPlain(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "PLAIN" || transport != "binary" {
		t.Skip("not testing this combination")
	}

	// Test with PLAIN configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	dsn := buildDSN("hs2.example.com", 10000, config.Database, auth, transport, ssl, true)
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

func TestSQLDriverAuthGssapi(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if auth != "GSSAPI" || transport != "binary" {
		t.Skip("not testing this combination")
	}

	// Test with GSSAPI configuration
	config := newConnectConfiguration()
	config.Service = "hive"
	config.Database = "default"

	dsn := buildDSN("hs2.example.com", 10000, config.Database, auth, transport, ssl, true)
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

func TestSQLDriverInvalidHost(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	// Test connection to a non-existent host
	dsn := buildDSN("nonexistent.example.com", 12345, "default", auth, transport, ssl, true)

	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	// Open a connection using the SQL interface
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	if auth != "NONE" || transport != "binary" {
		t.Skip("not testing this combination")
	}

	// Test without credentials in DSN
	db, err := sql.Open("hive", buildDSN("hs2.example.com", 10000, "default", auth, transport, true, true))
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
			dsn:         buildDSN("hs2.example.com", 10000, "default", auth, "binary", true, true),
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

	// Test with multiple query parameters
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, true, true) + "&service=hive"
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("test_table_%d", time.Now().UnixNano())
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
	transport := getSQLTransport()
	ssl := getSQLSsl()
	// Create a unique table name for this test
	tableName := fmt.Sprintf("test_types_table_%d", time.Now().UnixNano())

	// Connect to the database
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
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
		INSERT INTO %s VALUES (
			%d,
			'%s',
			%d,
			%f,
			%f,
			%t,
			'%s',
			'%s',
			'%s',
			'%s'
		)
	`, tableName, 1, "John Doe", 30, 1.75, 75.5, true, time.Now().Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02"), "Software Engineer", "example@example.com")

	_, err = db.Exec(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
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
		colId        int
		colName      string
		colAge       int16
		colHeight    float32
		colWeight    float64
		colIsActive  bool
		colCreatedAt time.Time
		colBirthDate time.Time
		colNotes     string
		colData      []byte
	)

	err = rows.Scan(&colId, &colName, &colAge, &colHeight, &colWeight, &colIsActive, &colCreatedAt, &colBirthDate, &colNotes, &colData)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the values
	if colId != 1 {
		t.Errorf("got col_id=%d, want 1", colId)
	}
	if colName != "John Doe" {
		t.Errorf("got col_name=%s, want John Doe", colName)
	}
	if colAge != 30 {
		t.Errorf("got col_age=%d, want 30", colAge)
	}
	if colHeight != 1.75 {
		t.Errorf("got col_height=%f, want 1.75", colHeight)
	}
	if colWeight != 75.5 {
		t.Errorf("got col_weight=%f, want 75.5", colWeight)
	}
	if !colIsActive {
		t.Error("expected col_is_active to be true")
	}
	if !colCreatedAt.Equal(time.Now()) {
		t.Errorf("got col_created_at=%v, want %v", colCreatedAt, time.Now())
	}
	if !colBirthDate.Equal(time.Now()) {
		t.Errorf("got col_birth_date=%v, want %v", colBirthDate, time.Now())
	}
	if colNotes != "Software Engineer" {
		t.Errorf("got col_notes=%s, want Software Engineer", colNotes)
	}
	if string(colData) != "example@example.com" {
		t.Errorf("got col_data=%s, want example@example.com", string(colData))
	}
}

func TestSQLDriverInsecureSkipVerify(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	if !ssl {
		t.Skip("SSL not enabled for testing")
	}

	// Test with sslinsecureskipverify=true
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	// Test with sslinsecureskipverify=false
	dsn = buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, false)
	db, err = sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
}
