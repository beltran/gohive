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

func TestSQLDirectInsert(t *testing.T) {
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

	// Insert data directly
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (4, 'test4')", tableName))
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

	// Insert a row with a timestamp directly
	testTime := time.Date(2024, 3, 20, 12, 34, 56, 0, time.UTC)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, '%s')", tableName, testTime.Format("2006-01-02 15:04:05")))
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

	// Insert data and check rows affected
	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2')", tableName))
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify table contents directly
	verifyRows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to verify table contents: %v", err)
	}
	defer verifyRows.Close()

	// Get column information
	cols, err := verifyRows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	t.Logf("Verification columns: %v", cols)

	// Verify column names (they come with table prefix)
	expectedCols := []string{
		fmt.Sprintf("%s.id", tableName),
		fmt.Sprintf("%s.name", tableName),
	}
	if !reflect.DeepEqual(cols, expectedCols) {
		t.Errorf("Expected columns %v, got %v", expectedCols, cols)
	}

	// Get column types
	columnTypes, err := verifyRows.ColumnTypes()
	if err != nil {
		t.Fatalf("Failed to get column types: %v", err)
	}

	// Verify column types
	expectedTypes := []struct {
		name     string
		dbType   string
		scanType reflect.Type
	}{
		{"id", "INT", reflect.TypeOf(int32(0))},
		{"name", "STRING", reflect.TypeOf("")},
	}

	if len(columnTypes) != len(expectedTypes) {
		t.Errorf("Expected %d column types, got %d", len(expectedTypes), len(columnTypes))
	}

	for i, colType := range columnTypes {
		if i >= len(expectedTypes) {
			continue
		}
		expected := expectedTypes[i]

		// Check database type name
		if dbType := colType.DatabaseTypeName(); dbType != expected.dbType {
			t.Errorf("Column %s: got database type %q, want %q", expected.name, dbType, expected.dbType)
		}

		// Check scan type
		if scanType := colType.ScanType(); scanType != expected.scanType {
			t.Errorf("Column %s: got scan type %v, want %v", expected.name, scanType, expected.scanType)
		}
	}

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

	// Create timestamps in UTC
	now := time.Now().UTC()
	createdAt := now.Format("2006-01-02 15:04:05")
	birthDate := now.Format("2006-01-02")

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
	`, tableName, 1, "John Doe", 30, 1.75, 75.5, true, createdAt, birthDate, "Software Engineer", "example@example.com")

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

	// Compare timestamps in UTC
	expectedCreatedAt, _ := time.Parse("2006-01-02 15:04:05", createdAt)
	if !colCreatedAt.Equal(expectedCreatedAt) {
		t.Errorf("got col_created_at=%v, want %v", colCreatedAt, expectedCreatedAt)
	}

	expectedBirthDate, _ := time.Parse("2006-01-02", birthDate)
	if !colBirthDate.Equal(expectedBirthDate) {
		t.Errorf("got col_birth_date=%v, want %v", colBirthDate, expectedBirthDate)
	}

	if colNotes != "Software Engineer" {
		t.Errorf("got col_notes=%s, want Software Engineer", colNotes)
	}
	if string(colData) != "example@example.com" {
		t.Errorf("got col_data=%s, want example@example.com", string(colData))
	}
}

func TestSQLConnectionProperties(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()

	// Create DSN
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)

	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set properties using SET command
	_, err = db.Exec("SET hadoop.madeup.one=value1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("SET hadoop.madeup.two=value2")
	if err != nil {
		t.Fatal(err)
	}

	// Verify properties are set in the session
	var propValue string
	err = db.QueryRow("SET hadoop.madeup.one").Scan(&propValue)
	if err != nil {
		t.Fatal(err)
	}
	if propValue != "hadoop.madeup.one=value1" {
		t.Errorf("Property value mismatch: got %s, want hadoop.madeup.two=value1", propValue)
	}

	err = db.QueryRow("SET hadoop.madeup.two").Scan(&propValue)
	if err != nil {
		t.Fatal(err)
	}
	if propValue != "hadoop.madeup.two=value2" {
		t.Errorf("Property value mismatch: got %s, want hadoop.madeup.two=value2", propValue)
	}
}

func TestSQLShowDatabases(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)

	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		t.Fatalf("Failed to execute SHOW DATABASES: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			t.Fatalf("Failed to scan database name: %v", err)
		}
		t.Logf("Database: %s", dbName)
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
	if count == 0 {
		t.Error("Expected at least one database, got 0")
	}
}

func TestSQLUseDatabase(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)

	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Drop table and database if they exist
	_, err = db.Exec("DROP TABLE IF EXISTS db.pokes")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("DROP DATABASE IF EXISTS db")
	if err != nil {
		t.Fatal(err)
	}

	// Create database and table
	_, err = db.Exec("CREATE DATABASE db")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE db.pokes (foo INT, bar INT)")
	if err != nil {
		t.Fatal(err)
	}

	// Use the database
	_, err = db.Exec("USE db")
	if err != nil {
		t.Fatal(err)
	}

	// Insert a row
	_, err = db.Exec("INSERT INTO pokes VALUES(1, 1111)")
	if err != nil {
		t.Fatal(err)
	}

	// Query using unqualified table name
	rows, err := db.Query("SELECT * FROM pokes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var foo, bar int32
	if rows.Next() {
		err = rows.Scan(&foo, &bar)
		if err != nil {
			t.Fatal(err)
		}
		if foo != 1 || bar != 1111 {
			t.Errorf("Expected foo=1, bar=1111, got foo=%d, bar=%d", foo, bar)
		}
	} else {
		t.Fatal("No rows returned")
	}

	// Query using qualified table name
	rows, err = db.Query("SELECT * FROM db.pokes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&foo, &bar)
		if err != nil {
			t.Fatal(err)
		}
		if foo != 1 || bar != 1111 {
			t.Errorf("Expected foo=1, bar=1111, got foo=%d, bar=%d", foo, bar)
		}
	} else {
		t.Fatal("No rows returned")
	}

	// Clean up
	_, err = db.Exec("DROP TABLE IF EXISTS db.pokes")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("DROP DATABASE IF EXISTS db")
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLColumnTypeDatabaseTypeName(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_column_types")

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
			col_varchar VARCHAR(100),
			col_char CHAR(10),
			col_timestamp TIMESTAMP,
			col_date DATE,
			col_binary BINARY,
			col_array ARRAY<INT>,
			col_map MAP<INT,STRING>,
			col_struct STRUCT<a:INT,b:STRING>,
			col_union UNIONTYPE<INT,STRING>
		)
	`, tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Query the table to get column types
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	// Expected type names (without _TYPE suffix)
	expectedTypes := []string{
		"BOOLEAN",
		"TINYINT",
		"SMALLINT",
		"INT",
		"BIGINT",
		"FLOAT",
		"DOUBLE",
		"DECIMAL",
		"STRING",
		"VARCHAR",
		"CHAR",
		"TIMESTAMP",
		"DATE",
		"BINARY",
		"ARRAY",
		"MAP",
		"STRUCT",
		"UNION",
	}

	// Verify each column's database type name
	for i, colType := range columnTypes {
		if i >= len(expectedTypes) {
			t.Errorf("Unexpected column at index %d", i)
			continue
		}

		dbTypeName := colType.DatabaseTypeName()
		expectedType := expectedTypes[i]
		if dbTypeName != expectedType {
			t.Errorf("Column %d: got type %q, want %q", i, dbTypeName, expectedType)
		}
	}

	if len(columnTypes) != len(expectedTypes) {
		t.Errorf("Got %d columns, want %d", len(columnTypes), len(expectedTypes))
	}
}

func TestSQLColumnTypeScanType(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_scan_types")

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

	// Query the table to get column types
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	// Expected Go types for scanning
	expectedTypes := []reflect.Type{
		reflect.TypeOf(false),       // BOOLEAN
		reflect.TypeOf(int8(0)),     // TINYINT
		reflect.TypeOf(int16(0)),    // SMALLINT
		reflect.TypeOf(int32(0)),    // INT
		reflect.TypeOf(int64(0)),    // BIGINT
		reflect.TypeOf(float32(0)),  // FLOAT
		reflect.TypeOf(float64(0)),  // DOUBLE
		reflect.TypeOf(""),          // DECIMAL
		reflect.TypeOf(""),          // STRING
		reflect.TypeOf(time.Time{}), // TIMESTAMP
		reflect.TypeOf(time.Time{}), // DATE
		reflect.TypeOf([]byte{}),    // BINARY
	}

	// Verify each column's scan type
	for i, colType := range columnTypes {
		if i >= len(expectedTypes) {
			t.Errorf("Unexpected column at index %d", i)
			continue
		}

		scanType := colType.ScanType()
		expectedType := expectedTypes[i]
		if scanType != expectedType {
			t.Errorf("Column %d: got scan type %v, want %v", i, scanType, expectedType)
		}
	}

	if len(columnTypes) != len(expectedTypes) {
		t.Errorf("Got %d columns, want %d", len(columnTypes), len(expectedTypes))
	}
}

func TestSQLMultipleCursors(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := getTestTableName("test_multiple_cursors")

	// Create a test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING)", tableName))
	if err != nil {
		t.Fatalf("error creating table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert test data
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')", tableName))
	if err != nil {
		t.Fatalf("error inserting data: %v", err)
	}

	// Create first cursor
	rows1, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id <= 2", tableName))
	if err != nil {
		t.Fatalf("error querying with first cursor: %v", err)
	}
	defer rows1.Close()

	// Create second cursor while first is still open
	rows2, err := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE id > 2", tableName))
	if err != nil {
		t.Fatalf("error querying with second cursor: %v", err)
	}
	defer rows2.Close()

	// Read from first cursor
	var id1 int
	var name1 string
	var count1 int
	for rows1.Next() {
		err = rows1.Scan(&id1, &name1)
		if err != nil {
			t.Fatalf("error scanning first cursor: %v", err)
		}
		count1++
		if id1 > 2 {
			t.Errorf("first cursor got id=%d, want id <= 2", id1)
		}
	}
	if err := rows1.Err(); err != nil {
		t.Fatalf("error iterating first cursor: %v", err)
	}
	if count1 != 2 {
		t.Errorf("first cursor got %d rows, want 2", count1)
	}

	// Read from second cursor
	var id2 int
	var name2 string
	var count2 int
	for rows2.Next() {
		err = rows2.Scan(&id2, &name2)
		if err != nil {
			t.Fatalf("error scanning second cursor: %v", err)
		}
		count2++
		if id2 <= 2 {
			t.Errorf("second cursor got id=%d, want id > 2", id2)
		}
	}
	if err := rows2.Err(); err != nil {
		t.Fatalf("error iterating second cursor: %v", err)
	}
	if count2 != 1 {
		t.Errorf("second cursor got %d rows, want 1", count2)
	}

	// Create a third cursor after closing the first two
	rows3, err := db.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		t.Fatalf("error querying with third cursor: %v", err)
	}
	defer rows3.Close()

	// Read from third cursor
	var totalCount int
	if !rows3.Next() {
		t.Fatal("expected a row from third cursor")
	}
	err = rows3.Scan(&totalCount)
	if err != nil {
		t.Fatalf("error scanning third cursor: %v", err)
	}
	if totalCount != 3 {
		t.Errorf("third cursor got count=%d, want 3", totalCount)
	}
}

func TestSQLConnectionPooling(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)

	// Create a DB with specific pool settings
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Configure pool settings
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetConnMaxIdleTime(time.Minute)

	// Verify initial connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Check current database
	var currentDB string
	err = db.QueryRow("SELECT current_database()").Scan(&currentDB)
	if err != nil {
		t.Fatalf("Failed to get current database: %v", err)
	}

	// List all tables in current database
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("Failed to scan table name: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating tables: %v", err)
	}

	// Create a test table with fully qualified name
	tableName := fmt.Sprintf("%s.pool_test", currentDB)
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, value STRING)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert some test data
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')", tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel to coordinate goroutines
	done := make(chan bool)
	errors := make(chan error, 10)

	// Launch multiple concurrent queries
	for i := 0; i < 10; i++ {
		go func(id int) {
			// Each goroutine will perform multiple operations
			for j := 0; j < 3; j++ {
				// Verify connection is still alive before each operation
				if err := db.Ping(); err != nil {
					errors <- fmt.Errorf("goroutine %d ping %d failed: %v", id, j, err)
					return
				}

				// Query operation
				rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
				if err != nil {
					errors <- fmt.Errorf("goroutine %d query %d failed: %v", id, j, err)
					return
				}
				rows.Close()

				// Exec operation
				_, err = db.Exec(fmt.Sprintf("SELECT * FROM %s", tableName))
				if err != nil {
					errors <- fmt.Errorf("goroutine %d exec %d failed: %v", id, j, err)
					return
				}

				// QueryRow operation
				var value string
				err = db.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE id = 1", tableName)).Scan(&value)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d queryrow %d failed: %v", id, j, err)
					return
				}

				// Small delay between operations to prevent overwhelming the connection
				time.Sleep(50 * time.Millisecond)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete or timeout
	timeout := time.After(30 * time.Second)
	completed := 0
	for {
		select {
		case err := <-errors:
			t.Fatal(err)
		case <-done:
			completed++
			if completed == 10 {
				return // All goroutines completed successfully
			}
		case <-timeout:
			t.Fatalf("Test timed out after 30 seconds. Completed %d/10 goroutines", completed)
		}
	}
}

func TestSQLConnectionPoolingTx(t *testing.T) {
	auth := getSQLAuth()
	transport := getSQLTransport()
	ssl := getSQLSsl()
	dsn := buildDSN("hs2.example.com", 10000, "default", auth, transport, ssl, true)

	// Create a DB with specific pool settings
	db, err := sql.Open("hive", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Configure pool settings
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetConnMaxIdleTime(time.Minute)

	// Verify initial connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	var currentDB string
	err = db.QueryRow("SELECT current_database()").Scan(&currentDB)
	if err != nil {
		t.Fatalf("Failed to get current database: %v", err)
	}

	// Create a test table with fully qualified name
	tableName := fmt.Sprintf("%s.pool_test", currentDB)
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, value STRING)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert some test data
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')", tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel to coordinate goroutines
	done := make(chan bool)
	errors := make(chan error, 10)

	// Launch multiple concurrent queries
	for i := 0; i < 10; i++ {
		go func(id int) {
			// Each goroutine will perform multiple operations
			for j := 0; j < 3; j++ {

				// Verify connection is still alive before each operation
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d begin %d failed: %v", id, j, err)
					return
				}

				// Query operation
				expectedPropValue := fmt.Sprintf("hadoop.madeup.one=%d", i*j)
				_, err = tx.Exec(fmt.Sprintf("SET %s", expectedPropValue))
				if err != nil {
					errors <- fmt.Errorf("goroutine %d query %d failed: %v", id, j, err)
					return
				}

				var propValue string
				err = tx.QueryRow("SET hadoop.madeup.one").Scan(&propValue)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d queryrow %d failed: %v", id, j, err)
					return
				}
				if expectedPropValue != propValue {
					errors <- fmt.Errorf("goroutine %d queryrow %d failed: %v", id, j, err)
					return
				}

				if err = tx.Commit(); err != nil {
					errors <- fmt.Errorf("goroutine %d commit %d failed: %v", id, j, err)
					return
				}

				// Small delay between operations to prevent overwhelming the connection
				time.Sleep(50 * time.Millisecond)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete or timeout
	timeout := time.After(30 * time.Second)
	completed := 0
	for {
		select {
		case err := <-errors:
			t.Fatal(err)
		case <-done:
			completed++
			if completed == 10 {
				return // All goroutines completed successfully
			}
		case <-timeout:
			t.Fatalf("Test timed out after 30 seconds. Completed %d/10 goroutines", completed)
		}
	}
}
