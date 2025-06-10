package gohive

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Driver is the interface that must be implemented by a database driver.
type Driver struct{}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// Parse the DSN (Data Source Name)
	// Format: "hive://username:password@host:port/database?param1=value1&param2=value2"
	if !strings.HasPrefix(name, "hive://") {
		return nil, fmt.Errorf("invalid DSN format: must start with hive://")
	}

	// Remove the hive:// prefix
	name = strings.TrimPrefix(name, "hive://")

	// Split into userinfo and host
	var userinfo, host string
	if strings.Contains(name, "@") {
		parts := strings.SplitN(name, "@", 2)
		userinfo = parts[0]
		host = parts[1]
	} else {
		userinfo = ""
		host = name
	}

	// Parse userinfo - now optional
	var username, password string
	if userinfo != "" {
		if strings.Contains(userinfo, ":") {
			userParts := strings.SplitN(userinfo, ":", 2)
			username = userParts[0]
			password = userParts[1]
		} else {
			username = userinfo
		}
	}

	// Parse host and database
	hostParts := strings.SplitN(host, "/", 2)
	if len(hostParts) != 2 {
		return nil, fmt.Errorf("invalid DSN format: missing database")
	}

	hostPort := hostParts[0]
	databaseAndParams := hostParts[1]

	// Split database and query parameters
	dbParts := strings.SplitN(databaseAndParams, "?", 2)
	database := dbParts[0]

	// Default auth to NONE if not specified
	auth := "NONE"

	// Parse query parameters if present
	if len(dbParts) > 1 {
		params := strings.Split(dbParts[1], "&")
		for _, param := range params {
			if strings.HasPrefix(param, "auth=") {
				auth = strings.TrimPrefix(param, "auth=")
				break
			}
		}
	}

	// Parse host and port
	hostPortParts := strings.Split(hostPort, ":")
	if len(hostPortParts) != 2 {
		return nil, fmt.Errorf("invalid DSN format: missing port")
	}

	hostname := hostPortParts[0]
	portStr := hostPortParts[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port number: %v", err)
	}

	// Create configuration
	config := newConnectConfiguration()
	config.Username = username
	config.Password = password
	config.Database = database

	// Set service name for Kerberos authentication
	if auth == "KERBEROS" {
		config.Service = "hive"
	}

	// Connect to Hive
	conn, err := connect(hostname, port, auth, config)
	if err != nil {
		return nil, err
	}

	return &connector{conn: conn}, nil
}

// connector implements driver.Connector
type connector struct {
	conn *connection
}

// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &sqlConnection{conn: c.conn}, nil
}

// Driver returns the underlying Driver of the Connector.
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

// sqlConnection implements driver.Conn
type sqlConnection struct {
	conn   *connection
	cursor *cursor // Single cursor per connection
	mu     sync.Mutex
}

// Prepare returns a prepared statement, bound to this connection.
func (c *sqlConnection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *sqlConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *sqlConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cursor != nil {
		c.cursor.close()
		c.cursor = nil
	}
	return nil
}

// Begin starts and returns a new transaction.
func (c *sqlConnection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
func (c *sqlConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported")
}

// stmt implements driver.Stmt
type stmt struct {
	conn  *sqlConnection
	query string
}

// Close closes the statement.
func (s *stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *stmt) NumInput() int {
	// Count the number of ? placeholders in the query
	return strings.Count(s.query, "?")
}

// Exec executes a query that doesn't return rows.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), args)
}

// ExecContext executes a query that doesn't return rows.
func (s *stmt) ExecContext(ctx context.Context, args []driver.Value) (driver.Result, error) {
	query := s.query
	if len(args) > 0 {
		// Replace ? placeholders with actual values
		for _, arg := range args {
			var value string
			switch v := arg.(type) {
			case string:
				value = "'" + strings.ReplaceAll(v, "'", "''") + "'"
			case time.Time:
				value = "'" + v.Format("2006-01-02 15:04:05") + "'"
			case nil:
				value = "NULL"
			default:
				value = fmt.Sprintf("%v", v)
			}
			query = strings.Replace(query, "?", value, 1)
		}
	}

	s.conn.mu.Lock()
	if s.conn.cursor == nil {
		s.conn.cursor = s.conn.conn.cursor()
	}
	cursor := s.conn.cursor
	s.conn.mu.Unlock()

	cursor.exec(ctx, query)
	if cursor.error() != nil {
		return nil, cursor.error()
	}
	return &result{cursor: cursor, query: query}, nil
}

// Query executes a query that may return rows.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), args)
}

// QueryContext executes a query that may return rows.
func (s *stmt) QueryContext(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	query := s.query
	if len(args) > 0 {
		// Replace ? placeholders with actual values
		for _, arg := range args {
			var value string
			switch v := arg.(type) {
			case string:
				value = "'" + strings.ReplaceAll(v, "'", "''") + "'"
			case time.Time:
				value = "'" + v.Format("2006-01-02 15:04:05") + "'"
			case nil:
				value = "NULL"
			default:
				value = fmt.Sprintf("%v", v)
			}
			query = strings.Replace(query, "?", value, 1)
		}
	}

	s.conn.mu.Lock()
	if s.conn.cursor == nil {
		s.conn.cursor = s.conn.conn.cursor()
	}
	cursor := s.conn.cursor
	s.conn.mu.Unlock()

	cursor.exec(ctx, query)
	if cursor.error() != nil {
		return nil, cursor.error()
	}
	return &rows{cursor: cursor}, nil
}

// result implements driver.Result
type result struct {
	cursor *cursor
	query  string
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary key.
func (r *result) LastInsertId() (int64, error) {
	// Hive doesn't support auto-generated IDs, but we can try to get the last inserted ID
	// if the query was an INSERT with an explicit ID
	if strings.HasPrefix(strings.ToUpper(r.query), "INSERT") {
		// Try to extract the ID from the last row
		if r.cursor.hasMore(context.Background()) {
			row := r.cursor.rowMap(context.Background())
			if id, ok := row["id"].(int64); ok {
				return id, nil
			}
		}
	}
	return 0, fmt.Errorf("LastInsertId is not supported for this operation")
}

// RowsAffected returns the number of rows affected by the query.
func (r *result) RowsAffected() (int64, error) {
	// For INSERT, UPDATE, DELETE operations, try to get the number of affected rows
	query := strings.ToUpper(r.query)
	if strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "UPDATE") || strings.HasPrefix(query, "DELETE") {
		// Try to get the number of affected rows from the cursor
		if r.cursor.hasMore(context.Background()) {
			row := r.cursor.rowMap(context.Background())
			if count, ok := row["count"].(int64); ok {
				return count, nil
			}
		}
	}
	return 0, fmt.Errorf("RowsAffected is not supported for this operation")
}

// rows implements driver.Rows
type rows struct {
	cursor *cursor
}

// Columns returns the names of the columns.
func (r *rows) Columns() []string {
	desc := r.cursor.description()
	columns := make([]string, len(desc))
	for i, col := range desc {
		columns[i] = col[0]
	}
	return columns
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice.
func (r *rows) Next(dest []driver.Value) error {
	// Defensive: always use a valid context
	if r.cursor == nil {
		return sql.ErrNoRows
	}
	if !r.cursor.hasMore(context.Background()) {
		return io.EOF
	}

	row := r.cursor.rowMap(context.Background())
	if r.cursor.error() != nil {
		return r.cursor.error()
	}

	columns := r.Columns()
	desc := r.cursor.description()
	for i := range dest {
		colName := columns[i]
		val := row[colName]

		// Handle NULL values
		if val == nil {
			dest[i] = nil
			continue
		}

		// Use column type from description
		var colType string
		if i < len(desc) {
			colType = strings.ToUpper(desc[i][1])
		}

		// Accept both TIMESTAMP and TIMESTAMP_TYPE, DATE and DATE_TYPE
		isTimestamp := colType == "TIMESTAMP" || colType == "TIMESTAMP_TYPE"
		isDate := colType == "DATE" || colType == "DATE_TYPE"

		switch v := val.(type) {
		case string:
			if isTimestamp {
				t, err := time.Parse("2006-01-02 15:04:05", v)
				if err == nil {
					dest[i] = t
					continue
				}
			}
			if isDate {
				t, err := time.Parse("2006-01-02", v)
				if err == nil {
					dest[i] = t
					continue
				}
			}
			dest[i] = v
		case int64:
			dest[i] = v
		case float64:
			dest[i] = v
		case bool:
			dest[i] = v
		case []byte:
			dest[i] = v
		case time.Time:
			dest[i] = v
		default:
			// For any other type, convert to string
			dest[i] = fmt.Sprintf("%v", v)
		}
	}

	return nil
}

// ColumnTypeScanType returns the Go type that should be used to scan values into.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	desc := r.cursor.description()
	if index >= len(desc) {
		return reflect.TypeOf(nil)
	}
	// Map Hive types to Go types
	hiveType := desc[index][1]
	switch strings.ToUpper(hiveType) {
	case "BOOLEAN":
		return reflect.TypeOf(false)
	case "TINYINT":
		return reflect.TypeOf(int8(0))
	case "SMALLINT":
		return reflect.TypeOf(int16(0))
	case "INT":
		return reflect.TypeOf(int32(0))
	case "BIGINT":
		return reflect.TypeOf(int64(0))
	case "FLOAT":
		return reflect.TypeOf(float32(0))
	case "DOUBLE":
		return reflect.TypeOf(float64(0))
	case "DECIMAL":
		return reflect.TypeOf("")
	case "STRING", "VARCHAR", "CHAR":
		return reflect.TypeOf("")
	case "TIMESTAMP":
		return reflect.TypeOf(time.Time{})
	case "DATE":
		return reflect.TypeOf(time.Time{})
	case "BINARY":
		return reflect.TypeOf([]byte{})
	default:
		return reflect.TypeOf("")
	}
}

// ColumnTypeDatabaseTypeName returns the database system type name.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	desc := r.cursor.description()
	if index >= len(desc) {
		return ""
	}
	return desc[index][1]
}

func init() {
	sql.Register("hive", &Driver{})
}
