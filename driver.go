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
	var sslCertFile, sslKeyFile string
	var insecureSkipVerify bool

	// Create configuration
	config := newConnectConfiguration()
	config.Username = username
	config.Password = password
	config.Database = database

	// Parse query parameters if present
	if len(dbParts) > 1 {
		params := strings.Split(dbParts[1], "&")
		for _, param := range params {
			if strings.HasPrefix(param, "auth=") {
				auth = strings.TrimPrefix(param, "auth=")
			} else if strings.HasPrefix(param, "sslcert=") {
				sslCertFile = strings.TrimPrefix(param, "sslcert=")
			} else if strings.HasPrefix(param, "sslkey=") {
				sslKeyFile = strings.TrimPrefix(param, "sslkey=")
			} else if strings.HasPrefix(param, "insecure_skip_verify=") {
				insecureSkipVerify = strings.TrimPrefix(param, "insecure_skip_verify=") == "true"
			} else if strings.HasPrefix(param, "transport=") {
				config.TransportMode = strings.TrimPrefix(param, "transport=")
			}
		}
	}

	// Default transport to binary if not provided
	if config.TransportMode == "" {
		config.TransportMode = "binary"
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

	// Set service name for Kerberos authentication
	if auth == "KERBEROS" {
		config.Service = "hive"
	}

	// Configure SSL if paths are provided
	if sslCertFile != "" && sslKeyFile != "" {
		tlsConfig, err := getTlsConfiguration(sslCertFile, sslKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SSL: %v", err)
		}
		tlsConfig.InsecureSkipVerify = insecureSkipVerify
		config.TLSConfig = tlsConfig
	}

	// Connect to Hive
	conn, err := connect(context.Background(), hostname, port, auth, config)
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
	conn *connection
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
	return c.conn.close()
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

	cursor := s.conn.conn.cursor()

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

	cursor := s.conn.conn.cursor()

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
	return -1, nil
}

// RowsAffected returns the number of rows affected by the query.
func (r *result) RowsAffected() (int64, error) {
	if r.cursor.operationHandle != nil && r.cursor.operationHandle.ModifiedRowCount != nil {
		return int64(*r.cursor.operationHandle.ModifiedRowCount), nil
	}
	return -1, nil
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
	if r.cursor != nil {
		r.cursor.close()
	}
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
	// Remove _TYPE suffix if present
	hiveType = strings.TrimSuffix(strings.ToUpper(hiveType), "_TYPE")
	switch hiveType {
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
	case "ARRAY", "MAP", "STRUCT", "UNION":
		return reflect.TypeOf("")
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
	return strings.TrimSuffix(desc[index][1], "_TYPE")
}

func init() {
	sql.Register("hive", &Driver{})
}
