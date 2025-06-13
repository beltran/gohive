package gohive

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"reflect"
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
	// Parse the DSN
	dsn, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}

	// Create configuration
	config := newConnectConfiguration()
	config.Username = dsn.Username
	config.Password = dsn.Password
	config.Database = dsn.Database
	config.TransportMode = dsn.TransportMode
	config.Service = dsn.Service

	// Configure SSL if paths are provided
	if dsn.SSLCertFile != "" && dsn.SSLKeyFile != "" {
		tlsConfig, err := getTlsConfiguration(dsn.SSLCertFile, dsn.SSLKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SSL: %v", err)
		}
		tlsConfig.InsecureSkipVerify = dsn.SSLInsecureSkip
		config.TLSConfig = tlsConfig
	}

	return &connector{host: dsn.Host, port: dsn.Port, auth: dsn.Auth, config: config}, nil
}

// connector implements driver.Connector
type connector struct {
	host   string
	port   int
	auth   string
	config *connectConfiguration
}

// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Connect to Hive
	conn, err := connect(ctx, c.host, c.port, c.auth, c.config)
	if err != nil {
		return nil, err
	}
	return &sqlConnection{conn: conn}, nil
}

// Driver returns the underlying Driver of the Connector.
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

// sqlConnection implements driver.Conn
type sqlConnection struct {
	conn *connection
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
	return nil, driver.ErrSkip
}

// Prepare returns a prepared statement, bound to this connection.
func (c *sqlConnection) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are not supported by Hive")
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *sqlConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are not supported by Hive")
}

// Exec executes a query that doesn't return rows.
// Implements driver.Execer
func (c *sqlConnection) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, args)
}

// ExecContext executes a query that doesn't return rows.
func (c *sqlConnection) ExecContext(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("query parameters are not supported by Hive")
	}

	cursor := c.conn.cursor()
	cursor.exec(ctx, query)
	if cursor.error() != nil {
		return nil, cursor.error()
	}
	return &result{cursor: cursor, query: query}, nil
}

// Query executes a query that may return rows.
// Implements driver.Queryer
func (c *sqlConnection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, args)
}

// QueryContext executes a query that may return rows.
func (c *sqlConnection) QueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("query parameters are not supported by Hive")
	}

	cursor := c.conn.cursor()
	cursor.exec(ctx, query)
	if cursor.error() != nil {
		return nil, cursor.error()
	}
	return &rows{cursor: cursor, ctx: ctx}, nil
}

// result implements driver.Result
type result struct {
	cursor *cursor
	query  string
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary key.
func (r *result) LastInsertId() (int64, error) {
	return 0, driver.ErrSkip
}

// RowsAffected returns the number of rows affected by the query.
func (r *result) RowsAffected() (int64, error) {
	if r.cursor.operationHandle != nil && r.cursor.operationHandle.ModifiedRowCount != nil {
		return int64(*r.cursor.operationHandle.ModifiedRowCount), nil
	}
	return 0, driver.ErrSkip
}

// rows implements driver.Rows
type rows struct {
	cursor *cursor
	ctx    context.Context
}

// Columns returns the names of the columns.
func (r *rows) Columns() []string {
	desc := r.cursor.description(r.ctx)
	columns := make([]string, len(desc))
	for i, col := range desc {
		columns[i] = col[0]
	}
	return columns
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	if r.cursor != nil {
		r.cursor.close(r.ctx)
	}
	return nil
}

// Next is called to populate the next row of data into
// the provided slice.
func (r *rows) Next(dest []driver.Value) error {
	if r.cursor == nil {
		return io.EOF
	}

	if !r.cursor.hasMore(r.ctx) {
		return io.EOF
	}

	// Create a slice of pointers to hold the values
	ptrs := make([]interface{}, len(dest))
	for i := range dest {
		ptrs[i] = &dest[i]
	}

	// Fetch the row directly into the destination slice
	r.cursor.fetchOne(r.ctx, ptrs...)
	if r.cursor.Err != nil {
		log.Printf("Error in fetchOne: %v", r.cursor.Err)
		return r.cursor.Err
	}

	return nil
}

// ColumnTypeScanType returns the scan type for the given column index
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if r.cursor == nil {
		return nil
	}
	desc := r.cursor.description(context.Background())
	if r.cursor.Err != nil || index >= len(desc) {
		return nil
	}
	colType := strings.TrimSuffix(strings.ToUpper(desc[index][1]), "_TYPE")
	switch colType {
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
		return reflect.TypeOf(float32(0)) // Return float32 for FLOAT type
	case "DOUBLE":
		return reflect.TypeOf(float64(0))
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
	desc := r.cursor.description(r.ctx)
	if index >= len(desc) {
		return ""
	}
	return strings.TrimSuffix(desc[index][1], "_TYPE")
}

func init() {
	sql.Register("hive", &Driver{})
}
