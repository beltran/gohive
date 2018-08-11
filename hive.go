package gohive

import (
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"hiveserver"
	"os/user"
	"time"
)

const DEFAULT_FETCH_SIZE int64 = 1000

// Connection holds the information for getting a cursor to hive
type Connection struct {
	host                string
	port                int
	username            string
	database            string
	auth                string
	kerberosServiceName string
	password            string
	sessionHandle       *hiveserver.TSessionHandle
	client              *hiveserver.TCLIServiceClient
	configuration       *ConnectConfiguration
}

// ConnectConfiguration is the configuration for the connection
// The fields have to be filled manually but not all of them are required
// Depends on the auth and kind of connection.
type ConnectConfiguration struct {
	Username             string
	Principal            string
	Password             string
	Service              string
	HiveConfiguration    map[string]string
	PollIntervalInMillis int
	FetchSize            int64
}

// NewConnectConfiguration returns a connect configuration, all with empty fields
func NewConnectConfiguration() *ConnectConfiguration {
	return &ConnectConfiguration{
		Username:             "",
		Password:             "",
		Service:              "",
		HiveConfiguration:    nil,
		PollIntervalInMillis: 200,
		FetchSize:            DEFAULT_FETCH_SIZE,
	}
}

// ConnectWithContext to hive server
func ConnectWithContext(ctx context.Context, host string, port int, auth string,
	configuration *ConnectConfiguration) (conn *Connection, err error) {
	socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))
	if err = socket.Open(); err != nil {
		return
	}

	var transport thrift.TTransport

	if configuration == nil {
		configuration = NewConnectConfiguration()
	}
	if configuration.Username == "" {
		_user, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("Can't determine the username")
		}
		configuration.Username = _user.Name
	}

	// password doesn't matter but can't be empty
	if auth == "NOSASL" {
		transport = thrift.NewTBufferedTransport(socket, 4096)
		if transport == nil {
			return
		}
	} else if auth == "NONE" {
		if configuration.Password == "" {
			configuration.Password = "x"
		}
		saslConfiguration := map[string]string{"username": configuration.Username, "password": configuration.Password}
		transport, err = NewTSaslTransport(socket, host, "PLAIN", saslConfiguration)
		if err != nil {
			return
		}
	} else if auth == "KERBEROS" {
		saslConfiguration := map[string]string{"service": configuration.Service}
		transport, err = NewTSaslTransport(socket, host, "GSSAPI", saslConfiguration)
		if err != nil {
			return
		}
	} else {
		panic("Unrecognized auth")
	}
	if err = transport.Open(); err != nil {
		return
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := hiveserver.NewTCLIServiceClientFactory(transport, protocolFactory)

	openSession := hiveserver.NewTOpenSessionReq()
	openSession.ClientProtocol = hiveserver.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V6
	openSession.Configuration = configuration.HiveConfiguration
	openSession.Username = &configuration.Username
	openSession.Password = &configuration.Password
	response, err := client.OpenSession(ctx, openSession)

	if err != nil {
		return
	}

	return &Connection{
		host:                host,
		port:                port,
		database:            "default",
		auth:                auth,
		kerberosServiceName: "",
		sessionHandle:       response.SessionHandle,
		client:              client,
		configuration:       configuration,
	}, nil
}

// Connect is ConnectWithContext with the background context
func Connect(host string, port int, auth string,
	configuration *ConnectConfiguration) (conn *Connection, err error) {
	return ConnectWithContext(context.Background(), host, port, auth, configuration)
}

// Cursor creates a cursor from a connection
func (c *Connection) Cursor() *Cursor {
	return &Cursor{
		conn:  c,
		queue: make([]*hiveserver.TColumn, 0),
	}
}

// CloseWithContext closes a session
func (c *Connection) CloseWithContext(ctx context.Context) error {
	closeRequest := hiveserver.NewTCloseSessionReq()
	closeRequest.SessionHandle = c.sessionHandle
	responseClose, err := c.client.CloseSession(ctx, closeRequest)
	if err != nil {
		return err
	}
	if !success(responseClose.GetStatus()) {
		return fmt.Errorf("Error closing the session: %s", responseClose.Status.String())
	}
	return nil
}

// Close is CloseWithContext with the background context
func (c *Connection) Close() error {
	return c.CloseWithContext(context.Background())
}

// Cursor is used for fetching the rows after a query
type Cursor struct {
	conn            *Connection
	operationHandle *hiveserver.TOperationHandle
	queue           []*hiveserver.TColumn
	response        *hiveserver.TFetchResultsResp
	columnIndex     int
	totalRows       int
}

// ExecuteWithContext sends a query to hive for execution with a context
func (c *Cursor) ExecuteWithContext(ctx context.Context, query string) (err error) {
	
	c.resetState(ctx)
	
	executeReq := hiveserver.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.conn.sessionHandle
	executeReq.Statement = query

	// The context from thrift doesn't seem to work
	done := make(chan interface{})
	defer close(done)
	var responseExecute *hiveserver.TExecuteStatementResp
	go func() {
		responseExecute, err = c.conn.client.ExecuteStatement(ctx, executeReq)
		done <- nil
	}()

	select {
	case <-done:
	case <-ctx.Done():
		go c.Cancel()
		return fmt.Errorf("Context was done before the query was executed")
	}

	if err != nil {
		return
	}
	if !success(responseExecute.GetStatus()) {
		return fmt.Errorf("Error while executing query: %s", responseExecute.Status.String())
	}

	c.operationHandle = responseExecute.OperationHandle

	return nil
}

// Execute is like ExecuteC setting the backgroun
func (c *Cursor) Execute(query string) error {
	return c.ExecuteWithContext(context.Background(), query)
}

func success(status *hiveserver.TStatus) bool {
	statusCode := status.GetStatusCode()
	return statusCode == hiveserver.TStatusCode_SUCCESS_STATUS || statusCode == hiveserver.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

// FetchOneWithContext returns one row
// TODO, check if this context is honored, which probably is not, and do something similar to Exec
func (c *Cursor) FetchOneWithContext(ctx context.Context, dests ...interface{}) (isRow bool, err error) {
	if c.totalRows == c.columnIndex {
		c.queue = nil
		if !c.HasMore() {
			return false, nil
		}
		err = c.pollUntilData(ctx, 1)
		if err != nil {
			return
		}
		// No rows where found when fetching
		if !c.HasMore() {
			return false, nil
		}
	}

	if len(c.queue) != len(dests) {
		return false, fmt.Errorf("%d arguments where passed for filling but the number of columns is %d", len(dests), len(c.queue))
	}
	for i := 0; i < len(c.queue); i++ {
		if c.queue[i].IsSetBinaryVal() {
			// TODO revisit this
			d, ok := dests[i].(*[]byte)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].BinaryVal.Values[c.columnIndex], c.queue[i].BinaryVal.Values[c.columnIndex])
			}
			*d = c.queue[i].BinaryVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetByteVal() {
			d, ok := dests[i].(*int8)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].ByteVal.Values[c.columnIndex], c.queue[i].ByteVal.Values[c.columnIndex])
			}
			*d = c.queue[i].ByteVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetI16Val() {
			d, ok := dests[i].(*int16)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].I16Val.Values[c.columnIndex], c.queue[i].I16Val.Values[c.columnIndex])
			}
			*d = c.queue[i].I16Val.Values[c.columnIndex]
		} else if c.queue[i].IsSetI32Val() {
			d, ok := dests[i].(*int32)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].I32Val.Values[c.columnIndex], c.queue[i].I32Val.Values[c.columnIndex])
			}
			*d = c.queue[i].I32Val.Values[c.columnIndex]
		} else if c.queue[i].IsSetI64Val() {
			d, ok := dests[i].(*int64)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].I64Val.Values[c.columnIndex], c.queue[i].I64Val.Values[c.columnIndex])
			}
			*d = c.queue[i].I64Val.Values[c.columnIndex]
		} else if c.queue[i].IsSetStringVal() {
			d, ok := dests[i].(*string)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].StringVal.Values[c.columnIndex], c.queue[i].StringVal.Values[c.columnIndex])
			}
			*d = c.queue[i].StringVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetDoubleVal() {
			d, ok := dests[i].(*float64)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].DoubleVal.Values[c.columnIndex], c.queue[i].DoubleVal.Values[c.columnIndex])
			}
			*d = c.queue[i].DoubleVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetStringVal() {
			d, ok := dests[i].(*string)
			if !ok {
				return false, fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].StringVal.Values[c.columnIndex], c.queue[i].StringVal.Values[c.columnIndex])
			}
			*d = c.queue[i].StringVal.Values[c.columnIndex]
		} else {
			return true, fmt.Errorf("Empty column %v", c.queue[i])
		}
	}
	c.columnIndex++

	return c.HasMore(), nil
}

// FetchOne is FetchOneWithContext with the background context
func (c *Cursor) FetchOne(dests ...interface{}) (isRow bool, err error) {
	return c.FetchOneWithContext(context.Background(), dests...)
}

// HasMore returns weather more rows can be fetched from the server
func (c *Cursor) HasMore() bool {
	if c.response == nil {
		return true
	}
	return *c.response.HasMoreRows || c.totalRows != c.columnIndex
}

func (c *Cursor) pollUntilData(ctx context.Context, n int) (err error) {
	rowsAvailable := make(chan error)
	defer close(rowsAvailable)

	go func() {
		for true {
			fetchRequest := hiveserver.NewTFetchResultsReq()
			fetchRequest.OperationHandle = c.operationHandle
			fetchRequest.Orientation = hiveserver.TFetchOrientation_FETCH_NEXT
			fetchRequest.MaxRows = c.conn.configuration.FetchSize

			responseFetch, err := c.conn.client.FetchResults(ctx, fetchRequest)
			if err != nil {
				rowsAvailable <- err
				return
			}
			c.response = responseFetch

			if responseFetch.Status.StatusCode != hiveserver.TStatusCode_SUCCESS_STATUS {
				rowsAvailable <- fmt.Errorf(responseFetch.Status.String())
				return
			}

			err = c.parseResults(responseFetch)
			if err != nil {
				rowsAvailable <- err
				return
			}

			if len(c.queue) > 0 {
				rowsAvailable <- nil
				return
			}
			time.Sleep(time.Duration(c.conn.configuration.PollIntervalInMillis) * time.Millisecond)
		}
	}()

	select {
	case err = <-rowsAvailable:
	case <-ctx.Done():
	}

	if err != nil {
		return err
	}

	if len(c.queue) < n {
		return fmt.Errorf("Only %d rows where received", len(c.queue))
	}
	return nil
}

// CancelWithContext tries to cancel the current operation
func (c *Cursor) CancelWithContext(ctx context.Context) error {
	cancelRequest := hiveserver.NewTCancelOperationReq()
	cancelRequest.OperationHandle = c.operationHandle
	responseCancel, err := c.conn.client.CancelOperation(ctx, cancelRequest)
	if err != nil {
		return err
	}
	if !success(responseCancel.GetStatus()) {
		return fmt.Errorf("Error closing the operation: %s", responseCancel.Status.String())
	}
	return nil
}

// Cancel is CancelWithContext with the context set to the background context
func (c *Cursor) Cancel() error {
	return c.CancelWithContext(context.Background())
}

// CloseWithContext close the cursor
func (c *Cursor) CloseWithContext(ctx context.Context) error {
	return c.resetState(ctx)
}

// Close is CloseWithContext accepting a background context
func (c *Cursor) Close() error {
	return c.CloseWithContext(context.Background())
}

func (c *Cursor) resetState(ctx context.Context) error {
	c.response = nil
	c.queue = nil
	c.columnIndex = 0
	c.totalRows = 0
	if c.operationHandle != nil {
		closeRequest := hiveserver.NewTCloseOperationReq()
		closeRequest.OperationHandle = c.operationHandle
		responseClose, err := c.conn.client.CloseOperation(ctx, closeRequest)
		c.operationHandle = nil
		if err != nil {
			return err
		}
		if !success(responseClose.GetStatus()) {
			return fmt.Errorf("Error closing the operation: %s", responseClose.Status.String())
		}
		return nil
	}
	return nil
}

func (c *Cursor) parseResults(response *hiveserver.TFetchResultsResp) (err error) {
	c.queue = response.Results.GetColumns()
	c.columnIndex = 0
	c.totalRows, err = getTotalRows(c.queue)
	return
}

func getTotalRows(columns []*hiveserver.TColumn) (int, error) {
	for _, el := range columns {
		if el.IsSetBinaryVal() {
			return len(el.BinaryVal.Values), nil
		} else if el.IsSetByteVal() {
			return len(el.ByteVal.Values), nil
		} else if el.IsSetI16Val() {
			return len(el.I16Val.Values), nil
		} else if el.IsSetI32Val() {
			return len(el.I32Val.Values), nil
		} else if el.IsSetI64Val() {
			return len(el.I64Val.Values), nil
		} else if el.IsSetBoolVal() {
			return len(el.BoolVal.Values), nil
		} else if el.IsSetDoubleVal() {
			return len(el.DoubleVal.Values), nil
		} else if el.IsSetStringVal() {
			return len(el.StringVal.Values), nil
		} else {
			return -1, fmt.Errorf("Unrecognized column type %T", el)
		}
	}
	return 0, fmt.Errorf("All columns seem empty")
}
