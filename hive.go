package gohive

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/beltran/gosasl"
	"hiveserver"
	"net/http"
	"net/url"
	"os/user"
	"strings"
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
	TransportMode        string
	HttpPath             string
	TlsConfig            *tls.Config
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
		TransportMode:        "binary",
		HttpPath:             "cliservice",
		TlsConfig:            nil,
	}
}

// Connect to hive server
func Connect(ctx context.Context, host string, port int, auth string,
	configuration *ConnectConfiguration) (conn *Connection, err error) {

	var socket thrift.TTransport
	if configuration.TlsConfig != nil {
		socket, err = thrift.NewTSSLSocket(fmt.Sprintf("%s:%d", host, port), configuration.TlsConfig)
	} else {
		socket, err = thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))
	}

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
		configuration.Username = strings.Replace(_user.Name, " ", "", -1)
	}
	// password may not matter but can't be empty
	if configuration.Password == "" {
		configuration.Password = "x"
	}

	if configuration.TransportMode == "http" {
		if auth == "NONE" {
			httpClient, protocol, err := getHttpClient(configuration)
			if err != nil {
				return nil, err
			}
			httpOptions := thrift.THttpClientOptions{Client: httpClient}
			transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(fmt.Sprintf(protocol+"://%s:%s@%s:%d/"+configuration.HttpPath, configuration.Username, configuration.Password, host, port), httpOptions).GetTransport(socket)
			if err != nil {
				return nil, err
			}
		} else if auth == "KERBEROS" {
			mechanism, err := gosasl.NewGSSAPIMechanism(configuration.Service)
			if err != nil {
				return nil, err
			}
			saslClient := gosasl.NewSaslClient(host, mechanism)
			token, err := saslClient.Start()
			if err != nil {
				return nil, err
			}
			if len(token) == 0 {
				return nil, fmt.Errorf("Gssapi init context returned an empty token. Probably the service is empty in the configuration")
			}

			httpClient, protocol, err := getHttpClient(configuration)
			if err != nil {
				return nil, err
			}
			httpClient.Jar = newCookieJar()

			httpOptions := thrift.THttpClientOptions{
				Client: httpClient,
			}
			transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(fmt.Sprintf(protocol+"://%s:%d/"+configuration.HttpPath, host, port), httpOptions).GetTransport(socket)
			httpTransport, ok := transport.(*thrift.THttpClient)
			if ok {
				httpTransport.SetHeader("Authorization", "Negotiate "+base64.StdEncoding.EncodeToString(token))
				httpTransport.SetHeader("X-XSRF-HEADER", "true ")
			}
			if err != nil {
				return nil, err
			}
		} else {
			panic("Unrecognized auth")
		}
	} else if configuration.TransportMode == "binary" {
		if auth == "NOSASL" {
			transport = thrift.NewTBufferedTransport(socket, 4096)
			if transport == nil {
				return nil, fmt.Errorf("BufferedTransport was nil")
			}
		} else if auth == "NONE" || auth == "LDAP" || auth == "CUSTOM" {
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
	} else {
		panic(fmt.Sprintf("Unrecognized transport mode %s", configuration.TransportMode))
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

func getHttpClient(configuration *ConnectConfiguration) (httpClient *http.Client, protocol string, err error) {
	if configuration.TlsConfig != nil {
		transport := &http.Transport{TLSClientConfig: configuration.TlsConfig}
		httpClient = &http.Client{Transport: transport}
		protocol = "https"
	} else {
		httpClient = http.DefaultClient
		protocol = "http"
	}
	return
}

// Cursor creates a cursor from a connection
func (c *Connection) Cursor() *Cursor {
	return &Cursor{
		conn:  c,
		queue: make([]*hiveserver.TColumn, 0),
	}
}

// Close closes a session
func (c *Connection) Close(ctx context.Context) error {
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

const _RUNNING = 0
const _FINISHED = 1
const _NONE = 2

// Cursor is used for fetching the rows after a query
type Cursor struct {
	conn            *Connection
	operationHandle *hiveserver.TOperationHandle
	queue           []*hiveserver.TColumn
	response        *hiveserver.TFetchResultsResp
	columnIndex     int
	totalRows       int
	state           int
	newData         bool
	Err             error
}

// Execute sends a query to hive for execution with a context
// If the context is Done it may not be possible to cancel the opeartion
// Use async = true
func (c *Cursor) Execute(ctx context.Context, query string, async bool) {
	c.resetState(ctx)

	c.state = _RUNNING
	executeReq := hiveserver.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.conn.sessionHandle
	executeReq.Statement = query
	executeReq.RunAsync = async

	// The context from thrift doesn't seem to work
	done := make(chan interface{})
	var responseExecute *hiveserver.TExecuteStatementResp
	go func() {
		defer close(done)
		responseExecute, c.Err = c.conn.client.ExecuteStatement(ctx, executeReq)
		done <- nil
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// TODO revisit this context
		go func() {
			// This can only be cancelled if it was async?
			/*
				err := c.Cancel(context.Background())
				if err != nil {
						panic (err)
				}
			*/
		}()
		// Mark the operation as finished
		c.state = _FINISHED
		c.Err = fmt.Errorf("Context was done before the query was executed")
		return
	}

	if c.Err != nil {
		return
	}
	if !success(responseExecute.GetStatus()) {
		c.Err = fmt.Errorf("Error while executing query: %s", responseExecute.Status.String())
		return
	}

	c.operationHandle = responseExecute.OperationHandle
	if !responseExecute.OperationHandle.HasResultSet {
		c.state = _FINISHED
	}
}

// Poll returns the current status of the last operation
func (c *Cursor) Poll(ctx context.Context) (status *hiveserver.TOperationState) {
	c.Err = nil
	progressGet := true
	pollRequest := hiveserver.NewTGetOperationStatusReq()
	pollRequest.OperationHandle = c.operationHandle
	pollRequest.GetProgressUpdate = &progressGet
	var responsePoll *hiveserver.TGetOperationStatusResp
	responsePoll, c.Err = c.conn.client.GetOperationStatus(ctx, pollRequest)
	if c.Err != nil {
		return nil
	}
	if !success(responsePoll.GetStatus()) {
		c.Err = fmt.Errorf("Error closing the operation: %s", responsePoll.Status.String())
		return nil
	}
	return responsePoll.OperationState
}

func (c *Cursor) Finished() bool {
	status := c.Poll(context.Background())
	if c.Err != nil {
		return false
	}
	return !(*status == hiveserver.TOperationState_INITIALIZED_STATE || *status == hiveserver.TOperationState_RUNNING_STATE)
}

func success(status *hiveserver.TStatus) bool {
	statusCode := status.GetStatusCode()
	return statusCode == hiveserver.TStatusCode_SUCCESS_STATUS || statusCode == hiveserver.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

// FetchOne returns one row
// TODO, check if this context is honored, which probably is not, and do something similar to Exec
func (c *Cursor) FetchOne(ctx context.Context, dests ...interface{}) (isRow bool) {
	c.Err = nil
	if c.totalRows == c.columnIndex {
		c.queue = nil
		if !c.HasMore(ctx) {
			return false
		}
	}

	if len(c.queue) != len(dests) {
		c.Err = fmt.Errorf("%d arguments where passed for filling but the number of columns is %d", len(dests), len(c.queue))
		return false
	}
	for i := 0; i < len(c.queue); i++ {
		if c.queue[i].IsSetBinaryVal() {
			// TODO revisit this
			d, ok := dests[i].(*[]byte)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].BinaryVal.Values[c.columnIndex], c.queue[i].BinaryVal.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].BinaryVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetByteVal() {
			d, ok := dests[i].(*int8)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].ByteVal.Values[c.columnIndex], c.queue[i].ByteVal.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].ByteVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetI16Val() {
			d, ok := dests[i].(*int16)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].I16Val.Values[c.columnIndex], c.queue[i].I16Val.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].I16Val.Values[c.columnIndex]
		} else if c.queue[i].IsSetI32Val() {
			d, ok := dests[i].(*int32)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].I32Val.Values[c.columnIndex], c.queue[i].I32Val.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].I32Val.Values[c.columnIndex]
		} else if c.queue[i].IsSetI64Val() {
			d, ok := dests[i].(*int64)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].I64Val.Values[c.columnIndex], c.queue[i].I64Val.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].I64Val.Values[c.columnIndex]
		} else if c.queue[i].IsSetStringVal() {
			d, ok := dests[i].(*string)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].StringVal.Values[c.columnIndex], c.queue[i].StringVal.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].StringVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetDoubleVal() {
			d, ok := dests[i].(*float64)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].DoubleVal.Values[c.columnIndex], c.queue[i].DoubleVal.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].DoubleVal.Values[c.columnIndex]
		} else if c.queue[i].IsSetStringVal() {
			d, ok := dests[i].(*string)
			if !ok {
				c.Err = fmt.Errorf("Unexpected data type %T for value %v (should be %T)", dests[i], c.queue[i].StringVal.Values[c.columnIndex], c.queue[i].StringVal.Values[c.columnIndex])
				return false
			}
			*d = c.queue[i].StringVal.Values[c.columnIndex]
		} else {
			c.Err = fmt.Errorf("Empty column %v", c.queue[i])
			return true
		}
	}
	c.columnIndex++

	return false
}

// HasMore returns weather more rows can be fetched from the server
func (c *Cursor) HasMore(ctx context.Context) bool {
	c.Err = nil
	if c.response == nil && c.state != _FINISHED {
		c.Err = c.pollUntilData(ctx, 1)
		return c.state != _FINISHED || c.totalRows != c.columnIndex
	}
	// *c.response.HasMoreRows is always false
	// so it can be checked and another roundtrip has to be done if etra data has been added
	if c.totalRows == c.columnIndex && c.state != _FINISHED {
		c.Err = c.pollUntilData(ctx, 1)
	}

	return c.state != _FINISHED || c.totalRows != c.columnIndex
}

func (c *Cursor) Error() error {
	return c.Err
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

// Cancel tries to cancel the current operation
func (c *Cursor) Cancel(ctx context.Context) {
	c.Err = nil
	cancelRequest := hiveserver.NewTCancelOperationReq()
	cancelRequest.OperationHandle = c.operationHandle
	var responseCancel *hiveserver.TCancelOperationResp
	responseCancel, c.Err = c.conn.client.CancelOperation(ctx, cancelRequest)
	if c.Err != nil {
		return
	}
	if !success(responseCancel.GetStatus()) {
		c.Err = fmt.Errorf("Error closing the operation: %s", responseCancel.Status.String())
	}
	return
}

// Close close the cursor
func (c *Cursor) Close(ctx context.Context) {
	c.Err = c.resetState(ctx)
}

func (c *Cursor) resetState(ctx context.Context) error {
	c.response = nil
	c.Err = nil
	c.queue = nil
	c.columnIndex = 0
	c.totalRows = 0
	c.state = _NONE
	c.newData = false
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
	c.newData = c.totalRows > 0
	if !c.newData {
		c.state = _FINISHED
	}
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

type InMemoryCookieJar struct {
	storage map[string][]http.Cookie
}

func (jar InMemoryCookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	for _, cookie := range cookies {
		jar.storage["cliservice"] = []http.Cookie{*cookie}
	}
}

func (jar InMemoryCookieJar) Cookies(u *url.URL) []*http.Cookie {
	cookiesArray := []*http.Cookie{}
	for pattern, cookies := range jar.storage {
		if strings.Contains(u.String(), pattern) {
			for i := range cookies {
				cookiesArray = append(cookiesArray, &cookies[i])
			}
		}
	}
	return cookiesArray
}

func newCookieJar() InMemoryCookieJar {
	storage := make(map[string][]http.Cookie)
	return InMemoryCookieJar{storage}
}
