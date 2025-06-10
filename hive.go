package gohive

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/beltran/gohive/hiveserver"
	"github.com/beltran/gosasl"
	"github.com/pkg/errors"
	"golang.org/x/net/publicsuffix"
)

const defaultFetchSize int64 = 1000
const zookeeperDefaultNamespace = "hiveserver2"
const defaultMaxLength = 16384000

// Cursor states
const (
	_NONE = iota
	_RUNNING
	_FINISHED
	_ERROR
	_CONTEXT_DONE
	_ASYNC_ENDED
)

type dialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// connection holds the information for getting a cursor to hive.
type connection struct {
	host                string
	port                int
	username            string
	database            string
	auth                string
	kerberosServiceName string
	password            string
	sessionHandle       *hiveserver.TSessionHandle
	client              *hiveserver.TCLIServiceClient
	configuration       *connectConfiguration
	transport           thrift.TTransport
	mu                  sync.Mutex // Mutex to protect connection operations
	clientMu            sync.Mutex // Mutex to protect client operations
}

// connectConfiguration is the configuration for the connection
// The fields have to be filled manually but not all of them are required
// Depends on the auth and kind of connection.
type connectConfiguration struct {
	Username             string
	Principal            string
	Password             string
	Service              string
	HiveConfiguration    map[string]string
	PollIntervalInMillis int
	FetchSize            int64
	TransportMode        string
	HTTPPath             string
	TLSConfig            *tls.Config
	ZookeeperNamespace   string
	Database             string
	ConnectTimeout       time.Duration
	SocketTimeout        time.Duration
	HttpTimeout          time.Duration
	DialContext          dialContextFunc
	DisableKeepAlives    bool
	// Maximum length of the data in bytes. Used for SASL.
	MaxSize uint32
}

// newConnectConfiguration returns a connect configuration, all with empty fields
func newConnectConfiguration() *connectConfiguration {
	return &connectConfiguration{
		Username:             "",
		Password:             "",
		Service:              "",
		HiveConfiguration:    nil,
		PollIntervalInMillis: 200,
		FetchSize:            defaultFetchSize,
		TransportMode:        "binary",
		HTTPPath:             "cliservice",
		TLSConfig:            nil,
		ZookeeperNamespace:   zookeeperDefaultNamespace,
		MaxSize:              defaultMaxLength,
	}
}

// hiveError represents an error surfaced from Hive. We attach the specific Error code along with the usual message.
type hiveError struct {
	error

	// Simple error message, without the full stack trace. Surfaced from Thrift.
	Message string
	// See https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/ql/ErrorMsg.java for info about error codes
	ErrorCode int
}

// connect to hive server
func connect(ctx context.Context, host string, port int, auth string,
	configuration *connectConfiguration) (conn *connection, err error) {
	return innerConnect(ctx, host, port, auth, configuration)
}

func parseHiveServer2Info(hsInfos []string) []map[string]string {
	results := make([]map[string]string, len(hsInfos))
	actualCount := 0

	for _, hsInfo := range hsInfos {
		validFormat := false
		node := make(map[string]string)

		for _, param := range strings.Split(hsInfo, ";") {
			kvPair := strings.Split(param, "=")
			if len(kvPair) < 2 {
				break
			}
			if kvPair[0] == "serverUri" {
				hostAndPort := strings.Split(kvPair[1], ":")
				if len(hostAndPort) == 2 {
					node["host"] = hostAndPort[0]
					node["port"] = hostAndPort[1]
					validFormat = len(node["host"]) != 0 && len(node["port"]) != 0
				} else {
					break
				}
			} else {
				node[kvPair[0]] = kvPair[1]
			}
		}
		if validFormat {
			results[actualCount] = node
			actualCount++
		}
	}
	return results[0:actualCount]
}

func dial(ctx context.Context, addr string, dialFn dialContextFunc, timeout time.Duration) (net.Conn, error) {
	dctx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return dialFn(dctx, "tcp", addr)
}

func innerConnect(ctx context.Context, host string, port int, auth string,
	configuration *connectConfiguration) (conn *connection, err error) {

	var socket thrift.TTransport
	addr := fmt.Sprintf("%s:%d", host, port)
	if configuration.DialContext != nil {
		var netConn net.Conn
		netConn, err = dial(ctx, addr, configuration.DialContext, configuration.ConnectTimeout)
		if err != nil {
			return
		}
		if configuration.TLSConfig != nil {
			socket = thrift.NewTSSLSocketFromConnConf(netConn, &thrift.TConfiguration{
				ConnectTimeout: configuration.ConnectTimeout,
				SocketTimeout:  configuration.SocketTimeout,
				TLSConfig:      configuration.TLSConfig,
			})
		} else {
			socket = thrift.NewTSocketFromConnConf(netConn, &thrift.TConfiguration{
				ConnectTimeout: configuration.ConnectTimeout,
				SocketTimeout:  configuration.SocketTimeout,
			})
		}
	} else {
		if configuration.TLSConfig != nil {
			socket = thrift.NewTSSLSocketConf(addr, &thrift.TConfiguration{
				ConnectTimeout: configuration.ConnectTimeout,
				SocketTimeout:  configuration.SocketTimeout,
				TLSConfig:      configuration.TLSConfig,
			})
		} else {
			socket = thrift.NewTSocketConf(addr, &thrift.TConfiguration{
				ConnectTimeout: configuration.ConnectTimeout,
				SocketTimeout:  configuration.SocketTimeout,
			})
		}
		if err = socket.Open(); err != nil {
			return
		}
	}

	var transport thrift.TTransport

	if configuration == nil {
		configuration = newConnectConfiguration()
	}
	if configuration.Username == "" {
		_user, err := user.Current()
		if err != nil {
			return nil, errors.New("Can't determine the username")
		}
		configuration.Username = strings.Replace(_user.Name, " ", "", -1)
	}
	// password may not matter but can't be empty
	if configuration.Password == "" {
		configuration.Password = "x"
	}

	if configuration.TransportMode == "http" {
		if auth == "NONE" {
			httpClient, protocol, err := getHTTPClient(configuration)
			if err != nil {
				return nil, err
			}

			httpClient.Jar, err = cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
			if err != nil {
				return nil, err
			}

			httpOptions := thrift.THttpClientOptions{Client: httpClient}
			transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(fmt.Sprintf(protocol+"://%s:%s@%s:%d/"+configuration.HTTPPath, url.QueryEscape(configuration.Username), url.QueryEscape(configuration.Password), host, port), httpOptions).GetTransport(socket)
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
				return nil, errors.New("Gssapi init context returned an empty token. Probably the service is empty in the configuration")
			}

			httpClient, protocol, err := getHTTPClient(configuration)
			if err != nil {
				return nil, err
			}
			httpClient.Jar, err = cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
			if err != nil {
				return nil, err
			}

			httpOptions := thrift.THttpClientOptions{
				Client: httpClient,
			}
			transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(fmt.Sprintf(protocol+"://%s:%d/"+configuration.HTTPPath, host, port), httpOptions).GetTransport(socket)
			httpTransport, ok := transport.(*thrift.THttpClient)
			if ok {
				httpTransport.SetHeader("Authorization", "Negotiate "+base64.StdEncoding.EncodeToString(token))
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
				return nil, errors.New("BufferedTransport was nil")
			}
		} else if auth == "NONE" || auth == "LDAP" || auth == "CUSTOM" {
			saslConfiguration := map[string]string{"username": configuration.Username, "password": configuration.Password}
			transport, err = NewTSaslTransport(socket, host, "PLAIN", saslConfiguration, configuration.MaxSize)
			if err != nil {
				return
			}
		} else if auth == "KERBEROS" {
			saslConfiguration := map[string]string{"service": configuration.Service}
			transport, err = NewTSaslTransport(socket, host, "GSSAPI", saslConfiguration, configuration.MaxSize)
			if err != nil {
				return
			}
		} else if auth == "DIGEST-MD5" {
			saslConfiguration := map[string]string{"username": configuration.Username, "password": configuration.Password, "service": configuration.Service}
			transport, err = NewTSaslTransport(socket, host, "DIGEST-MD5", saslConfiguration, configuration.MaxSize)
			if err != nil {
				return
			}
		} else {
			panic("Unrecognized auth")
		}
		if !transport.IsOpen() {
			if err = transport.Open(); err != nil {
				return
			}
		}
	} else {
		panic("Unrecognized transport mode " + configuration.TransportMode)
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := hiveserver.NewTCLIServiceClientFactory(transport, protocolFactory)

	openSession := hiveserver.NewTOpenSessionReq()
	openSession.ClientProtocol = hiveserver.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V6
	openSession.Configuration = configuration.HiveConfiguration
	openSession.Username = &configuration.Username
	openSession.Password = &configuration.Password
	// Context is ignored
	response, err := client.OpenSession(context.Background(), openSession)
	if err != nil {
		return
	}

	database := configuration.Database
	if database == "" {
		database = "default"
	}
	conn = &connection{
		host:                host,
		port:                port,
		database:            database,
		auth:                auth,
		kerberosServiceName: "",
		sessionHandle:       response.SessionHandle,
		client:              client,
		configuration:       configuration,
		transport:           transport,
	}

	if configuration.Database != "" {
		cursor := conn.cursor()
		defer cursor.close()
		cursor.exec(context.Background(), "USE "+configuration.Database)
		if cursor.Err != nil {
			return nil, cursor.Err
		}
	}

	return conn, nil
}

type cookieDedupTransport struct {
	http.RoundTripper
}

// RoundTrip removes duplicate cookies (cookies with the same name) from the request
// This is a mitigation for the issue where Hive/Impala cookies get duplicated in the response
func (d *cookieDedupTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cookieMap := map[string]string{}
	for _, cookie := range req.Cookies() {
		cookieMap[cookie.Name] = cookie.Value
	}

	req.Header.Set("Cookie", "")

	for key, value := range cookieMap {
		req.AddCookie(&http.Cookie{Name: key, Value: value})
	}

	resp, err := d.RoundTripper.RoundTrip(req)

	return resp, err
}

func getHTTPClient(configuration *connectConfiguration) (httpClient *http.Client, protocol string, err error) {
	if configuration.TLSConfig != nil {
		httpClient = &http.Client{
			Timeout: configuration.HttpTimeout,
			Transport: &http.Transport{
				TLSClientConfig:   configuration.TLSConfig,
				DialContext:       configuration.DialContext,
				DisableKeepAlives: configuration.DisableKeepAlives,
			},
		}
		protocol = "https"
	} else {
		httpClient = &http.Client{
			Timeout: configuration.HttpTimeout,
			Transport: &http.Transport{
				DialContext:       configuration.DialContext,
				DisableKeepAlives: configuration.DisableKeepAlives,
			},
		}
		protocol = "http"
	}

	httpClient.Transport = &cookieDedupTransport{httpClient.Transport}

	return
}

// cursor is used for fetching the rows after a query
type cursor struct {
	conn            *connection
	operationHandle *hiveserver.TOperationHandle
	queue           []*hiveserver.TColumn
	response        *hiveserver.TFetchResultsResp
	columnIndex     int
	totalRows       int
	state           int
	newData         bool
	Err             error
	descriptionData [][]string
	mu              sync.Mutex // Mutex to protect cursor operations
	id              string     // Unique identifier for logging

	// Caller is responsible for managing this channel
	Logs chan<- []string
}

// exec issues a synchronous query.
func (c *cursor) exec(ctx context.Context, query string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.execute(ctx, query)
}

// execute sends a query to hive for execution with a context
func (c *cursor) execute(ctx context.Context, query string) {
	c.executeSync(ctx, query)
	// We cannot trust in setting executeReq.RunAsync = true
	// because if the context ends the operation can't be cancelled cleanly
	if c.Err != nil {
		if c.state == _CONTEXT_DONE {
			c.handleDoneContext()
		}
		return
	}

	if c.Err != nil {
		if c.state == _CONTEXT_DONE {
			c.handleDoneContext()
		} else if c.state == _ERROR {
			c.Err = errors.New("Probably the context was over when passed to execute. This probably resulted in the message being sent but we didn't get an operation handle so it's most likely a bug in thrift")
		}
		return
	}

	// Flush logs after execution is finished
	if c.Logs != nil {
		logs := c.fetchLogs()
		if c.error() != nil {
			c.state = _ASYNC_ENDED
			return
		}
		c.Logs <- logs
	}

	c.state = _ASYNC_ENDED
}

func (c *cursor) handleDoneContext() {
	originalError := c.Err
	if c.operationHandle != nil {
		c.cancel()
		if c.Err != nil {
			return
		}
	}
	c.resetState()
	c.Err = originalError
	c.state = _FINISHED
}

// executeSync sends a query to hive for execution with a context
func (c *cursor) executeSync(ctx context.Context, query string) {
	c.resetState()

	c.state = _RUNNING
	executeReq := hiveserver.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.conn.sessionHandle
	executeReq.Statement = query
	executeReq.RunAsync = false
	var responseExecute *hiveserver.TExecuteStatementResp = nil

	c.conn.clientMu.Lock()
	responseExecute, c.Err = c.conn.client.ExecuteStatement(ctx, executeReq)
	c.conn.clientMu.Unlock()

	if c.Err != nil {
		if strings.Contains(c.Err.Error(), "context deadline exceeded") {
			c.state = _CONTEXT_DONE
			if responseExecute == nil {
				c.state = _ERROR
			} else if responseExecute != nil {
				// We may need this to cancel the operation
				c.operationHandle = responseExecute.OperationHandle
			}
		}
		return
	}
	if !success(safeStatus(responseExecute.GetStatus())) {
		status := safeStatus(responseExecute.GetStatus())
		c.Err = hiveError{
			error:     errors.New("Error while executing query: " + status.String()),
			Message:   status.GetErrorMessage(),
			ErrorCode: int(status.GetErrorCode()),
		}
		return
	}

	c.operationHandle = responseExecute.OperationHandle
	if !responseExecute.OperationHandle.HasResultSet {
		c.state = _FINISHED
	}
}

// poll returns the current status of the last operation
func (c *cursor) poll(getProgress bool) (status *hiveserver.TGetOperationStatusResp) {
	c.Err = nil
	progressGet := getProgress
	pollRequest := hiveserver.NewTGetOperationStatusReq()
	pollRequest.OperationHandle = c.operationHandle
	pollRequest.GetProgressUpdate = &progressGet
	var responsePoll *hiveserver.TGetOperationStatusResp
	// Context ignored
	c.conn.clientMu.Lock()
	responsePoll, c.Err = c.conn.client.GetOperationStatus(context.Background(), pollRequest)
	c.conn.clientMu.Unlock()
	if c.Err != nil {
		return nil
	}
	if !success(safeStatus(responsePoll.GetStatus())) {
		c.Err = errors.New("Error closing the operation: " + safeStatus(responsePoll.GetStatus()).String())
		return nil
	}
	return responsePoll
}

// fetchLogs returns all the Hive execution logs for the latest query up to the current point
func (c *cursor) fetchLogs() []string {
	logRequest := hiveserver.NewTFetchResultsReq()
	logRequest.OperationHandle = c.operationHandle
	logRequest.Orientation = hiveserver.TFetchOrientation_FETCH_NEXT
	logRequest.MaxRows = c.conn.configuration.FetchSize
	// FetchType 1 is "logs"
	logRequest.FetchType = 1

	c.conn.clientMu.Lock()
	resp, err := c.conn.client.FetchResults(context.Background(), logRequest)
	c.conn.clientMu.Unlock()
	if err != nil || resp == nil || resp.Results == nil {
		c.Err = err
		return nil
	}

	// resp contains 1 row, with a column for each line in the log
	cols := resp.Results.GetColumns()
	var logs []string

	for _, col := range cols {
		logs = append(logs, col.StringVal.Values...)
	}

	return logs
}

// finished returns true if the last async operation has finished
func (c *cursor) finished() bool {
	operationStatus := c.poll(true)

	if c.Err != nil {
		return true
	}
	status := operationStatus.OperationState
	return !(*status == hiveserver.TOperationState_INITIALIZED_STATE || *status == hiveserver.TOperationState_RUNNING_STATE)
}

func success(status *hiveserver.TStatus) bool {
	statusCode := status.GetStatusCode()
	return statusCode == hiveserver.TStatusCode_SUCCESS_STATUS || statusCode == hiveserver.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

func (c *cursor) fetchIfEmpty(ctx context.Context) {
	c.Err = nil
	if c.totalRows == c.columnIndex {
		c.queue = nil
		if !c.hasMore(ctx) {
			c.Err = errors.New("No more rows are left")
			return
		}
		if c.Err != nil {
			return
		}
	}
}

// rowMap returns one row as a map. Advances the cursor one
func (c *cursor) rowMap(ctx context.Context) map[string]interface{} {
	c.Err = nil
	c.fetchIfEmpty(ctx)
	if c.Err != nil {
		return nil
	}

	d := c.description()
	if c.Err != nil || len(d) != len(c.queue) {
		return nil
	}
	m := make(map[string]interface{}, len(c.queue))
	for i := 0; i < len(c.queue); i++ {
		columnName := d[i][0]
		columnType := d[i][1]
		if columnType == "BOOLEAN_TYPE" {
			if isNull(c.queue[i].BoolVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].BoolVal.Values[c.columnIndex]
			}
		} else if columnType == "TINYINT_TYPE" {
			if isNull(c.queue[i].ByteVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].ByteVal.Values[c.columnIndex]
			}
		} else if columnType == "SMALLINT_TYPE" {
			if isNull(c.queue[i].I16Val.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].I16Val.Values[c.columnIndex]
			}
		} else if columnType == "INT_TYPE" {
			if isNull(c.queue[i].I32Val.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].I32Val.Values[c.columnIndex]
			}
		} else if columnType == "BIGINT_TYPE" {
			if isNull(c.queue[i].I64Val.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].I64Val.Values[c.columnIndex]
			}
		} else if columnType == "FLOAT_TYPE" {
			if isNull(c.queue[i].DoubleVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].DoubleVal.Values[c.columnIndex]
			}
		} else if columnType == "DOUBLE_TYPE" {
			if isNull(c.queue[i].DoubleVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].DoubleVal.Values[c.columnIndex]
			}
		} else if columnType == "STRING_TYPE" || columnType == "VARCHAR_TYPE" || columnType == "CHAR_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "TIMESTAMP_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "DATE_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "BINARY_TYPE" {
			if isNull(c.queue[i].BinaryVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].BinaryVal.Values[c.columnIndex]
			}
		} else if columnType == "ARRAY_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "MAP_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "STRUCT_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "UNION_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if columnType == "DECIMAL_TYPE" {
			if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
				m[columnName] = nil
			} else {
				m[columnName] = c.queue[i].StringVal.Values[c.columnIndex]
			}
		}
	}
	if len(m) != len(d) {
		log.Printf("Some columns have the same name as per the description: %v, this makes it impossible to get the values using the RowMap API, please use the FetchOne API", d)
	}
	c.columnIndex++
	return m
}

// fetchOne returns one row and advances the cursor one
func (c *cursor) fetchOne(ctx context.Context, dests ...interface{}) {
	c.Err = nil
	c.fetchIfEmpty(ctx)
	if c.Err != nil {
		return
	}

	if len(c.queue) != len(dests) {
		c.Err = errors.Errorf("%d arguments where passed for filling but the number of columns is %d", len(dests), len(c.queue))
		return
	}
	for i := 0; i < len(c.queue); i++ {
		if c.queue[i].IsSetBinaryVal() {
			if dests[i] == nil {
				dests[i] = c.queue[i].BinaryVal.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*[]byte)
			if !ok {
				c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].BinaryVal.Values[c.columnIndex], c.queue[i].BinaryVal.Values[c.columnIndex], i)
				return
			}
			if isNull(c.queue[i].BinaryVal.Nulls, c.columnIndex) {
				*d = nil
			} else {
				*d = c.queue[i].BinaryVal.Values[c.columnIndex]
			}
		} else if c.queue[i].IsSetByteVal() {
			if dests[i] == nil {
				dests[i] = c.queue[i].ByteVal.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*int8)
			if !ok {
				d, ok := dests[i].(**int8)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].ByteVal.Values[c.columnIndex], c.queue[i].ByteVal.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].ByteVal.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(int8)
					}
					**d = c.queue[i].ByteVal.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].ByteVal.Values[c.columnIndex]
			}

		} else if c.queue[i].IsSetI16Val() {
			if dests[i] == nil {
				dests[i] = c.queue[i].I16Val.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*int16)
			if !ok {
				d, ok := dests[i].(**int16)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].I16Val.Values[c.columnIndex], c.queue[i].I16Val.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].I16Val.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(int16)
					}
					**d = c.queue[i].I16Val.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].I16Val.Values[c.columnIndex]
			}
		} else if c.queue[i].IsSetI32Val() {
			if dests[i] == nil {
				dests[i] = c.queue[i].I32Val.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*int32)
			if !ok {
				d, ok := dests[i].(**int32)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].I32Val.Values[c.columnIndex], c.queue[i].I32Val.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].I32Val.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(int32)
					}
					**d = c.queue[i].I32Val.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].I32Val.Values[c.columnIndex]
			}
		} else if c.queue[i].IsSetI64Val() {
			if dests[i] == nil {
				dests[i] = c.queue[i].I64Val.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*int64)
			if !ok {
				d, ok := dests[i].(**int64)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].I64Val.Values[c.columnIndex], c.queue[i].I64Val.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].I64Val.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(int64)
					}
					**d = c.queue[i].I64Val.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].I64Val.Values[c.columnIndex]
			}
		} else if c.queue[i].IsSetStringVal() {
			if dests[i] == nil {
				dests[i] = c.queue[i].StringVal.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*string)
			if !ok {
				d, ok := dests[i].(**string)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].StringVal.Values[c.columnIndex], c.queue[i].StringVal.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].StringVal.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(string)
					}
					**d = c.queue[i].StringVal.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].StringVal.Values[c.columnIndex]
			}
		} else if c.queue[i].IsSetDoubleVal() {
			if dests[i] == nil {
				dests[i] = c.queue[i].DoubleVal.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*float64)
			if !ok {
				d, ok := dests[i].(**float64)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].DoubleVal.Values[c.columnIndex], c.queue[i].DoubleVal.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].DoubleVal.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(float64)
					}
					**d = c.queue[i].DoubleVal.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].DoubleVal.Values[c.columnIndex]
			}
		} else if c.queue[i].IsSetBoolVal() {
			if dests[i] == nil {
				dests[i] = c.queue[i].BoolVal.Values[c.columnIndex]
				continue
			}
			d, ok := dests[i].(*bool)
			if !ok {
				d, ok := dests[i].(**bool)
				if !ok {
					c.Err = errors.Errorf("Unexpected data type %T for value %v (should be %T) index is %v", dests[i], c.queue[i].BoolVal.Values[c.columnIndex], c.queue[i].BoolVal.Values[c.columnIndex], i)
					return
				}

				if isNull(c.queue[i].BoolVal.Nulls, c.columnIndex) {
					*d = nil
				} else {
					if *d == nil {
						*d = new(bool)
					}
					**d = c.queue[i].BoolVal.Values[c.columnIndex]
				}
			} else {
				*d = c.queue[i].BoolVal.Values[c.columnIndex]
			}
		} else {
			c.Err = errors.Errorf("Empty column %v", c.queue[i])
			return
		}
	}
	c.columnIndex++

	return
}

func isNull(nulls []byte, position int) bool {
	index := position / 8
	if len(nulls) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

// description return a map with the names of the columns and their types
// must be called after a FetchResult request
// a context should be added here but seems to be ignored by thrift
func (c *cursor) description() [][]string {
	if c.descriptionData != nil {
		return c.descriptionData
	}
	if c.operationHandle == nil {
		c.Err = errors.Errorf("Description can only be called after after a Poll or after an async request")
	}

	metaRequest := hiveserver.NewTGetResultSetMetadataReq()
	metaRequest.OperationHandle = c.operationHandle
	c.conn.clientMu.Lock()
	metaResponse, err := c.conn.client.GetResultSetMetadata(context.Background(), metaRequest)
	c.conn.clientMu.Unlock()
	if err != nil {
		c.Err = err
		return nil
	}
	if metaResponse.Status.StatusCode != hiveserver.TStatusCode_SUCCESS_STATUS {
		c.Err = errors.New(safeStatus(metaResponse.GetStatus()).String())
		return nil
	}
	m := make([][]string, len(metaResponse.Schema.Columns))
	for i, column := range metaResponse.Schema.Columns {
		for _, typeDesc := range column.TypeDesc.Types {
			m[i] = []string{column.ColumnName, typeDesc.PrimitiveEntry.Type.String()}
		}
	}
	c.descriptionData = m
	return m
}

// hasMore returns whether more rows can be fetched from the server
func (c *cursor) hasMore(ctx context.Context) bool {
	c.Err = nil
	if c.response == nil && c.state != _FINISHED {
		c.Err = c.pollUntilData(ctx, 1)
		return c.state != _FINISHED || c.totalRows != c.columnIndex
	}
	// *c.response.HasMoreRows is always false
	// so it can be checked and another roundtrip has to be done if extra data has been added
	if c.totalRows == c.columnIndex && c.state != _FINISHED {
		c.Err = c.pollUntilData(ctx, 1)
	}

	return c.state != _FINISHED || c.totalRows != c.columnIndex
}

func (c *cursor) error() error {
	return c.Err
}

func (c *cursor) pollUntilData(ctx context.Context, n int) (err error) {
	rowsAvailable := make(chan error)
	var stopLock sync.Mutex
	var done = false
	go func() {
		defer close(rowsAvailable)
		for true {
			stopLock.Lock()
			if done {
				stopLock.Unlock()
				rowsAvailable <- nil
				return
			}
			stopLock.Unlock()

			fetchRequest := hiveserver.NewTFetchResultsReq()
			fetchRequest.OperationHandle = c.operationHandle
			fetchRequest.Orientation = hiveserver.TFetchOrientation_FETCH_NEXT
			fetchRequest.MaxRows = c.conn.configuration.FetchSize
			c.conn.clientMu.Lock()
			responseFetch, err := c.conn.client.FetchResults(context.Background(), fetchRequest)
			c.conn.clientMu.Unlock()
			if err != nil {
				rowsAvailable <- err
				return
			}
			c.response = responseFetch

			if safeStatus(responseFetch.GetStatus()).StatusCode != hiveserver.TStatusCode_SUCCESS_STATUS {
				rowsAvailable <- errors.New(safeStatus(responseFetch.GetStatus()).String())
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
		stopLock.Lock()
		done = true
		stopLock.Unlock()
		select {
		// Wait for goroutine to finish
		case <-rowsAvailable:
		}
		err = errors.New("Context is done")
	}

	if err != nil {
		return err
	}

	if len(c.queue) < n {
		return errors.Errorf("Only %d rows where received", len(c.queue))
	}
	return nil
}

// cancel cancels the current operation
func (c *cursor) cancel() {
	c.Err = nil
	cancelRequest := hiveserver.NewTCancelOperationReq()
	cancelRequest.OperationHandle = c.operationHandle
	var responseCancel *hiveserver.TCancelOperationResp
	// This context is simply ignored
	c.conn.clientMu.Lock()
	responseCancel, c.Err = c.conn.client.CancelOperation(context.Background(), cancelRequest)
	c.conn.clientMu.Unlock()
	if c.Err != nil {
		return
	}
	if !success(safeStatus(responseCancel.GetStatus())) {
		c.Err = errors.New("Error closing the operation: " + safeStatus(responseCancel.GetStatus()).String())
	}
}

// close closes the cursor
func (c *cursor) close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Err = c.resetState()
}

func (c *cursor) resetState() error {
	c.response = nil
	c.Err = nil
	c.queue = nil
	c.columnIndex = 0
	c.totalRows = 0
	c.state = _NONE
	c.descriptionData = nil
	c.newData = false
	if c.operationHandle != nil {
		closeRequest := hiveserver.NewTCloseOperationReq()
		closeRequest.OperationHandle = c.operationHandle

		c.conn.clientMu.Lock()
		responseClose, err := c.conn.client.CloseOperation(context.Background(), closeRequest)
		c.conn.clientMu.Unlock()
		c.operationHandle = nil
		if err != nil {
			return err
		}
		if !success(safeStatus(responseClose.GetStatus())) {
			err := errors.New("Error closing the operation: " + safeStatus(responseClose.GetStatus()).String())
			return err
		}
		return nil
	}
	return nil
}

func (c *cursor) parseResults(response *hiveserver.TFetchResultsResp) (err error) {
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
			return -1, errors.Errorf("Unrecognized column type %T", el)
		}
	}
	return 0, errors.New("All columns seem empty")
}

func safeStatus(status *hiveserver.TStatus) *hiveserver.TStatus {
	if status == nil {
		return &DEFAULT_STATUS
	}
	return status
}

var DEFAULT_SQL_STATE = ""
var DEFAULT_ERROR_CODE = int32(-1)
var DEFAULT_ERROR_MESSAGE = "unknown error"
var DEFAULT_STATUS = hiveserver.TStatus{
	StatusCode:   hiveserver.TStatusCode_ERROR_STATUS,
	InfoMessages: nil,
	SqlState:     &DEFAULT_SQL_STATE,
	ErrorCode:    &DEFAULT_ERROR_CODE,
	ErrorMessage: &DEFAULT_ERROR_MESSAGE,
}

// cursor creates a cursor from a connection
func (c *connection) cursor() *cursor {
	return &cursor{
		conn: c,
	}
}

// close closes a session
func (c *connection) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.transport != nil {
		return c.transport.Close()
	}
	return nil
}
