package gohive

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"fmt"
	"hiveserver"
	"context"
	"time"
	"os/user"
)

const pollIntervalInMillis int = 100
const DEFAULT_FETCH_SIZE int64 = 1000

// Connection holds the information for getting a cursor to hive
type Connection struct {
	host string
	port int
	username string
	database string
	auth string
	kerberosServiceName string
	password string
	sessionHandle *hiveserver.TSessionHandle
	client *hiveserver.TCLIServiceClient
	configuration map[string]string
	RequestSize int64
}

// Connect to hive server
func Connect(host string, port int, auth string, 
		configuration map[string]string) (conn *Connection, err error) {
	socket, err := thrift.NewTSocket(fmt.Sprintf("%s:%d", host, port))
	
	if configuration == nil {
		configuration = make(map[string]string)
	}
	
	if err = socket.Open(); err != nil {
		fmt.Println("Error opening socket:", err)
        return
	}
	
	var transport thrift.TTransport

	if _, ok := configuration["username"]; !ok {
		_user, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("Can't determine the username")
		}
		configuration["username"] = _user.Name
	}
	
	// password doesn't matter but can't be empty
	if auth == "NOSASL" {
		transport = thrift.NewTBufferedTransport(socket, 4096)
		if transport == nil {
			fmt.Println("Error opening socket:", transport)
			return
		}
	} else if auth == "NONE" {
		if _, ok := configuration["password"]; !ok {
			configuration["password"] = "x"
		}
		transport, err = NewTSaslTransport(socket, host, "PLAIN", configuration)
		if err != nil {
			fmt.Println("Error opening socket:", transport)
			return
		}
	} else if auth == "KERBEROS" {
		transport, err = NewTSaslTransport(socket, host, "GSSAPI", configuration)
		if err != nil {
			fmt.Println("Error opening socket:", transport)
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
	response, err := client.OpenSession(context.Background(), openSession)

	if (err != nil) {
		fmt.Println("Error opening session:", err)
        return
	}

	return &Connection {
		host: host,
		port: port,
		database: "default",
		auth: auth,
		kerberosServiceName: "",
		sessionHandle: response.SessionHandle,
		client: client,
		configuration: configuration,
		RequestSize: DEFAULT_FETCH_SIZE,
	}, nil
}

// Cursor creates a cursor from a connection
func (c *Connection) Cursor() *Cursor {
	return &Cursor{
		conn: c,
		queue: make([]interface{}, 0),
	}
}

// Cursor is used for fetching the rows after a query
type Cursor struct {
	conn *Connection
	operationHandle *hiveserver.TOperationHandle
	queue []interface{}
	response *hiveserver.TFetchResultsResp
}

// Execute sends a query to hive for execution
func (c *Cursor) Execute(query string) (err error) {
	executeReq := hiveserver.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.conn.sessionHandle
	executeReq.Statement = query

	responseExecute, err := c.conn.client.ExecuteStatement(context.Background(), executeReq)
	if err != nil {
        fmt.Println("Error executing statement:", err)
        return
	}
	if !success(responseExecute.GetStatus()) {
		return fmt.Errorf("Error while executing query: %s", responseExecute.Status.String())
	}

	c.operationHandle = responseExecute.OperationHandle
	fmt.Println(responseExecute, err)
	return nil
}

func success(status *hiveserver.TStatus) bool {
	statusCode := status.GetStatusCode()
	return statusCode == hiveserver.TStatusCode_SUCCESS_STATUS || statusCode == hiveserver.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

// FetchOne returns one row
func (c *Cursor) FetchOne() (row interface{}, err error) {
	fmt.Println("Polling until data")
	err = c.pollUntilData(context.Background(), 1)
	if err != nil {
		return
	}
	fmt.Println("Back from polling")
	nextRow := c.queue[0]
	c.queue = c.queue[1:]

	return nextRow, nil
}

func (c *Cursor) HasMore() bool{
	return *c.response.HasMoreRows
}

func (c *Cursor) pollUntilData(ctx context.Context, n int)  (err error) {
	ticker := time.NewTicker(time.Duration(pollIntervalInMillis) * time.Millisecond)
	quit := make(chan struct{})
	rowsAvailable := make(chan error)
	
	defer close(quit)
	defer close(rowsAvailable)


	go func() {
		for {
		    select {
			case <- ticker.C:
				fmt.Println("Sending request")
				fetchRequest := hiveserver.NewTFetchResultsReq()
				fetchRequest.OperationHandle = c.operationHandle
				fetchRequest.Orientation = hiveserver.TFetchOrientation_FETCH_NEXT
				fetchRequest.MaxRows = c.conn.RequestSize

				responseFetch, err := c.conn.client.FetchResults(ctx, fetchRequest)
				c.response = responseFetch
				if err != nil {
					fmt.Println("Error fecthing the response:", err)
					rowsAvailable <- err
					return
				}

				if responseFetch.Status.StatusCode != hiveserver.TStatusCode_SUCCESS_STATUS {
					fmt.Println("Request code is not sucess my friend")
					fmt.Println(responseFetch)
					rowsAvailable <- fmt.Errorf(responseFetch.Status.String())
					return
				}

				c.parseResults(responseFetch)
				if len(c.queue) >= n {
					rowsAvailable <- nil
				}
			case <- quit:
				ticker.Stop()
				return
			}
		}
	 }()

	select {
	case err = <- rowsAvailable:
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

func (c *Cursor) parseResults(response *hiveserver.TFetchResultsResp) {
	for _, element := range response.Results.GetColumns() {
		// row = make([]interface{}, *responseFetch.Results.ColumnCount)
		c.queue = append(c.queue, element)
	}
}

