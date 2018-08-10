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
	c.operationHandle = responseExecute.OperationHandle
	fmt.Println(responseExecute, err)
	return nil
}

// FetchOne returns one row
func (c *Cursor) FetchOne() (row interface{}, err error) {
	c.pollUntilData(context.Background())
	nextRow := c.queue[0]
	c.queue = c.queue[1:]

	return nextRow, nil
}

func (c *Cursor) pollUntilData(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(pollIntervalInMillis) * time.Millisecond)
	quit := make(chan struct{})
	defer close(quit)

	rowsAvailable := make(chan struct{})

	go func() {
		for {
		    select {
			case <- ticker.C:
				fetchRequest := hiveserver.NewTFetchResultsReq()
				fetchRequest.OperationHandle = c.operationHandle
				fetchRequest.Orientation = hiveserver.TFetchOrientation_FETCH_NEXT
				fetchRequest.MaxRows = 10

				responseFetch, err := c.conn.client.FetchResults(context.Background(), fetchRequest)
				if err != nil {
					fmt.Println("Error fecthing the response:", err)
					return
				}

				if responseFetch.Status.StatusCode != hiveserver.TStatusCode_SUCCESS_STATUS {
					return
				}

				c.parseResults(responseFetch)
				close(rowsAvailable)
			case <- quit:
				ticker.Stop()
				return
			}
		}
	 }()

	select {
	case <-rowsAvailable:
	case <-ctx.Done(): 
	}
	if len(c.queue) == 0 {
		return fmt.Errorf("No rows where received")
	}
	return nil
}

func (c *Cursor) parseResults(response *hiveserver.TFetchResultsResp) {
	for _, element := range response.Results.GetColumns() {
		// row = make([]interface{}, *responseFetch.Results.ColumnCount)
		c.queue = append(c.queue, element)
	}
}
