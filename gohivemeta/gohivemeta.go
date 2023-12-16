// +build all integration

package gohivemeta

import (
	"context"
	"fmt"
	"net/http"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/beltran/gosasl"
	"github.com/beltran/gohive"
	"github.com/beltran/gohive/gohivemeta/hive_metastore"
	"net"
	"strconv"
	"strings"
	"encoding/base64"
)

type TableType int

const (
	TableTypeManaged TableType = iota
	TableTypeExternal
	TableTypeView
	TableTypeIndex
)

// String representation of table types, consumed by Hive
var tableTypes = []string{
	"MANAGED_TABLE",
	"EXTERNAL_TABLE",
	"VIRTUAL_VIEW",
	"INDEX_TABLE",
}

const (
	bufferSize = 1024 * 1024
)

// HiveMetastoreClient.
type HiveMetastoreClient struct {
	context   context.Context
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
	server    string
	port      int
}

// Database is a container of other objects in Hive.
type Database struct {
	Name        string                       `json:"name"`
	Description string                       `json:"description,omitempty"`
	Owner       string                       `json:"owner,omitempty"`
	OwnerType   hive_metastore.PrincipalType `json:"ownerType,omitempty"`
	Location    string                       `json:"location"`
	Parameters  map[string]string            `json:"parameters,omitempty"`
}

func (val TableType) String() string {
	return tableTypes[val]
}

// Open connection to metastore and return client handle.
func Open(host string, port int, auth string) (client *HiveMetastoreClient, err error) {
	server := host
	portStr := strconv.Itoa(port)
	if strings.Contains(host, ":") {
		s, pStr, err := net.SplitHostPort(host)
		if err != nil {
			return nil, err
		}
		server = s
		portStr = pStr
	}

	socket, err := thrift.NewTSocket(net.JoinHostPort(server, portStr))
	if err != nil {
		return nil, fmt.Errorf("error resolving address %s: %v", host, err)
	}

	if err = socket.Open(); err != nil {
		return
	}

	var transport thrift.TTransport

	if auth == "KERBEROS" {
		saslConfiguration := map[string]string{"service": "hive", "javax.security.sasl.qop": auth, "javax.security.sasl.server.authentication": "true"}
		transport, err = gohive.NewTSaslTransport(socket, host, "GSSAPI", saslConfiguration)
		if err != nil {
			return
		}
	} else if auth == "HTTP" {
		mechanism, err := gosasl.NewGSSAPIMechanism("hive")
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

		httpClient, protocol, err := getHTTPClient()
		if err != nil {
			return nil, err
		}

		httpOptions := thrift.THttpClientOptions{
			Client: httpClient,
		}
		transport, err = thrift.NewTHttpClientTransportFactoryWithOptions(fmt.Sprintf(protocol+"://%s:%d/"+"cliservice", host, port), httpOptions).GetTransport(socket)
		httpTransport, ok := transport.(*thrift.THttpClient)
		if ok {
			httpTransport.SetHeader("Authorization", "Negotiate "+base64.StdEncoding.EncodeToString(token))
		}
		if err != nil {
			return nil, err
		}
	} else if auth == "NONE" {
		saslConfiguration := map[string]string{"username": "hive", "password": "pass"}
		transport, err = gohive.NewTSaslTransport(socket, host, "PLAIN", saslConfiguration)
		if err != nil {
			return
		}
	} else if auth == "NOSASL" {
		transport = thrift.NewTBufferedTransport(socket, 4096)
			if transport == nil {
				return nil, fmt.Errorf("BufferedTransport was nil")
			}
	} else {
		panic("Unrecognized auth")
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	c := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	if !transport.IsOpen() {
		if err = transport.Open(); err != nil {
			return
		}
	}
	return &HiveMetastoreClient{
		context:   context.Background(),
		transport: transport,
		client:    c,
		server:    host,
		port:      port,
	}, nil
}

// Close connection to metastore.
func (c *HiveMetastoreClient) Close() {
	c.transport.Close()
}

// GetAllDatabases returns list of all Hive databases.
func (c *HiveMetastoreClient) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(c.context)
}

// GetDatabases returns list of all databases matching pattern. The pattern is interpreted by HMS.
func (c *HiveMetastoreClient) GetDatabases(pattern string) ([]string, error) {
	return c.client.GetDatabases(c.context, pattern)
}

func getHTTPClient() (httpClient *http.Client, protocol string, err error) {
	httpClient = http.DefaultClient
	protocol = "http"
	return
}
