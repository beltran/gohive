package gohive

import (
	"context"
	"fmt"
	"net/http"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/beltran/gosasl"
	"github.com/beltran/gohive/gohivemeta/hive_metastore"
	"encoding/base64"
)

type HiveMetastoreClient struct {
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
	server    string
	port      int
}

type Database struct {
	Name        string                       `json:"name"`
	Description string                       `json:"description,omitempty"`
	Owner       string                       `json:"owner,omitempty"`
	OwnerType   hive_metastore.PrincipalType `json:"ownerType,omitempty"`
	Location    string                       `json:"location"`
	Parameters  map[string]string            `json:"parameters,omitempty"`
}

type MetastoreConnectConfiguration struct {
	TransportMode	string
}

func NewMetastoreConnectConfiguration() *MetastoreConnectConfiguration{
	return &MetastoreConnectConfiguration{
		TransportMode:	"binary",
	}
}

// Open connection to the metastore.
func ConnectToMetastore(host string, port int, auth string, configuration *MetastoreConnectConfiguration) (client *HiveMetastoreClient, err error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	socket, err := thrift.NewTSocket(addr)
	if err != nil {
		return nil, fmt.Errorf("error resolving address %s: %v", host, err)
	}

	if err = socket.Open(); err != nil {
		return
	}

	var transport thrift.TTransport

	if configuration.TransportMode == "binary" {
		if auth == "KERBEROS" {
			saslConfiguration := map[string]string{"service": "hive", "javax.security.sasl.qop": auth, "javax.security.sasl.server.authentication": "true"}
			transport, err = NewTSaslTransport(socket, host, "GSSAPI", saslConfiguration, 16384000)
			if err != nil {
				return
			}
		} else if auth == "NONE" {
			saslConfiguration := map[string]string{"username": "hive", "password": "pass"}
			transport, err = NewTSaslTransport(socket, host, "PLAIN", saslConfiguration, 16384000)
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
	} else if configuration.TransportMode == "http" {
		if auth == "KERBEROS" {
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

			httpClient, protocol, err := getHTTPClientForMeta()
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
		} else {
			 panic("Unrecognized auth")
		}
	} else {
		panic("Unrecognized transport mode " + configuration.TransportMode)
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
		transport: transport,
		client:    c,
		server:    host,
		port:      port,
	}, nil
}

func (c *HiveMetastoreClient) Close() {
	c.transport.Close()
}

func (c *HiveMetastoreClient, ) GetAllDatabases(context context.Context) ([]string, error) {
	return c.client.GetAllDatabases(context)
}

func getHTTPClientForMeta() (httpClient *http.Client, protocol string, err error) {
	httpClient = http.DefaultClient
	protocol = "http"
	return
}
