package gohive

import (
	"encoding/base64"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/beltran/gosasl"
	"net/http"
	"os/user"
	"strings"
)

type HiveMetastoreClient struct {
	transport thrift.TTransport
	Client    *hive_metastore.ThriftHiveMetastoreClient
	server    string
	port      int
}

type MetastoreConnectConfiguration struct {
	TransportMode string
	Username      string
	Password      string
}

func NewMetastoreConnectConfiguration() *MetastoreConnectConfiguration {
	return &MetastoreConnectConfiguration{
		TransportMode: "binary",
		Username:      "",
		Password:      "",
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
			transport, err = NewTSaslTransport(socket, host, "GSSAPI", saslConfiguration, DEFAULT_MAX_LENGTH)
			if err != nil {
				return
			}
		} else if auth == "NONE" {
			if configuration.Password == "" {
				configuration.Password = "x"
			}
			var _user *user.User
			if configuration.Username == "" {
				_user, err = user.Current()
				if err != nil {
					return
				}
				configuration.Username = strings.Replace(_user.Name, " ", "", -1)
			}
			saslConfiguration := map[string]string{"username": configuration.Username, "password": configuration.Password}
			transport, err = NewTSaslTransport(socket, host, "PLAIN", saslConfiguration, DEFAULT_MAX_LENGTH)
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
		Client:    c,
		server:    host,
		port:      port,
	}, nil
}

func (c *HiveMetastoreClient) Close() {
	c.transport.Close()
}

func getHTTPClientForMeta() (httpClient *http.Client, protocol string, err error) {
	httpClient = http.DefaultClient
	protocol = "http"
	return
}
