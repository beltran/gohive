package gohive

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os/user"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/beltran/gohive/gohivemeta/hive_metastore"
	"github.com/beltran/gosasl"
)

type HiveMetastoreClient struct {
	context   context.Context
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
		context:   context.Background(),
		transport: transport,
		client:    c,
		server:    host,
		port:      port,
	}, nil
}

func (c *HiveMetastoreClient) Close() {
	c.transport.Close()
}

// GetAllDatabases returns list of all Hive databases.
func (c *HiveMetastoreClient) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(c.context)
}

func getHTTPClientForMeta() (httpClient *http.Client, protocol string, err error) {
	httpClient = http.DefaultClient
	protocol = "http"
	return
}

// GetDatabases returns list of all databases matching pattern. The pattern is interpreted by HMS
func (c *HiveMetastoreClient) GetDatabases(pattern string) ([]string, error) {
	return c.client.GetDatabases(c.context, pattern)
}

// GetDatabase returns detailed information about specified Hive database.
func (c *HiveMetastoreClient) GetDatabase(dbName string) (*Database, error) {
	db, err := c.client.GetDatabase(c.context, dbName)
	if err != nil {
		return nil, err
	}

	result := &Database{
		Name:        db.GetName(),
		Description: db.GetDescription(),
		Parameters:  db.GetParameters(),
		Location:    db.GetLocationUri(),
		Owner:       db.GetOwnerName(),
	}

	if db.OwnerType != nil {
		result.OwnerType = *db.OwnerType
	}

	return result, nil
}

// CreateDatabase creates database with the specified name, description, parameters and owner.
func (c *HiveMetastoreClient) CreateDatabase(db *Database) error {
	database := &hive_metastore.Database{
		Name:        db.Name,
		Description: db.Description,
		Parameters:  db.Parameters,
	}
	if db.Owner != "" {
		database.OwnerName = &db.Owner
	}
	// Thrift defines location as non-optional, but it turns out that it is optional for writing
	// (in which case HMS uses its own default) but not for reading.
	// The underlying Thrift-generated code is modified by hand to allow for missing locationUri
	// field. Here we send nil as location URI when location is empty.
	if db.Location != "" {
		database.LocationUri = db.Location
	}
	if db.OwnerType != 0 {
		database.OwnerType = &db.OwnerType
	}
	return c.client.CreateDatabase(c.context, database)
}

// DropDatabases removes the database specified by name
// Parameters:
//
//	dbName     - database name
//	deleteData - if true, delete data as well
//	cascade    - delete everything under the db if true
func (c *HiveMetastoreClient) DropDatabase(dbName string, deleteData bool, cascade bool) error {
	return c.client.DropDatabase(c.context, dbName, deleteData, cascade)
}

// GetAllTables returns list of all table names for a given database
func (c *HiveMetastoreClient) GetAllTables(dbName string) ([]string, error) {
	return c.client.GetAllTables(c.context, dbName)
}

// GetTables returns list of tables matching given pattern for the given database.
// Matching is performed on the server side.
func (c *HiveMetastoreClient) GetTables(dbName string, pattern string) ([]string, error) {
	return c.client.GetTables(c.context, dbName, pattern)
}

// GetTableObjects returns list of Table objects for the given database and list of table names.
func (c *HiveMetastoreClient) GetTableObjects(dbName string, tableNames []string) ([]*hive_metastore.Table, error) {
	return c.client.GetTableObjectsByName(c.context, dbName, tableNames)
}

// GetTable returns detailed information about the specified table
func (c *HiveMetastoreClient) GetTable(dbName string, tableName string) (*hive_metastore.Table, error) {
	return c.client.GetTable(c.context, dbName, tableName)
}

// CreateTable Creates HMS table
func (c *HiveMetastoreClient) CreateTable(table *hive_metastore.Table) error {
	return c.client.CreateTable(c.context, table)
}

// DropTable drops table.
// Parameters
//
//	dbName     - Database name
//	tableName  - Table name
//	deleteData - if True, delete data as well
func (c *HiveMetastoreClient) DropTable(dbName string, tableName string, deleteData bool) error {
	return c.client.DropTable(c.context, dbName, tableName, deleteData)
}

// GetPartitionNames returns list of partition names for a table.
func (c *HiveMetastoreClient) GetPartitionNames(dbName string, tableName string, max int) ([]string, error) {
	return c.client.GetPartitionNames(c.context, dbName, tableName, int16(max))
}

// GetPartitionByName returns Partition for the given partition name.
func (c *HiveMetastoreClient) GetPartitionByName(dbName string, tableName string,
	partName string) (*hive_metastore.Partition, error) {
	return c.client.GetPartitionByName(c.context, dbName, tableName, partName)
}

// GetPartitionsByNames returns multiple partitions specified by names.
func (c *HiveMetastoreClient) GetPartitionsByNames(dbName string, tableName string,
	partNames []string) ([]*hive_metastore.Partition, error) {
	return c.client.GetPartitionsByNames(c.context, dbName, tableName, partNames)
}

// AddPartition adds partition to Hive table.
func (c *HiveMetastoreClient) AddPartition(partition *hive_metastore.Partition) (*hive_metastore.Partition, error) {
	return c.client.AddPartition(c.context, partition)
}

// AddPartitions adds multipe partitions in a single call.
func (c *HiveMetastoreClient) AddPartitions(newParts []*hive_metastore.Partition) error {
	_, err := c.client.AddPartitions(c.context, newParts)
	return err
}

// GetPartitions returns all (or up to maxCount partitions of a table.
func (c *HiveMetastoreClient) GetPartitions(dbName string, tableName string,
	maxCount int) ([]*hive_metastore.Partition, error) {
	return c.client.GetPartitions(c.context, dbName, tableName, int16(maxCount))
}

// DropPartitionByName drops partition specified by name.
func (c *HiveMetastoreClient) DropPartitionByName(dbName string,
	tableName string, partName string, dropData bool) (bool, error) {
	return c.client.DropPartitionByName(c.context, dbName, tableName, partName, dropData)
}

// DropPartition drops partition specified by values.
func (c *HiveMetastoreClient) DropPartition(dbName string,
	tableName string, values []string, dropData bool) (bool, error) {
	return c.client.DropPartition(c.context, dbName, tableName, values, dropData)
}

// DropPartitions drops multiple partitions within a single table.
// Partitions are specified by names.
func (c *HiveMetastoreClient) DropPartitions(dbName string,
	tableName string, partNames []string) error {
	dropRequest := hive_metastore.NewDropPartitionsRequest()
	dropRequest.DbName = dbName
	dropRequest.TblName = tableName
	dropRequest.Parts = &hive_metastore.RequestPartsSpec{Names: partNames}
	_, err := c.client.DropPartitionsReq(c.context, dropRequest)
	return err
}

// GetCurrentNotificationId returns value of last notification ID
func (c *HiveMetastoreClient) GetCurrentNotificationId() (int64, error) {
	r, err := c.client.GetCurrentNotificationEventId(c.context)
	return r.EventId, err
}

// AlterTable modifies existing table with data from the new table
func (c *HiveMetastoreClient) AlterTable(dbName string, tableName string,
	table *hive_metastore.Table) error {
	return c.client.AlterTable(c.context, dbName, tableName, table)
}

// GetNextNotification returns next available notification.
func (c *HiveMetastoreClient) GetNextNotification(lastEvent int64,
	maxEvents int32) ([]*hive_metastore.NotificationEvent, error) {
	r, err := c.client.GetNextNotification(c.context,
		&hive_metastore.NotificationEventRequest{LastEvent: lastEvent, MaxEvents: &maxEvents})
	if err != nil {
		return nil, err
	}
	return r.Events, nil
}

// GetTableMeta returns list of tables matching specified search criteria.
// Parameters:
//
//	db - database name pattern
//	table - table name pattern
//	tableTypes - list of Table types - should be either TABLE or VIEW
func (c *HiveMetastoreClient) GetTableMeta(db string,
	table string, tableTypes []string) ([]*hive_metastore.TableMeta, error) {
	return c.client.GetTableMeta(c.context, db, table, tableTypes)
}

// GetTablesByType returns list of tables matching specified search criteria.
// Parameters:
//
//	dbName - database name
//	table - table name pattern
//	tableType - Table type - should be either TABLE or VIEW
func (c *HiveMetastoreClient) GetTablesByType(dbName string,
	table string, tableType string) ([]string, error) {
	return c.client.GetTablesByType(c.context, dbName, table, tableType)
}
