//go:build all || integration
// +build all integration

package gohive

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
)

func TestConnectDefault(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	ssl := os.Getenv("SSL")
	if auth != "KERBEROS" || transport != "binary" || ssl == "1" {
		return
	}

	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	connection, err := Connect("hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
}

func prepareAllTypesTable(t *testing.T, cursor *Cursor) {
	createAllTypesTable(t, cursor)
	insertAllTypesTable(t, cursor)
}

func prepareAllTypesTableWithNull(t *testing.T, cursor *Cursor) {
	createAllTypesTableNoUnion(t, cursor)
	insertAllTypesTableWithNulls(t, cursor)
}

func createAllTypesTable(t *testing.T, cursor *Cursor) {
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	createAll := "CREATE TABLE all_types (" +
		"`boolean` BOOLEAN," +
		"`tinyint` TINYINT," +
		"`smallint` SMALLINT," +
		"`int` INT," +
		"`bigint` BIGINT," +
		"`float` FLOAT," +
		"`double` DOUBLE," +
		"`string` STRING," +
		"`timestamp` TIMESTAMP," +
		"`binary` BINARY," +
		"`array` ARRAY<int>," +
		"`map` MAP<int, int>," +
		"`struct` STRUCT<a: int, b: int>," +
		"`union` UNIONTYPE<int, string>," +
		"`decimal` DECIMAL(10, 1))"
	cursor.Execute(context.Background(), createAll, false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
}

func createAllTypesTableNoUnion(t *testing.T, cursor *Cursor) {
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	createAll := "CREATE TABLE all_types (" +
		"`boolean` BOOLEAN," +
		"`tinyint` TINYINT," +
		"`smallint` SMALLINT," +
		"`int` INT," +
		"`bigint` BIGINT," +
		"`float` FLOAT," +
		"`double` DOUBLE," +
		"`string` STRING," +
		"`timestamp` TIMESTAMP," +
		"`binary` BINARY," +
		"`array` ARRAY<int>," +
		"`map` MAP<int, int>," +
		"`struct` STRUCT<a: int, b: int>," +
		"`decimal` DECIMAL(10, 1))"
	cursor.Execute(context.Background(), createAll, false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
}

func insertAllTypesTable(t *testing.T, cursor *Cursor) {
	insertAll := `INSERT INTO TABLE all_types VALUES(
		true,
		127,
		32767,
		2147483647,
		9223372036854775807,
		0.5,
		0.25,
		'a string',
		0,
		'123',
		array(1, 2),
		map(1, 2, 3, 4),
		named_struct('a', 1, 'b', 2),
		create_union(0, 1, 'test_string'),
		0.1)`
	cursor.Execute(context.Background(), insertAll, false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
}

func insertAllTypesTableWithNulls(t *testing.T, cursor *Cursor) {
	insertAll := "INSERT INTO TABLE all_types(`int`) VALUES(2147483647)"
	cursor.Execute(context.Background(), insertAll, false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
}

func prepareTable(t *testing.T, rowsToInsert int, fetchSize int64) (*Connection, *Cursor) {
	connection, cursor := makeConnection(t, fetchSize)
	createTable(t, cursor)
	insertInTable(t, cursor, rowsToInsert)
	return connection, cursor
}

func prepareTableSingleValue(t *testing.T, rowsToInsert int, fetchSize int64) (*Connection, *Cursor) {
	connection, cursor := makeConnection(t, fetchSize)
	createTable(t, cursor)
	insertInTableSingleValue(t, cursor, rowsToInsert)
	return connection, cursor
}

func createTable(t *testing.T, cursor *Cursor) {
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	if !cursor.Finished() {
		t.Fatal("Finished should be true")
	}
	cursor.Execute(context.Background(), "CREATE TABLE pokes (a INT, b STRING)", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	if !cursor.Finished() {
		t.Fatal("Finished should be true")
	}
}

func insertInTable(t *testing.T, cursor *Cursor, rowsToInsert int) {
	if rowsToInsert > 0 {
		values := ""
		for i := 1; i <= rowsToInsert; i++ {
			values += fmt.Sprintf("(%d, '%d')", i, i)
			if i != rowsToInsert {
				values += ","
			}
		}
		cursor.Execute(context.Background(), "INSERT INTO pokes VALUES "+values, false)
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		if !cursor.Finished() {
			t.Fatal("Finished should be true")
		}
	}
}

func insertInTableSingleValue(t *testing.T, cursor *Cursor, rowsToInsert int) {
	if rowsToInsert > 0 {
		values := ""
		for i := 1; i <= rowsToInsert; i++ {
			values += fmt.Sprintf("('%d')", i)
			if i != rowsToInsert {
				values += ","
			}
		}
		cursor.Execute(context.Background(), "INSERT INTO pokes(b) VALUES "+values, false)
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		if !cursor.Finished() {
			t.Fatal("Finished should be true")
		}
	}
}

func makeConnection(t *testing.T, fetchSize int64) (*Connection, *Cursor) {
	return makeConnectionWithConfiguration(t, fetchSize, nil)
}

func makeConnectionWithConfiguration(t *testing.T, fetchSize int64, hiveConfiguration map[string]string) (*Connection, *Cursor) {
	mode := getTransport()
	ssl := getSsl()
	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	configuration.FetchSize = fetchSize
	configuration.TransportMode = mode
	configuration.HiveConfiguration = hiveConfiguration
	configuration.MaxSize = 16384001

	if ssl {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}

	var port int = 10000
	if mode == "http" {
		port = 10000
		configuration.HTTPPath = "cliservice"
	}
	connection, errConn := Connect("hs2.example.com", port, getAuth(), configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	return connection, cursor
}

func makeConnectionWithConnectConfiguration(t *testing.T, configuration *ConnectConfiguration) (*Connection, *Cursor) {
	mode := getTransport()
	ssl := getSsl()
	if ssl {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}

	var port int = 10000
	if mode == "http" {
		port = 10000
		configuration.HTTPPath = "cliservice"
	}
	connection, errConn := Connect("hs2.example.com", port, getAuth(), configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	return connection, cursor
}

func closeAll(t *testing.T, connection *Connection, cursor *Cursor) {
	cursor.Close()
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}
	err := connection.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func getAuth() string {
	auth := os.Getenv("AUTH")
	os.Setenv("KRB5CCNAME", "/tmp/krb5_gohive")
	if auth == "" {
		auth = "KERBEROS"
	}
	return auth
}

func getTransport() string {
	transport := os.Getenv("TRANSPORT")
	if transport == "" {
		transport = "binary"
	}
	return transport
}

func getSsl() bool {
	ssl := os.Getenv("SSL")
	if ssl == "1" {
		return true
	}
	return false
}

func getTlsConfiguration(SslPemPath, SslKeyPath string) (tlsConfig *tls.Config, err error) {
	var cert tls.Certificate
	cert, err = tls.LoadX509KeyPair(SslPemPath, SslKeyPath)
	if err != nil {
		return
	}

	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	tlsConfig.BuildNameToCertificate()
	return
}
