// +build all integration

package gohive

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
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


func TestConnectZookeeper(t *testing.T) {
	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	_, err := ConnectZookeeper("host1:port1,host2:port2", getAuth(), configuration)
	if err == nil {
		t.Fatal("error was expected")
	}
}

func TestDomainDoesntExist(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	ssl := os.Getenv("SSL")
	if auth != "KERBEROS" || transport != "binary" || ssl == "1" {
		return
	}

	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	_, err := Connect("nonexistentdomain", 10000, getAuth(), configuration)
	if err == nil {
		t.Fatal("Expected error because domain doesn't exist")
	}
}

func TestConnectDigestMd5(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	ssl := os.Getenv("SSL")
	if auth != "NONE" || transport != "binary" || ssl == "1" {
		return
	}

	configuration := NewConnectConfiguration()
	configuration.Service = "null"
	configuration.Password = "pass"
	configuration.Username = "hive"
	_, err := Connect("hs2.example.com", 10000, "DIGEST-MD5", configuration)
	if err == nil {
		t.Fatal("Error was expected because the server won't accept this mechanism")
	}
}

func TestResuseConnection(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute(context.Background(), "SHOW DATABASES", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	cursor.Close()

	newCursor := connection.Cursor()
	cursor.Execute(context.Background(), "SHOW DATABASES", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	closeAll(t, connection, newCursor)
}

func TestConnectHttp(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	ssl := os.Getenv("SSL")
	if transport != "http" {
		return
	}
	configuration := NewConnectConfiguration()
	configuration.TransportMode = transport
	configuration.Service = "hive"
	if ssl == "1" {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}
	connection, err := Connect("hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
}

func TestConnectSasl(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	ssl := os.Getenv("SSL")
	if transport != "binary" {
		return
	}
	configuration := NewConnectConfiguration()
	configuration.TransportMode = "binary"
	configuration.Service = "hive"
	if ssl == "1" {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}
	connection, err := Connect("hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
}

func TestClosedPort(t *testing.T) {
	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	_, err := Connect("hs2.example.com", 12345, getAuth(), configuration)
	if err == nil {
		t.Fatal(err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("Wrong error: %s, it should contain connection refused", err.Error())
	}
}

func TestFetchDatabase(t *testing.T) {
	async := false
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute(context.Background(), "SHOW DATABASES", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var s string
	cursor.FetchOne(context.Background(), &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}
	if s != "default" {
		t.Fatalf("Unrecognized dabase found: %s", s)
	}
	closeAll(t, connection, cursor)
}

func TestCreateTable(t *testing.T) {
	async := false
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS pokes6", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	// Now it should fail because the table already exists
	cursor.Execute(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)", async)
	if cursor.Error() == nil {
		t.Fatal("Error should have happened")
	}
	closeAll(t, connection, cursor)
}

func TestManyFailures(t *testing.T) {
	async := false
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS pokes6", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	for i := 0; i < 20; i++ {
		// Now it should fail because the table already exists
		cursor.Execute(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)", async)
		if cursor.Error() == nil {
			t.Fatal("Error should have happened")
		}
	}

	closeAll(t, connection, cursor)
}

func TestDescription(t *testing.T) {
	async := false
	connection, cursor := prepareTable(t, 2, 1000)

	// We come from an insert
	d := cursor.Description()
	expected := [][]string{[]string{"col1", "INT_TYPE"}, []string{"col2", "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	d = cursor.Description()
	expected = [][]string{[]string{"pokes.a", "INT_TYPE"}, []string{"pokes.b", "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int32
	var s string
	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	d = cursor.Description()
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	expected = [][]string{[]string{"pokes.a", "INT_TYPE"}, []string{"pokes.b", "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}

	// Call again it will follow a different path
	d = cursor.Description()
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	closeAll(t, connection, cursor)
}

func TestDescriptionAsync(t *testing.T) {
	async := true
	connection, cursor := prepareTable(t, 2, 1000)

	cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	d := cursor.Description()
	expected := [][]string{[]string{"pokes.a", "INT_TYPE"}, []string{"pokes.b", "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.WaitForCompletion(context.Background())
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int32
	var s string

	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}
	if i != 1 || s != "1" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	closeAll(t, connection, cursor)
}

func TestHiveProperties(t *testing.T) {
	configuration := map[string]string{
		"hadoop.madeup.one": "one",
	}

	connection, cursor := makeConnectionWithConfiguration(t, 1000, configuration)

	cursor.Exec(context.Background(), "SET hadoop.madeup.one")

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{"set": "hadoop.madeup.one=one"}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.Exec(context.Background(), "SET hadoop.madeup.two = two")
	cursor.Exec(context.Background(), "SET hadoop.madeup.two")

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	m = cursor.RowMap(context.Background())
	expected = map[string]interface{}{"set": "hadoop.madeup.two=two"}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	closeAll(t, connection, cursor)
}

func TestSelect(t *testing.T) {
	connection, cursor := prepareTable(t, 6000, 1000)

	var i int32
	var s string
	var j int32
	var z int

	for z, j = 0, 0; z < 10; z, j, i, s = z+1, 0, 0, "-1" {
		cursor.Exec(context.Background(), "SELECT * FROM pokes")
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		if !cursor.Finished() {
			t.Fatal("Finished should be true")
		}
		for cursor.HasMore(context.Background()) {
			if cursor.Error() != nil {
				t.Fatal(cursor.Error())
			}
			cursor.FetchOne(context.Background(), &i, &s)
			if cursor.Err != nil {
				t.Fatal(cursor.Err)
			}
			j++
		}
		if i != 6000 || s != "6000" {
			log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
		}
		if cursor.HasMore(context.Background()) {
			log.Fatal("Shouldn't have any more values")
		}
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		if j != 6000 {
			t.Fatalf("6000 rows expected here")
		}
	}
	closeAll(t, connection, cursor)
}

func TestUseDatabase(t *testing.T) {
	async := false

	connection, cursor := makeConnection(t, 1000)

	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS db.pokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "DROP DATABASE IF EXISTS db", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "CREATE DATABASE db", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "CREATE TABLE db.pokes (foo INT, bar INT)", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "USE db", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "INSERT INTO pokes VALUES(1, 1111)", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Exec(context.Background(), "SELECT * FROM pokes")
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{"pokes.foo": int32(1), "pokes.bar": int32(1111)}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.Exec(context.Background(), "SELECT * FROM db.pokes")
	m = cursor.RowMap(context.Background())
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS db.pokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "DROP DATABASE IF EXISTS db", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	closeAll(t, connection, cursor)
}

func TestSetDatabaseConfig(t *testing.T) {
	async := false
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS datbas.dpokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS dpokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "CREATE DATABASE IF NOT EXISTS datbas", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	configuration := NewConnectConfiguration()
	configuration.Database = "datbas"
	configuration.Service = "hive"
	configuration.FetchSize = 1000
	configuration.TransportMode = getTransport()
	configuration.HiveConfiguration = nil

	connection, cursor = makeConnectionWithConnectConfiguration(t, configuration)

	cursor.Execute(context.Background(), "CREATE TABLE datbas.dpokes (foo INT, bar INT)", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "INSERT INTO dpokes VALUES(1, 1111)", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Exec(context.Background(), "SELECT * FROM dpokes")
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{"dpokes.foo": int32(1), "dpokes.bar": int32(1111)}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.Exec(context.Background(), "SELECT * FROM datbas.dpokes")
	m = cursor.RowMap(context.Background())
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS datbas.dpokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "DROP DATABASE IF EXISTS datbas", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	closeAll(t, connection, cursor)
}

func TestSelectNull(t *testing.T) {
	async := false
	connection, cursor := prepareTableSingleValue(t, 6000, 1000)
	cursor.Exec(context.Background(), "INSERT into pokes(a) values(1);")
	closeAll(t, connection, cursor)

	connection, cursor = makeConnection(t, 199)

	cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	if !cursor.Finished() {
		t.Fatal("Finished should be true")
	}
	j := 0
	for cursor.HasMore(context.Background()) {
		var i *int32 = new(int32)
		var s *string = new(string)
		*i = 1
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		cursor.FetchOne(context.Background(), &i, &s)
		if cursor.Err != nil {
			t.Fatal(cursor.Err)
		}
		if i != nil {
			log.Fatalf("Unexpected value for i: %d", *i)
		}
		if j == 6000 {
			if *i != 1 && s != nil {
				log.Fatalf("Unexpected values for i(%d)  or s(%s) ", *i, *s)
			}
		} else {
			if i != nil && *s != strconv.Itoa(j) {
				log.Fatalf("Unexpected values for i(%d)  or s(%s) ", *i, *s)
			}
		}
		j++
	}
	if cursor.HasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	if j != 6000 {
		t.Fatalf("6000 rows expected here")
	}
	closeAll(t, connection, cursor)
}

func TestSimpleSelect(t *testing.T) {
	connection, cursor := prepareTable(t, 1, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	var s string
	var i int32
	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	closeAll(t, connection, cursor)
}

func TestSimpleSelectWithNil(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	cursor.Execute(context.Background(), "INSERT INTO pokes VALUES (1, NULL) ", false)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	var s string
	var i int32
	cursor.FetchOne(context.Background(), &i, &s)

	if i != 1 || s != "" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}

	closeAll(t, connection, cursor)
}

func TestIsRow(t *testing.T) {
	connection, cursor := prepareTable(t, 1, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int32
	var s string

	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if i != 1 || s != "1" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.HasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	for i := 0; i < 10; i++ {
		cursor.FetchOne(context.Background(), &i, &s)
		if cursor.Error() == nil {
			t.Fatal("Error shouldn't be nil")
		}
		if cursor.Err.Error() != "No more rows are left" {
			t.Fatal("Error should be 'No more rows are left'")
		}
	}

	closeAll(t, connection, cursor)
}

func TestFetchContext(t *testing.T) {
	connection, cursor := prepareTable(t, 2, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	var i int32
	var s string

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
	defer cancel()
	time.Sleep(500 * time.Millisecond)
	cursor.FetchOne(ctx, &i, &s)

	if cursor.Error() == nil {
		t.Fatal("Error should be context has been done")
	}
	closeAll(t, connection, cursor)
}

func TestFetchLogs(t *testing.T) {
	connection, cursor := prepareTable(t, 2, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	logs := cursor.FetchLogs()
	if logs == nil {
		t.Fatal("Logs should not be nil")
	}

	if len(logs) == 0 {
		t.Fatal("Logs should non-empty")
	}

	if cursor.Error() != nil {
		t.Fatal("Error should be nil")
	}

	closeAll(t, connection, cursor)
}

func TestFetchLogsDuringExecution(t *testing.T) {
	connection, cursor := prepareTable(t, 2, 1000)
	// Buffered so we only have to read at end
	
	logs := make(chan []string, 30)
	defer close(logs)
	
	cursor.Logs = logs
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	if len(logs) == 0 {
		t.Fatal("Logs should be non-empty")
	}

	closeAll(t, connection, cursor)
}

func TestHiveError(t *testing.T) {
	connection, cursor := prepareTable(t, 2, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM table_doesnt_exist", false)
	if cursor.Error() == nil {
		t.Fatal("Querying a non-existing table should cause an error")
	}

	hiveErr, ok := cursor.Error().(HiveError)
	if !ok {
		t.Fatal("A HiveError should have been returned")
	}

	// table not found is code 10001 (1xxxx is SemanticException)
	if hiveErr.ErrorCode != 10001 {
		t.Fatalf("expected error code 10001, got %d", hiveErr.ErrorCode)
	}

	closeAll(t, connection, cursor)
}

func TestHasMoreContext(t *testing.T) {
	connection, cursor := prepareTable(t, 2, 1)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	var i int32
	var s string

	cursor.FetchOne(context.Background(), &i, &s)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
	defer cancel()
	time.Sleep(500 * time.Millisecond)
	cursor.HasMore(ctx)
	if cursor.Error() == nil {
		t.Fatal("Error should be context has been done")
	}
	closeAll(t, connection, cursor)
}

func TestRowMap(t *testing.T) {
	connection, cursor := prepareTable(t, 2, 1)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{"pokes.a": int32(1), "pokes.b": "1"}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	m = cursor.RowMap(context.Background())
	expected = map[string]interface{}{"pokes.a": int32(2), "pokes.b": "2"}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	if cursor.HasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}

	closeAll(t, connection, cursor)
}

func TestRowMapColumnRename(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.Exec(context.Background(), "create table if not exists t(a int, b int)")
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	cursor.Exec(context.Background(), "insert into t values(1,2)")
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	cursor.Exec(context.Background(), "select * from t as x left join t as y on x.a=y.b")
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{"x.a": int32(1), "x.b": int32(2), "y.a": nil, "y.b": nil}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	if cursor.HasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	cursor.Exec(context.Background(), "drop table t")
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	closeAll(t, connection, cursor)
}

func TestRowMapAllTypes(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{
		"all_types.smallint":  int16(32767),
		"all_types.int":       int32(2147483647),
		"all_types.float":     float64(0.5),
		"all_types.double":    float64(0.25),
		"all_types.string":    "a string",
		"all_types.boolean":   true,
		"all_types.struct":    "{\"a\":1,\"b\":2}",
		"all_types.bigint":    int64(9223372036854775807),
		"all_types.array":     "[1,2]",
		"all_types.map":       "{1:2,3:4}",
		"all_types.decimal":   "0.1",
		"all_types.binary":    []uint8{49, 50, 51},
		"all_types.timestamp": "1970-01-01 00:00:00",
		"all_types.union":     "{0:1}",
		"all_types.tinyint":   int8(127),
	}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	closeAll(t, connection, cursor)
}

func TestRowMapAllTypesWithNull(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTableWithNull(t, cursor)

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{
		"all_types.smallint":  nil,
		"all_types.int":       int32(2147483647),
		"all_types.float":     nil,
		"all_types.double":    nil,
		"all_types.string":    nil,
		"all_types.boolean":   nil,
		"all_types.struct":    nil,
		"all_types.bigint":    nil,
		"all_types.array":     nil,
		"all_types.map":       nil,
		"all_types.decimal":   nil,
		"all_types.binary":    nil,
		"all_types.timestamp": nil,
		"all_types.tinyint":   nil,
	}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	closeAll(t, connection, cursor)
}

func TestSmallFetchSize(t *testing.T) {
	async := false
	connection, cursor := prepareTable(t, 4, 2)

	var i int32
	var s string
	var j int

	cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	// Fetch all rows
	// The query happens behind the scenes
	// The other rows are discarted
	for j = 0; cursor.HasMore(context.Background()); {
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		cursor.FetchOne(context.Background(), &i, &s)
		if cursor.Err != nil {
			t.Fatal(cursor.Err)
		}
		j++
	}
	if i != 4 || s != "4" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.HasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	if j != 4 {
		t.Fatalf("Fetch size was set to 4 but had %d iterations", j)
	}

	closeAll(t, connection, cursor)
}

func TestWithContextSync(t *testing.T) {
	if os.Getenv("TRANSPORT") == "http" {
		if os.Getenv("SKIP_UNSTABLE") == "1" {
			return
		}
	}
	connection, cursor := prepareTable(t, 0, 1000)

	values := []int{0, 0, 0, 200, 200, 200, 300, 400, 100, 500, 1000}

	for _, value := range values {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(value)*time.Millisecond)
		defer cancel()
		cursor.Execute(ctx, "SELECT reflect('java.lang.Thread', 'sleep', 1000L * 1000L) FROM pokes a JOIN pokes b", false)
		if cursor.Error() == nil {
			t.Fatal("Error should be context has been done")
		}

		if strings.Contains(cursor.Error().Error(), "context") {
			if cursor.HasMore(context.Background()) {
				t.Fatal("All rows should have been read")
			}
			if cursor.Error() != nil {
				t.Fatal(cursor.Error())
			}
		}
	}

	closeAll(t, connection, cursor)
}

func TestWithContextAsync(t *testing.T) {
	if os.Getenv("TRANSPORT") == "http" {
		if os.Getenv("SKIP_UNSTABLE") == "1" {
			return
		}
	}
	connection, cursor := prepareTable(t, 0, 1000)

	value := 0

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(value)*time.Millisecond)
		defer cancel()
		time.Sleep(100 * time.Millisecond)
		cursor.Execute(ctx, "SELECT reflect('java.lang.Thread', 'sleep', 1000L * 1000L) FROM pokes a JOIN pokes b", true)
		if cursor.Error() != nil {
			t.Fatal("Error shouldn't happen despite the context being done: ", cursor.Err)
		}
	}

	closeAll(t, connection, cursor)
}

func TestExecute(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	if auth == "KERBEROS" && transport == "http" || true {
		return
	}

	connection, cursor := prepareTable(t, 0, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	cursor.Execute(ctx, "INSERT INTO pokes VALUES(1, '1')", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	if !cursor.Finished() {
		t.Fatal("Operation should have finished")
	}

	cursor.Cancel()
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	cursor.Execute(context.Background(), "INSERT INTO pokes VALUES(2, '2')", true)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int32
	var s string
	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("All rows should have been read")
	}

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	if i != 2 || s != "2" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}

	closeAll(t, connection, cursor)
}

func TestConsecutiveAsyncStatements(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	async_statements := []string{"INSERT INTO pokes VALUES(1, '1')", "USE DEFAULT", "USE DEFAULT", "SELECT * FROM pokes", "SELECT * FROM pokes"}

	for _, stm := range async_statements {
		cursor.Execute(context.Background(), stm, true)
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}

		cursor.WaitForCompletion(context.Background())

		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
	}
	closeAll(t, connection, cursor)
}

func TestAsync(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	start := time.Now()

	cursor.Execute(context.Background(), "INSERT INTO pokes VALUES(1, '1')", true)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	stop := time.Now()
	elapsed := stop.Sub(start)
	if elapsed > time.Duration(time.Second*7) {
		t.Fatal("It shouldn't have taken more than 7 seconds to run the query in async mode")
	}

	for !cursor.Finished() {
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("Shouldn't have any more rows")
	}

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int32
	var s string
	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("All rows should have been read")
	}

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	if i != 1 || s != "1" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}

	closeAll(t, connection, cursor)
}

func TestWaitForCompletion(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	cursor.Execute(context.Background(), "INSERT INTO pokes VALUES(1, '1')", true)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.WaitForCompletion(context.Background())

	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int32
	var s string
	cursor.FetchOne(context.Background(), &i, &s)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("All rows should have been read")
	}

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	if i != 1 || s != "1" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}

	closeAll(t, connection, cursor)
}

func TestWaitForCompletionContext(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes d, pokes e, pokes f order by d.a, e.a, f.a", true)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	values := []int{0, 0, 0}
	for _, value := range values {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
		defer cancel()
		time.Sleep(time.Duration(value) * time.Millisecond)
		cursor.WaitForCompletion(ctx)

		if cursor.Error() == nil {
			t.Fatal("Context should have been done")
		}
	}

	closeAll(t, connection, cursor)
}

func TestCancel(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	start := time.Now()
	cursor.Execute(context.Background(),
		"INSERT INTO pokes values(1, '1')", true)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	stop := time.Now()
	elapsed := stop.Sub(start)
	if elapsed > time.Duration(time.Second*8) {
		t.Fatal("It shouldn't have taken more than 8 seconds to run the query in async mode")
	}
	cursor.Cancel()
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	for !cursor.Finished() {
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}

	cursor.Execute(context.Background(), "SELECT count(*) FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	var i int64 = 10
	cursor.FetchOne(context.Background(), &i)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("All rows should have been read")
	}

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	if i != 0 {
		t.Fatal("Table should have zero rows")
	}

	closeAll(t, connection, cursor)
}

func TestNoResult(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)

	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("Shouldn't have any rows")
	}

	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	closeAll(t, connection, cursor)
}

func TestHasMore(t *testing.T) {
	connection, cursor := prepareTable(t, 5, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	for i := 0; i < 10; i++ {
		if !cursor.HasMore(context.Background()) {
			t.Fatalf("Should have more rows, iteration %d", i)
		}
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
	}

	var j int32
	var s string
	for i := 0; i < 5; i++ {
		if !cursor.HasMore(context.Background()) {
			t.Fatalf("Should have more rows, iteration %d", i)
		}
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
		cursor.FetchOne(context.Background(), &j, &s)
		if cursor.Err != nil {
			t.Fatal(cursor.Err)
		}
		if cursor.Error() != nil {
			t.Fatal(cursor.Error())
		}
	}
	if cursor.HasMore(context.Background()) {
		t.Fatalf("Should not have more rows")
	}
	closeAll(t, connection, cursor)
}

func TestTypesError(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	var b bool
	var tinyInt int8
	var smallInt int16
	var normalInt int32
	var bigInt int64
	// This value is store as a float32. The go thrift API returns a floa64 though.
	var floatType float64
	var double float64
	var s string
	var timeStamp string
	var binary []byte
	var array string
	var mapType string
	var structType string
	var union string
	var decimal string
	var dummy chan<- int

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &dummy, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &dummy, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &dummy, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &dummy, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &dummy,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&dummy, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &dummy, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &dummy, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &dummy, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &dummy, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &dummy, &mapType, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &dummy, &structType, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &dummy, &union, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &dummy, &decimal)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &dummy)
	if cursor.Err == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	d := cursor.Description()
	expected := [][]string{
		[]string{"all_types.boolean", "BOOLEAN_TYPE"},
		[]string{"all_types.tinyint", "TINYINT_TYPE"},
		[]string{"all_types.smallint", "SMALLINT_TYPE"},
		[]string{"all_types.int", "INT_TYPE"},
		[]string{"all_types.bigint", "BIGINT_TYPE"},
		[]string{"all_types.float", "FLOAT_TYPE"},
		[]string{"all_types.double", "DOUBLE_TYPE"},
		[]string{"all_types.string", "STRING_TYPE"},
		[]string{"all_types.timestamp", "TIMESTAMP_TYPE"},
		[]string{"all_types.binary", "BINARY_TYPE"},
		[]string{"all_types.array", "ARRAY_TYPE"},
		[]string{"all_types.map", "MAP_TYPE"},
		[]string{"all_types.struct", "STRUCT_TYPE"},
		[]string{"all_types.union", "UNION_TYPE"},
		[]string{"all_types.decimal", "DECIMAL_TYPE"},
	}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}

	closeAll(t, connection, cursor)
}

func TestTypes(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)
	var b bool
	var tinyInt int8
	var smallInt int16
	var normalInt int32
	var bigInt int64
	// This value is store as a float32. The go thrift API returns a floa64 though.
	var floatType float64
	var double float64
	var s string
	var timeStamp string
	var binary []byte
	var array string
	var mapType string
	var structType string
	var union string
	var decimal string

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	closeAll(t, connection, cursor)
}

func TestTypesInterface(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	i := make([]interface{}, 15)
	expected := make([]interface{}, 15)
	expected[0] = true
	expected[1] = int8(127)
	expected[2] = int16(32767)
	expected[3] = int32(2147483647)
	expected[4] = int64(9223372036854775807)
	expected[5] = float64(0.5)
	expected[6] = float64(0.25)
	expected[7] = "a string"
	expected[8] = "1970-01-01 00:00:00"
	expected[9] = []uint8{49, 50, 51}
	expected[10] = "[1,2]"
	expected[11] = "{1:2,3:4}"
	expected[12] = "{\"a\":1,\"b\":2}"
	expected[13] = "{0:1}"
	expected[14] = "0.1"

	cursor.FetchOne(context.Background(), i...)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if !reflect.DeepEqual(i, expected) {
		t.Fatalf("Expected array: %+v, got: %+v", expected, i)
	}

	closeAll(t, connection, cursor)
}

func TestTypesWithPointer(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)
	var b bool
	var tinyInt *int8 = new(int8)
	var smallInt *int16 = new(int16)
	var normalInt *int32 = new(int32)
	var bigInt *int64 = new(int64)
	var floatType *float64 = new(float64)
	var double *float64 = new(float64)
	var s *string = new(string)
	var timeStamp *string = new(string)
	var binary []byte
	var array *string = new(string)
	var mapType *string = new(string)
	var structType *string = new(string)
	var union *string = new(string)
	var decimal *string = new(string)

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if *tinyInt != 127 || *smallInt != 32767 || *bigInt != 9223372036854775807 || binary == nil || *array != "[1,2]" || *s != "a string" {
		t.Fatalf("Unexpected value, tinyInt: %d, smallInt: %d, bigInt: %d, binary: %x, array: %s, s: %s", *tinyInt, *smallInt, *bigInt, binary, *array, *s)
	}

	closeAll(t, connection, cursor)
}

func TestTypesWithNulls(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTableWithNull(t, cursor)
	var b bool
	var tinyInt *int8 = new(int8)
	var smallInt *int16 = new(int16)
	var normalInt *int32 = new(int32)
	var bigInt *int64 = new(int64)
	// This value is store as a float32. The go thrift API returns a floa64 though.
	var floatType *float64 = new(float64)
	var double *float64 = new(float64)
	var s *string = new(string)
	var timeStamp string
	var binary []byte
	var array string
	var mapType string
	var structType string
	var decimal string

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	cursor.FetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &decimal)
	if cursor.Err != nil {
		t.Fatal(cursor.Err)
	}

	if tinyInt != nil || smallInt != nil || bigInt != nil || binary != nil || array != "" || s != nil {
		t.Fatalf("Unexpected value, tinyInt: %p, smallInt: %p, bigInt: %p, binary: %x, array: %s, s: %s", tinyInt, smallInt, bigInt, binary, array, *s)
	}

	closeAll(t, connection, cursor)
}

func TestParseZookeeperHiveServer2Info(t *testing.T) {
	children := []string{
		"serverUri=x1.test.io:10000;version=2.3.2;sequence=0000000792",
		"",
		"serverUri=x2.test.io:10001;version=2.3.2;sequence=0000000794",
		"serverUri=x3.test.io:10006;version=2.3.2;sequence=0000000791",
		"serverUri=invalid.test.io;version=2.3.2;sequence=0000000791",
		"invalid=invalid",
	}
	expected := []map[string]string{
		map[string]string{"host": "x1.test.io", "port": "10000", "version": "2.3.2", "sequence": "0000000792"},
		map[string]string{"host": "x2.test.io", "port": "10001", "version": "2.3.2", "sequence": "0000000794"},
		map[string]string{"host": "x3.test.io", "port": "10006", "version": "2.3.2", "sequence": "0000000791"},
	}
	result := parseHiveServer2Info(children)
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("Expected : %+v, got: %+v", expected, result)
	}
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
