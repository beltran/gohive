// +build all integration

package gohive

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"reflect"
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

func TestSelect(t *testing.T) {
	async := false
	connection, cursor := prepareTable(t, 6000, 1000)

	var i int32
	var s string
	var j int32
	var z int

	for z, j = 0, 0; z < 10; z, j, i, s = z+1, 0, 0, "-1" {
		cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
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

func TestSimpleSelect(t *testing.T) {
	connection, cursor := prepareTable(t, 1, 1000)
	cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	var s string
	var i int32
	cursor.FetchOne(context.Background(), &i, &s)

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

func TestRowMapAllTypes(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	cursor.Execute(context.Background(), "SELECT * FROM all_types", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{
		"all_types.smallint": int16(32767),
		"all_types.int": int32(2147483647),
		"all_types.float": float64(0.5),
		"all_types.double": float64(0.25),
		"all_types.string": "a string",
		"all_types.boolean": true,
		"all_types.struct": "{\"a\":1,\"b\":2}",
		"all_types.bigint": int64(9223372036854775807),
		"all_types.array": "[1,2]",
		"all_types.map": "{1:2,3:4}",
		"all_types.decimal": "0.1",
		"all_types.binary": []uint8{49, 50, 51},
		"all_types.timestamp": "1970-01-01 00:00:00",
		"all_types.union": "{0:1}",
		"all_types.tinyint": int8(127),
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

func TestNullTypes(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)
	insertNullValuesAllTypes(t, cursor)

	cursor.Execute(context.Background(), "SELECT * FROM all_types LIMIT 1", false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}

	m := cursor.RowMap(context.Background())
	expected := map[string]interface{}{
		"all_types.smallint": int16(32767),
		"all_types.int": int32(2147483647),
		"all_types.float": float64(0.5),
		"all_types.double": float64(0.25),
		"all_types.string": "",
		"all_types.boolean": true,
		"all_types.struct": nil,
		"all_types.bigint": int64(9223372036854775807),
		"all_types.array": "[1,2]",
		"all_types.map": "{1:2,3:4}",
		"all_types.decimal": "0.1",
		"all_types.binary": []uint8{49, 50, 51},
		"all_types.timestamp": nil,
		"all_types.union": nil,
		"all_types.tinyint": int8(127),
	}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	closeAll(t, connection, cursor)
}

func prepareAllTypesTable(t *testing.T, cursor *Cursor) {
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

func insertNullValuesAllTypes(t *testing.T, cursor *Cursor) {
	insertAll := `INSERT INTO TABLE all_types VALUES(
		true,
		127,
		32767,
		2147483647,
		9223372036854775807,
		0.5,
		0.25,
		NULL,
		0,
		'123',
		array(1, 2),
		map(1, 2, 3, 4),
		NULL,
		NULL,
		0.1)`
	cursor.Execute(context.Background(), insertAll, false)
	if cursor.Error() != nil {
		t.Fatal(cursor.Error())
	}
}

func prepareTable(t *testing.T, rowsToInsert int, fetchSize int64) (*Connection, *Cursor) {
	connection, cursor := makeConnection(t, fetchSize)
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
	return connection, cursor
}

func makeConnection(t *testing.T, fetchSize int64) (*Connection, *Cursor) {
	mode := getTransport()
	ssl := getSsl()
	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	configuration.FetchSize = fetchSize
	configuration.TransportMode = mode

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
