package gohive

import (
	"context"
	"fmt"
	"hiveserver"
	"log"
	"os"
	"testing"
	"time"
)

func TestConnectDefault(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("TRANSPORT")
	if auth != "KERBEROS" || transport != "binary" {
		return
	}

	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	connection, err := Connect(context.Background(), "hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close(context.Background())
}

func TestFetchDatabase(t *testing.T) {
	async := false
	connection, cursor := makeConnection(t, 1000)
	errExecute := cursor.Execute(context.Background(), "SHOW DATABASES", async)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var s string
	_, errCursor := cursor.FetchOne(context.Background(), &s)
	if errCursor != nil {
		t.Fatal(errCursor)
	}
	if s != "default" {
		t.Fatalf("Unrecognized dabase found: %s", s)
	}
	closeAll(t, connection, cursor)
}

func TestCreateTable(t *testing.T) {
	async := false
	connection, cursor := makeConnection(t, 1000)
	errExecute := cursor.Execute(context.Background(), "DROP TABLE IF EXISTS pokes6", async)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	errExecute = cursor.Execute(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)", async)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	// Now it should fail because the table already exists
	errExecute = cursor.Execute(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)", async)
	if errExecute == nil {
		t.Fatal(errExecute)
	}
	closeAll(t, connection, cursor)
}

func TestSelect(t *testing.T) {
	async := false
	connection, cursor := prepareTable(t, 2, 1000)

	var i int32
	var s string
	var j int
	var z int
	var errExecute error

	for z, j = 0, 0; z < 10; z, j, i, s = z+1, 0, 0, "-1" {
		errExecute = cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
		if errExecute != nil {
			t.Fatal(errExecute)
		}

		for cursor.HasMore(context.Background()) {
			_, errExecute = cursor.FetchOne(context.Background(), &i, &s)
			if errExecute != nil {
				t.Fatal(errExecute)
			}
			j++
		}
		if i != 2 || s != "2" {
			log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
		}
		if cursor.HasMore(context.Background()) {
			log.Fatal("Shouldn't have any more values")
		}
		if j != 2 {
			t.Fatal("Two rows expected here")
		}
	}
	closeAll(t, connection, cursor)
}

func TestSmallFetchSize(t *testing.T) {
	async := false
	connection, cursor := prepareTable(t, 4, 2)

	var i int32
	var s string
	var j int

	errExecute := cursor.Execute(context.Background(), "SELECT * FROM pokes", async)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	// Fetch all rows
	// The query happens behind the scenes
	// The other rows are discarted
	for j = 0; cursor.HasMore(context.Background()); {
		_, errExecute = cursor.FetchOne(context.Background(), &i, &s)
		if errExecute != nil {
			t.Fatal(errExecute)
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

func TestWithContext(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	errExecute := cursor.Execute(ctx, "INSERT INTO pokes VALUES(1, '1')", false)
	if errExecute == nil {
		t.Fatal("Context should have been done")
	}
	closeAll(t, connection, cursor)
}

func TestAsync(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	start := time.Now()
	errExecute := cursor.Execute(context.Background(), "INSERT INTO pokes VALUES(1, '1')", true)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	stop := time.Now()
	elapsed := stop.Sub(start)
	if elapsed > time.Duration(time.Second*5) {
		t.Fatal("It shouldn't have taken more than 5 seconds to run the query in async mode")
	}

	status, errStatus := cursor.Poll(context.Background())
	if errStatus != nil {
		t.Fatal(errStatus)
	}
	for *status == hiveserver.TOperationState_INITIALIZED_STATE || *status == hiveserver.TOperationState_RUNNING_STATE {
		status, errStatus = cursor.Poll(context.Background())
		if errStatus != nil {
			t.Fatal(errStatus)
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}

	errExecute = cursor.Execute(context.Background(), "SELECT * FROM pokes", false)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var i int32
	var s string
	_, errExecute = cursor.FetchOne(context.Background(), &i, &s)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("All rows should have been read")
	}

	if i != 1 || s != "1" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}

	closeAll(t, connection, cursor)
}

func TestCancel(t *testing.T) {
	connection, cursor := prepareTable(t, 0, 1000)
	start := time.Now()
	errExecute := cursor.Execute(context.Background(), "INSERT INTO pokes VALUES(1, '1')", true)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	stop := time.Now()
	elapsed := stop.Sub(start)
	if elapsed > time.Duration(time.Second*5) {
		t.Fatal("It shouldn't have taken more than 5 seconds to run the query in async mode")
	}
	errCancel := cursor.Cancel(context.Background())
	if errExecute != nil {
		t.Fatal(errCancel)
	}

	status, errStatus := cursor.Poll(context.Background())
	if errStatus != nil {
		t.Fatal(errStatus)
	}
	for *status == hiveserver.TOperationState_INITIALIZED_STATE || *status == hiveserver.TOperationState_RUNNING_STATE {
		status, errStatus = cursor.Poll(context.Background())
		if errStatus != nil {
			t.Fatal(errStatus)
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}

	errExecute = cursor.Execute(context.Background(), "SELECT count(*) FROM pokes", false)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var i int64 = 10
	_, errExecute = cursor.FetchOne(context.Background(), &i)
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	if cursor.HasMore(context.Background()) {
		t.Fatal("All rows should have been read")
	}

	if i != 0 {
		t.Fatal("Table should have zero rows")
	}

	closeAll(t, connection, cursor)
}

func prepareTable(t *testing.T, rowsToInsert int, fetchSize int64) (*Connection, *Cursor) {
	connection, cursor := makeConnection(t, fetchSize)
	cursor.Execute(context.Background(), "DROP TABLE IF EXISTS pokes", false)
	errExecute := cursor.Execute(context.Background(), "CREATE TABLE pokes (a INT, b STRING)", false)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if rowsToInsert > 0 {
		values := ""
		for i := 1; i <= rowsToInsert; i++ {
			values += fmt.Sprintf("(%d, '%d')", i, i)
			if i != rowsToInsert {
				values += ","
			}
		}
		errExecute = cursor.Execute(context.Background(), "INSERT INTO pokes VALUES "+values, false)
		if errExecute != nil {
			t.Fatal(errExecute)
		}
	}
	return connection, cursor
}

func makeConnection(t *testing.T, fetchSize int64) (*Connection, *Cursor) {
	mode := getTransport()
	configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	configuration.FetchSize = fetchSize
	configuration.TransportMode = mode
	var port int = 10000
	if mode == "http" {
		port = 10001
		configuration.HttpPath = "cliservice"
	}
	connection, errConn := Connect(context.Background(), "hs2.example.com", port, getAuth(), configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	return connection, cursor
}

func closeAll(t *testing.T, connection *Connection, cursor *Cursor) {
	err := cursor.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = connection.Close(context.Background())
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
