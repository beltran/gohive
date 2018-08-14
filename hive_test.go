package gohive

import (
	"context"
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
	connection, err := Connect("hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
}

func TestFetchDatabase(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	errExecute := cursor.Execute("SHOW DATABASES")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var s string
	_, errCursor := cursor.FetchOne(&s)
	if errCursor != nil {
		t.Fatal(errCursor)
	}
	if s != "default" {
		t.Fatalf("Unrecognized dabase found: %s", s)
	}
	closeAll(t, connection, cursor)
}

func TestCreateTable(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	errExecute := cursor.Execute("DROP TABLE IF EXISTS pokes6")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	errExecute = cursor.Execute("CREATE TABLE pokes6 (foo INT, bar INT)")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	// Now it should fail because the table already exists
	errExecute = cursor.Execute("CREATE TABLE pokes6 (foo INT, bar INT)")
	if errExecute == nil {
		t.Fatal(errExecute)
	}
	closeAll(t, connection, cursor)
}

func TestSelect(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute("DROP TABLE IF EXISTS pokes")
	errExecute := cursor.Execute("CREATE TABLE pokes (a INT, b STRING)")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	errExecute = cursor.Execute("INSERT INTO pokes VALUES(1, '1'), (2, '2')")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var i int32
	var s string
	var j int
	var z int

	for z, j = 0, 0; z < 10; z, j, i, s = z+1, 0, 0, "-1" {
		errExecute = cursor.Execute("SELECT * FROM pokes")
		if errExecute != nil {
			t.Fatal(errExecute)
		}

		for cursor.HasMore() {
			_, errExecute = cursor.FetchOne(&i, &s)
			if errExecute != nil {
				t.Fatal(errExecute)
			}
			j++
		}
		if i != 2 || s != "2" {
			log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
		}
		if cursor.HasMore() {
			log.Fatal("Shouldn't have any more values")
		}
		if j != 2 {
			t.Fatal("Two rows expected here")
		}
	}
	closeAll(t, connection, cursor)
}

func TestSmallFetchSize(t *testing.T) {
	connection, cursor := makeConnection(t, 2)
	cursor.Execute("DROP TABLE IF EXISTS pokes")
	errExecute := cursor.Execute("CREATE TABLE pokes (a INT, b STRING)")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	errExecute = cursor.Execute("INSERT INTO pokes VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var i int32
	var s string
	var j int

	errExecute = cursor.Execute("SELECT * FROM pokes")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	// Fetch first two rows
	for j = 0; cursor.HasMore(); {
		_, errExecute = cursor.FetchOne(&i, &s)
		if errExecute != nil {
			t.Fatal(errExecute)
		}
		j++
	}
	if i != 2 || s != "2" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.HasMore() {
		log.Fatal("Shouldn't have any more values")
	}
	if j != 2 {
		t.Fatal("Fetch size was set to 2")
	}

	// Fext next two rows
	errExecute = cursor.Execute("SELECT * FROM pokes")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	for j = 0; cursor.HasMore(); {
		_, errExecute = cursor.FetchOne(&i, &s)
		if errExecute != nil {
			t.Fatal(errExecute)
		}
		j++
	}
	if i != 2 || s != "2" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.HasMore() {
		log.Fatal("Shouldn't have any more values")
	}
	if j != 2 {
		t.Fatal("Fetch size was set to 2")
	}
	_, errExecute = cursor.FetchOne(&i, &s)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if cursor.HasMore() {
		t.Fatal("No more rows should be left")
	}
	closeAll(t, connection, cursor)
}

func TestWithContext(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.Execute("DROP TABLE IF EXISTS pokes")
	errExecute := cursor.Execute("CREATE TABLE pokes (a INT, b STRING)")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	errExecute = cursor.ExecuteWithContext(ctx, "")
	if errExecute == nil {
		t.Fatal("Context should have been done")
	}
	closeAll(t, connection, cursor)
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
	connection, errConn := Connect("hs2.example.com", port, getAuth(), configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	return connection, cursor
}

func closeAll(t *testing.T, connection *Connection, cursor *Cursor) {
	err := cursor.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = connection.Close()
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
