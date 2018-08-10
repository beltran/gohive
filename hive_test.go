package gohive

import (
	"testing"
	"fmt"
	"time"
	//"os"
)

func TestConnectNoSasl(t *testing.T) {
	Connect("127.0.0.1", 10000, "NOSASL", nil)
}

func TestConnectSasl(t *testing.T) {
	Connect("127.0.0.1", 10000, "NONE", nil)
}
func TestFetchDatabasesPlain(t *testing.T) {
	connection, errConn := Connect("127.0.0.1", 10000, "NONE", nil)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	errExecute := cursor.Execute("SHOW DATABASES")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	row, errCursor := cursor.FetchOne()
	fmt.Println(row)
	if errCursor != nil {
		t.Fatal(errCursor)
	}
}

func TestWriteAndFetchData(t *testing.T) {
	connection, errConn := Connect("127.0.0.1", 10000, "NONE", nil)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	errExecute := cursor.Execute("CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING);")
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	
	time.Sleep(2 * time.Second)

	row, errCursor := cursor.FetchOne()
	fmt.Println(row)
	if errCursor != nil {
		t.Fatal(errCursor)
	}
}


func TestFetchDatabasesPlainGSSAPI(t *testing.T) {
	configuration := map[string]string{
		"service": "hive",
		"realm": "EXAMPLE.COM",
	}
	
	cursor := makeConnection(t, configuration)
	errExecute := cursor.Execute("SHOW DATABASES")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	row, errCursor := cursor.FetchOne()
	fmt.Println(row)
	if errCursor != nil {
		t.Fatal(errCursor)
	}
}

func TestCreateTablePlainGSSAPI(t *testing.T) {
	configuration := map[string]string{
		"service": "hive",
		"realm": "EXAMPLE.COM",
	}
	
	cursor := makeConnection(t, configuration)
	errExecute := cursor.Execute("CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING)")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	row, errCursor := cursor.FetchOne()
	fmt.Println(row)
	if errCursor != nil {
		t.Fatal(errCursor)
	}
}

func TestGSSAPIConnect(t *testing.T) {
	//os.Setenv("KRB5CCNAME", )
	configuration := map[string]string{
		"service": "hive",
	}
	connection, errConn := Connect("hs2.example.com", 10000, "KERBEROS", configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	fmt.Println(connection)
}

func makeConnection(t *testing.T, configuration map[string]string) *Cursor {
	connection, errConn := Connect("hs2.example.com", 10000, "KERBEROS", configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.Cursor()
	return cursor
}
