package gohive

import (
	"log"
	"testing"
	"fmt"
	"time"
	"os"
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
	errExecute := cursor.Execute("CREATE TABLE pokes4 (foo INT, bar INT)")
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
}

func TestSelectGSSAPI(t *testing.T) {
	os.Setenv("KRB5CCNAME", "/tmp/krb5cc_502")
	configuration := map[string]string{
		"service": "hive",
		"realm": "EXAMPLE.COM",
	}
	cursor := makeConnection(t, configuration)
	cursor.Execute("DROP TABLE IF EXISTS pokes")
	errExecute := cursor.Execute("CREATE TABLE pokes (a INT, b STRING)")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	cursor.Execute("INSERT INTO pokes VALUES(1, '1')")
	errExecute = cursor.Execute("INSERT INTO pokes VALUES(2, '2')")
	if errExecute != nil {
		t.Fatal(errExecute)
	}

	var i int32
	var s string
	var j int
	var z int

	for z, j = 0, 0; z < 10; z, j, i, s = z + 1, 0, 0, "-1" {
		fmt.Println("=====================================")
		errExecute = cursor.Execute("SELECT * FROM pokes")
		if errExecute != nil {
			t.Fatal(errExecute)
		}

		for ; cursor.HasMore(); {
			_, errExecute = cursor.FetchOne(&i, &s)
			if errExecute != nil {
				t.Fatal(errExecute)
			}
			fmt.Println("Printing rows")
			fmt.Println(i, s, j)
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
