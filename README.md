# GoHive

GoHive is a driver for Hive in go that supports mechanisms `KERBEROS`(GSSAPI SASL), `NONE`(PLAIN SASL) and `NOSASL`.

## Usage

```go
    configuration := NewConnectConfiguration()
	configuration.Service = "hive"
	connection, errConn := Connect("hs2.example.com", 10000, "KERBEROS", configuration)
	if errConn != nil {
		log.Fatal(errConn)
	}
    cursor := connection.Cursor()
    
    err := cursor.Execute("CREATE TABLE myTable (a INT, b STRING)")
	if err != nil {
		log.Fatal(err)
	}

	err = cursor.Execute("INSERT INTO myTable VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')")
	if err != nil {
		log.Fatal(err)
    }

    err = cursor.Execute("SELECT * FROM myTable")
	if errExecute != nil {
		log.Fatal(err)
    }
    
    var i int32
	var s string
    for ; cursor.HasMore(); {
		_, errExecute = cursor.FetchOne(&i, &s)
		if errExecute != nil {
			log.Fatal(err)
		}
		fmt.Println(i, s)
    }
    
    cursor.Close()
    connection.Close()
```

# Tests
Tests can be run with 
```
./scripts/integration
```
This uses [dhive](https://github.com/beltran/dhive) and it will start two docker instances with hive and kerberos and test against that. `kinit`, `klist`, `kdestroy` have to be installed locally. `hs2.example.com` will have to be an alias for 127.0.0.1 in `/etc/hosts`
