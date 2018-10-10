# GoHive
[![Build Status](https://travis-ci.org/beltran/gohive.svg?branch=master)](https://travis-ci.org/beltran/gohive) [![Coverage Status](https://coveralls.io/repos/github/beltran/gohive/badge.svg?branch=master)](https://coveralls.io/github/beltran/gohive?branch=master)


GoHive is a driver for Hive in go that supports mechanisms KERBEROS(Gssapi Sasl), NONE(Plain Sasl), LDAP, CUSTOM and NOSASL, both for binary and http transport, with and without SSL. The kerberos mechanism will pick a different authentication level depending on `hive.server2.thrift.sasl.qop`.

## Installation
```
go get github.com/beltran/gohive
```

## Quickstart

```go
    connection, errConn := gohive.Connect("hs2.example.com", 10000, "KERBEROS", configuration)
    if errConn != nil {
        log.Fatal(errConn)
    }
    cursor := connection.Cursor()
    
    cursor.Execute(ctx, "INSERT INTO myTable VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')", async)
    if cursor.Err != nil {
        log.Fatal(cursor.Err)
    }

    cursor.Execute(ctx, "SELECT * FROM myTable", async)
    if cursor.Err != nil {
        log.Fatal(cursor.Err)
    }

    var i int32
    var s string
    for cursor.HasMore(ctx) {
        cursor.FetchOne(ctx, &i, &s)
        if cursor.Err != nil {
            log.Fatal(cursor.Err)
        }
        log.Println(i, s)
    }

    cursor.Close()
    connection.Close()
```

`cursor.HasMore` may query hive for more rows if not all of them have been received. Once the row is
read is discarded from memory so as long as the fetch size is not too big there's no limit to how much
data can be queried.

## Supported connections
### Connect with Sasl kerberos:
``` go
configuration := NewConnectConfiguration()
configuration.Service = "hive"
// Previously kinit should have done: kinit -kt ./secret.keytab hive/hs2.example.com@EXAMPLE.COM
connection, errConn := Connect(ctx, "hs2.example.com", 10000, "KERBEROS", configuration)
```
This implies setting in hive-site.xml:
- `hive.server2.authentication = KERBEROS`
- `hive.server2.authentication.kerberos.principal = hive/_HOST@EXAMPLE.COM`
- `hive.server2.authentication.kerberos.keytab = path/to/keytab.keytab`

### Connnect using Plain Sasl:
``` go
configuration := NewConnectConfiguration()
// If it's not set it will be picked up from the logged user
configuration.Username = "myUsername"
// This may not be necessary
configuration.Username = "myPassword"
connection, errConn := Connect(ctx, "hs2.example.com", 10000, "NONE", configuration)
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = NONE`

### Connnect using No Sasl:
``` go
connection, errConn := Connect(ctx, "hs2.example.com", 10000, "NOSASL", nil)
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = NOSASL`

### Connect using Http transport mode
Binary transport mode is supported for this three options(PLAIN, KERBEROS and NOSASL). Http transport is supported for PLAIN and KERBEROS:
``` go
configuration := NewConnectConfiguration()
configuration.HttpPath = "cliservice" // this is the default path although in hive
configuration.TransportMode = "http"
configuration.Service = "hive"

connection, errConn := Connect(ctx, "hs2.example.com", 10000, "KERBEROS", configuration)
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = KERBEROS`, or NONE
- `hive.server2.transport.mode = http`
- `hive.server2.thrift.http.port = 10001`

## Running tests
Tests can be run with:
```
./scripts/integration
```
This uses [dhive](https://github.com/beltran/dhive) and it will start two docker instances with hive and kerberos. `kinit`, `klist`, `kdestroy` have to be installed locally. `hs2.example.com` will have to be an alias for 127.0.0.1 in `/etc/hosts`.
