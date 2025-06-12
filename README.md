# GoHive
[![Build Status](https://api.travis-ci.com/beltran/gohive.svg?branch=master)](https://app.travis-ci.com/beltran/gohive) [![Coverage Status](https://coveralls.io/repos/github/beltran/gohive/badge.svg?branch=master)](https://coveralls.io/github/beltran/gohive?branch=master)


GoHive is a driver for Hive and the [Spark Distributed SQL Engine](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html) in go that supports connection mechanisms KERBEROS(Gssapi Sasl), NONE(Plain Sasl), LDAP, CUSTOM and NOSASL, both for binary and HTTP transport, with and without SSL, compliant with the `database/sql` interface. The kerberos mechanism will pick a different authentication level depending on `hive.server2.thrift.sasl.qop`.

GoHive also offers support to query the Hive metastore with various authentication mechanisms, including KERBEROS.

## Installation
GoHive can be installed with:
```
go get github.com/beltran/gohive
```

To add kerberos support GoHive requires header files to build against the GSSAPI C library. They can be installed with:
- Ubuntu: `sudo apt-get install libkrb5-dev`
- MacOS: `brew install homebrew/dupes/heimdal --without-x11`
- Debian: `yum install -y krb5-devel`

Then:
```
go get -tags kerberos github.com/beltran/gohive
```

## Quickstart

### Connection to Hive

GoHive supports the standard `database/sql` interface:

```go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

// Open a connection
// Format: hive://username:password@host:port/database
db, err := sql.Open("hive", "hive://username:password@hs2.example.com:10000/default")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Execute a query
rows, err := db.Query("SELECT * FROM my_table LIMIT 10")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Process results
for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("id: %d, name: %s\n", id, name)
}
```

## Supported connections
### Connect with SASL KERBEROS:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

db, err := sql.Open("hive", "hive://hs2.example.com:10000/default?auth=KERBEROS&service=hive")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:
- `hive.server2.authentication = KERBEROS`
- `hive.server2.authentication.kerberos.principal = hive/_HOST@EXAMPLE.COM`
- `hive.server2.authentication.kerberos.keytab = path/to/keytab.keytab`

### Connect using Plain SASL:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

db, err := sql.Open("hive", "hive://myUsername:myPassword@hs2.example.com:10000/default?auth=NONE")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = NONE`

### Connect using NOSASL:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

db, err := sql.Open("hive", "hive://@hs2.example.com:10000/default?auth=NOSASL")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = NOSASL`

### Connect using HTTP transport mode
Binary transport mode is supported for auth mechanisms PLAIN, KERBEROS and NOSASL. HTTP transport mode is supported for PLAIN and KERBEROS:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

db, err := sql.Open("hive", "hive://@hs2.example.com:10000/default?auth=NOSASL&service=hive&transport=http&httpPath=cliservice")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = KERBEROS`, or `NONE`
- `hive.server2.transport.mode = http`
- `hive.server2.thrift.http.port = 10001`

## Connection to the Hive Metastore

The thrift client is directly exposed, so the API exposed by the Hive metastore can be called directly.

```go
    configuration := gohive.NewMetastoreConnectConfiguration()
    connection, err := gohive.ConnectToMetastore("hm.example.com", 9083, "KERBEROS", configuration)
    if err != nil {
        log.Fatal(err)
    }
    database := hive_metastore.Database{
        Name:        "my_new_database",
        LocationUri: "/"}
    err = connection.Client.CreateDatabase(context.Background(), &database)
    if err != nil {
        log.Fatal(err)
    }
    databases, err := connection.Client.GetAllDatabases(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    log.Println("databases ", databases)
    connection.Close()
```

## Running tests
Tests can be run with:
```
./scripts/integration
```
This uses [dhive](https://github.com/beltran/dhive) and it will start three docker instances with Hive, the Hive metastore, and Kerberos. `kinit`, `klist`, `kdestroy` have to be installed locally. `hs2.example.com` and `hm.example.com` will have to be an alias for 127.0.0.1 in `/etc/hosts`. The krb5 configuration file should be created with `bash scripts/create_krbconf.sh`. Overall the [steps used in the travis CI](https://github.com/beltran/gohive/blob/ec69b5601829296a56ca0558693ed30c11180a94/.travis.yml#L24-L46) can be followed.
