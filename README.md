# GoHive
[![Build Status](https://api.travis-ci.com/beltran/gohive.svg?branch=master)](https://app.travis-ci.com/beltran/gohive) [![Coverage Status](https://coveralls.io/repos/github/beltran/gohive/badge.svg?branch=master)](https://coveralls.io/github/beltran/gohive?branch=master)


GoHive is a driver for Hive and the [Spark Distributed SQL Engine](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html) in go that supports connection mechanisms KERBEROS(Gssapi Sasl), NONE(Plain Sasl), LDAP, CUSTOM and NOSASL, both for binary and HTTP transport, with and without SSL. The kerberos mechanism will pick a different authentication level depending on `hive.server2.thrift.sasl.qop`.

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

### Using the SQL Interface

GoHive now supports the standard `database/sql` interface, making it easier to use with existing Go code:

```go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

// Open a connection using the SQL interface
// Format: hive://username:password@host:port/database
db, err := sql.Open("hive", "hive://username:password@localhost:10000/default")
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

### Connection to the Hive Metastore

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

## Supported connections
### Connect with Sasl kerberos:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

// Format: hive://username:password@host:port/database?auth=KERBEROS&service=hive
db, err := sql.Open("hive", "hive://@hs2.example.com:10000/default?auth=KERBEROS&service=hive")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:
- `hive.server2.authentication = KERBEROS`
- `hive.server2.authentication.kerberos.principal = hive/_HOST@EXAMPLE.COM`
- `hive.server2.authentication.kerberos.keytab = path/to/keytab.keytab`

### Connect using Plain Sasl:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

// Format: hive://username:password@host:port/database?auth=NONE
db, err := sql.Open("hive", "hive://myUsername:myPassword@hs2.example.com:10000/default?auth=NONE")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = NONE`

### Connect using No Sasl:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

// Format: hive://username:password@host:port/database?auth=NOSASL
db, err := sql.Open("hive", "hive://@hs2.example.com:10000/default?auth=NOSASL")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = NOSASL`

### Connect using Http transport mode
Binary transport mode is supported for auth mechanisms PLAIN, KERBEROS and NOSASL. Http transport mode is supported for PLAIN and KERBEROS:
``` go
import (
    "database/sql"
    _ "github.com/beltran/gohive"
)

// Format: hive://username:password@host:port/database?auth=KERBEROS&service=hive&transport=http&httpPath=cliservice
db, err := sql.Open("hive", "hive://@hs2.example.com:10000/default?auth=KERBEROS&service=hive&transport=http&httpPath=cliservice")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```
This implies setting in hive-site.xml:

- `hive.server2.authentication = KERBEROS`, or `NONE`
- `hive.server2.transport.mode = http`
- `hive.server2.thrift.http.port = 10001`

## Using the Native Interface

```go
    connection, errConn := gohive.Connect("hs2.example.com", 10000, "KERBEROS", configuration)
    if errConn != nil {
        log.Fatal(errConn)
    }
    cursor := connection.Cursor()

    cursor.Exec(ctx, "INSERT INTO myTable VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')")
    if cursor.Err != nil {
        log.Fatal(cursor.Err)
    }

    cursor.Exec(ctx, "SELECT * FROM myTable")
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

`cursor.HasMore` may query Hive for more rows if not all of them have been received. Once the row is
read is discarded from memory so as long as the fetch size is not too big there's no limit to how much
data can be queried.

### Zookeeper
A connection can be made using zookeeper:

```go
connection, errConn := ConnectZookeeper("zk1.example.com:2181,zk2.example.com:2181", "NONE", configuration)
```
The last two parameters determine how the connection to Hive will be made once the Hive hosts are retrieved from zookeeper.

### NULL values
For example if a `NULL` value is in a row, the following operations would put `0` into `i`:
```
var i int32
cursor.FetchOne(context.Background(), &i)
```
To differentiate between these two values (`NULL` and `0`) the following will set `i` to `nil` or `*i` to `0`:
```
var i *int32 = new(int32)
cursor.FetchOne(context.Background(), &i)
```
which will produce the same result as:
```
var i *int32
cursor.FetchOne(context.Background(), &i)
```
Alternatively, using the rowmap API, `m := cursor.RowMap(context.Background())`,
 `m` would be `map[string]interface{}{"table_name.column_name": nil}` for a `NULL` value. It will return a map
where the keys are `table_name.column_name`. This works fine with Hive but using [Spark Thirft SQL server](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html) `table_name` is not present and the keys are `column_name` and it can [lead to problems](https://github.com/beltran/gohive/issues/120) if two tables have the same column name so the `FetchOne` API should be used in this case.

## Running tests
Tests can be run with:
```
./scripts/integration
```
This uses [dhive](https://github.com/beltran/dhive) and it will start three docker instances with Hive, the Hive metastore, and Kerberos. `kinit`, `klist`, `kdestroy` have to be installed locally. `hs2.example.com` and `hm.example.com` will have to be an alias for 127.0.0.1 in `/etc/hosts`. The krb5 configuration file should be created with `bash scripts/create_krbconf.sh`. Overall the [steps used in the travis CI](https://github.com/beltran/gohive/blob/ec69b5601829296a56ca0558693ed30c11180a94/.travis.yml#L24-L46) can be followed.
