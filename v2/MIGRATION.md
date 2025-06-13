# Migration Guide: GoHive 1.x to 2.0

This guide will help you migrate your GoHive applications from version 1.x to 2.0.

## Breaking Changes

### 1. Database/SQL Interface Changes

Version 2.0 introduces full support for the standard Go `database/sql` interface. This means you'll need to update your code to use the new interface.

**Before (1.x):**
```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/beltran/gohive"
)

func main() {
    configuration := gohive.NewConnectConfiguration()
    configuration.Username = "cloudera"
    configuration.Password = "cloudera"
    configuration.Database = "default"
    configuration.FetchSize = 1000
    configuration.TransportMode = "binary"
    configuration.HiveConfiguration = map[string]string{
        "hive.server2.thrift.sasl.qop": "auth-conf",
    }

    connection, errConn := gohive.Connect("hs2.example.com", 10000, "KERBEROS", configuration)
    if errConn != nil {
        log.Fatal(errConn)
    }
    cursor := connection.Cursor()
    defer cursor.Close()
    defer connection.Close()

    cursor.Exec(context.Background(), "SELECT * FROM one_row")
    if cursor.Err != nil {
        log.Fatal(cursor.Err)
    }

    var i int32
    for cursor.HasMore(context.Background()) {
        cursor.FetchOne(context.Background(), &i)
        if cursor.Err != nil {
            log.Fatal(cursor.Err)
        }
        fmt.Println(i)
    }
}
```

**After (2.0):**
```go
package main

import (
    "database/sql"
    "fmt"
    "log"
    "os"

    _ "github.com/beltran/gohive"
)

func main() {
    // Format: hive://username:password@host:port/database?auth=KERBEROS&service=hive
    db, err := sql.Open("hive", "hive://cloudera:cloudera@hs2.example.com:10000/default?auth=KERBEROS&service=hive")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    rows, err := db.Query("SELECT * FROM one_row")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    var i int32
    for rows.Next() {
        err := rows.Scan(&i)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println(i)
    }
}
```

### 2. Unsupported Features

The following features from version 1.x are no longer supported in version 2.0:

- Connection via Zookeeper
- Log Fetching
- Custom dial function
- Setting the connection timeout, socket timeout, http timeout

## Migration Steps

1. **Update Connection Code**
   - Replace `gohive.Connect()` with `sql.Open()`
   - Update connection string format
   - Remove manual cursor management

2. **Update Query Code**
   - Replace `cursor.Exec()` with `db.Query()` or `db.Exec()`
   - Replace `cursor.FetchOne()` with `rows.Scan()`
   - Use `rows.Next()` instead of `cursor.HasMore()`

## Need Help?

If you encounter any issues during migration:
1. Check the [GitHub Issues](https://github.com/beltran/gohive/issues)
2. Review the [documentation](https://github.com/beltran/gohive/v2/readme.md)
3. Open a new issue if you find a bug

## Why These Changes?

These changes were made to:
- Provide full compatibility with Go's standard database interface
- Enable better integration with existing Go database code
- Improve performance through connection pooling
- Support multiple cursors per connection
- Make the code more maintainable and familiar to Go developers
