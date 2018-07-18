# yoursql
Sorry, We don't have MySQL available, but we have ...

... PostgreSQL, or MS SQL Server, or InterBase, or Firebird, or MonetDB.

## What this project is about.

A MySQL to PostgreSQL/*Insert-Any-Other-RDBMS* Gateway.
The Goal is to allow (almost) any MySQL client Software (like PHP application)
to use PostgreSQL (or other RDBMSes), without intrusive modifications, such as
switching from the `mysql` or `mysqli` PHP-library to the PostgreSQL client library.

## What this software does.

The software serves the MySQL wire protocol using the excelent [go-vitess](https://github.com/src-d/go-vitess/) framework (well, vitess is a product and not a library, but who cares) and [go-mysql-server](https://github.com/src-d/go-mysql-server).

The SQL statements are parsed with the [vitess](https://github.com/src-d/go-vitess/)-parser and then translated from the MySQL dialect into the PostgreSQL dialect (other dialects can be implemented as plugins).

## Future Ideas

- Implementing adapters for other databases, that have a golang-[SQLDriver](https://github.com/golang/go/wiki/SQLDrivers), especially...
	- [N1QL](https://github.com/couchbase/go_n1ql), however, it differs strongly from the well known SQL behavoir.
	- [MonetDB](https://github.com/fajran/go-monetdb). Because it is a really impressive RDBMS.
- Implementing adapters for NoSQL databases, such as...
	- Cassandra using [gocql](https://github.com/gocql/gocql), including a query rewriter to allow joins and sub-queries.
	- [N1QL](https://github.com/couchbase/go_n1ql), and to emulate the SQL-Behavoir on it.
	- MongoDB. This will require to do some hard work, translating SQL to MongoDB-queries.

## Example-Server

```go
package main

import "fmt"

import (
	"database/sql"

	_ "github.com/lib/pq"
)

import "github.com/a-mail-group/yoursql/my2any"
import "github.com/a-mail-group/yoursql/my2any/my2pg"
import "gopkg.in/src-d/go-vitess.v0/mysql"

func main(){
	connStr := "user=moos password=moos dbname=mysql1 sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err!=nil {
		fmt.Println("err",err)
		return
	}
	
	auth := mysql.NewAuthServerStatic()
	auth.Entries["user"] = []*mysql.AuthServerStaticEntry{{
		Password: "pass",
	}}
	gw := &my2any.Gateway{
		db,
		my2pg.PqConverter{my2any.DefaultConverter},
		my2pg.PgSyntaxer{my2any.DefaultSyntaxer},
		my2pg.PgSpecialFeatures{my2any.DefaultSpecialFeatures},
	}
	
	lst,err := mysql.NewListener("tcp", "localhost:3306", auth, gw)
	if err!=nil {
		fmt.Println("err",err)
		return
	}
	fmt.Println("ACCEPT")
	lst.Accept()
	
	fmt.Println("Hello World")
}
```
