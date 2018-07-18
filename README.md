# yoursql
Sorry, We don't have MySQL available, but we have ...

... other databases.

The software serves the MySQL wire protocol using the excelent [go-vitess](https://github.com/src-d/go-vitess/) framework (well, vitess is a product and not a library, but who cares) and [go-mysql-server](https://github.com/src-d/go-mysql-server).

# Subprojects:

## my2any

The [my2any](my2any) subproject is a MySQL to *Any* Gateway.
The Goal is to allow (almost) any MySQL client Software (like PHP application)
to use PostgreSQL (or other RDBMSes), without intrusive modifications, such as
switching from the `mysql` or `mysqli` PHP-library to the PostgreSQL client library.

### Currently implemented:

- PostgreSQL (converts MySQL's SQL-dialect to PostgreSQL's)

## generaldb

The [generaldb](generaldb) subproject is a MySQL to NoSQL Gateway.
The Goal was to implement (almost) full SQL query support to Apache Cassandra
and other NoSQL databases. So users have the best of both world: Massive Fault
tolerance and SQL-datamodel and Apis.

### Currently implemented:

- Features
	- Select queries, including joins.
- Backends
	- Apache Cassandra 3.0 or later.

# Future Ideas

- Implementing adapters for other databases, that have a golang-[SQLDriver](https://github.com/golang/go/wiki/SQLDrivers), especially...
	- ~~[N1QL](https://github.com/couchbase/go_n1ql), however, it differs strongly from the well known SQL behavoir.~~ No!
	- [MonetDB](https://github.com/fajran/go-monetdb). Because it is a really impressive RDBMS.
- Implementing adapters for NoSQL databases, such as...
	- ~~Cassandra using [gocql](https://github.com/gocql/gocql), including a query rewriter to allow joins and sub-queries.~~ Done!
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
