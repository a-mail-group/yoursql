# generaldb

The `generaldb` subproject is a MySQL to NoSQL Gateway.
The Goal was to implement (almost) full SQL query support to Apache Cassandra
and other NoSQL databases. So users have the best of both world:
Massive fault-tolerance and SQL-functionality.

## Currently implemented:

- Features
	- Select queries, including joins.
- Backends
	- Apache Cassandra 3.0 or later.

## special notes to Insert, Update and Delete statements

Unlike select statements, update, insert and delete statements are usually
pushed to the backend with only little transformation (like SQL dialect conversions).

In order to involve the query engine in update and/or delete statements, a special annotation
can be added to the where clause.

```sql
DELETE FROM mytable WHERE gdb.via(id) AND <<your-where-clause>>;
```

The `gdb.via` - annotation has multiple aliases:
- `gdb.via`
- `gdb.ident`
- `gdb.rowident`
- `gdb.row_ident`

## Backends

- [cassdb](cassdb) Cassanra 3.0+ binding using [gocql](https://github.com/gocql/gocql).



