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

## Backends

- (cassdb)[cassdb] Cassanra 3.0+ binding using [gocql](https://github.com/gocql/gocql).



