# my2any

A MySQL to PostgreSQL/*Insert-Any-Other-RDBMS* Gateway.
The Goal is to allow (almost) any MySQL client Software (like PHP application)
to use PostgreSQL (or other RDBMSes), without intrusive modifications, such as
switching from the `mysql` or `mysqli` PHP-library to the PostgreSQL client library.

## What this software does.

The software serves the MySQL wire protocol using the excelent [go-vitess](https://github.com/src-d/go-vitess/) framework (well, vitess is a product and not a library, but who cares) and [go-mysql-server](https://github.com/src-d/go-mysql-server).

The SQL statements are parsed with the [vitess](https://github.com/src-d/go-vitess/)-parser and then translated from the MySQL dialect into the PostgreSQL dialect (other dialects can be implemented as plugins).


