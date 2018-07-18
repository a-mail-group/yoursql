# cassdb

Cassandra 3.* Backend for [generaldb](..)

The plugin is developed with [gocql](https://github.com/gocql/gocql) and tested against Cassandra 3.*!

## update-statements.

cassdb-specific informations on update statements.

### Where-clauses

Remember, Cassandra is not an RDBMS. Therefore updates are a little bit different that in MySQL or PostgreSQL.
See [CQL reference/Update](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlUpdate.html).

#### Where clauses

In Cassandra, on `update`the where-clause must specify the entire primary key.

Fields, that are not part of the primary key must not be specified in the where-clause, but those may be specified,
in a if-clause which is CQL-specific and invalid in the SQL-language.

SQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' AND c = 2;
```

CQL-Counterpart:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' IF c = 2; -- pk is a primary key, c is not
```

If only the primary key is specified and the if-clause is empty, eigther `IF EXISTS` is appended to the update
statement, or Cassandra will insert the row, if it doesn't exists.

CQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' IF EXISTS;
```

The following statement could insert a new row.

CQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1';
```

#### GeneralDB SQL syntax for Cassandra

GeneralDB uses a SQL parser, and is therefore not capable to process CQL directly.

CQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' IF c = 2;
```

GeneralDB SQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' AND cond(c = 2);
-- or
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE row(pk = 'key-1') AND c = 2;
-- or
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE row(pk = 'key-1') AND cond(c = 2);
```

CQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' IF EXIST;
```

GeneralDB SQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1';
```

CQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1';
```

GeneralDB SQL:
```sql
UPDATE sometable SET a = 1, b = 2, c = 3 WHERE pk = 'key-1' AND anycase();
```


### Syntactic Rat-Poison

#### Updating map-fields

CQL:
```sql
map_name[ 'index' ] = 'map_value'
```

GeneralDB SQL:
```sql
map_name = mapfield.put('index','map_value')
```

#### Map-Literals

CQL:
```sql
{ 'key1': 'value1', 'key2': 'value2' }
```

GeneralDB SQL:
```sql
kvmap('key1', 'value1', 'key2', 'value2')
```

#### Set-Literals

CQL:
```sql
{ 'value1', 'value2', 'value3', 'value4' }
```

GeneralDB SQL:
```sql
vset('value1', 'value2', 'value3', 'value4')
```

#### List-Literals

CQL:
```sql
[ 'value1', 'value2', 'value3', 'value4' ]
```

GeneralDB SQL:
```sql
vlist('value1', 'value2', 'value3', 'value4')
```

