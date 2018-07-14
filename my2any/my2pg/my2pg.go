/*
   Copyright 2018 Simon Schmidt

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


package my2pg

import "github.com/a-mail-group/yoursql/my2any"
import "database/sql"
import sqlv "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-vitess.v0/sqltypes"
import "github.com/lib/pq"
import "github.com/lib/pq/hstore"
import "strings"
import "reflect"
import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "fmt"

import iradix "github.com/hashicorp/go-immutable-radix"

var atypes *iradix.Tree

func init() {
	atypes = iradix.New()
	atypes,_,_ = atypes.Insert([]byte("_INT4"),1)
	atypes,_,_ = atypes.Insert([]byte("_INT8"),1)
	atypes,_,_ = atypes.Insert([]byte("_FLOAT4"),2)
	atypes,_,_ = atypes.Insert([]byte("_FLOAT8"),2)
	atypes,_,_ = atypes.Insert([]byte("_NUMERIC"),2)
	atypes,_,_ = atypes.Insert([]byte("_TEXT"),3)
	atypes,_,_ = atypes.Insert([]byte("_VARCHAR"),3)
	atypes,_,_ = atypes.Insert([]byte("_BPCHAR"),3)
	atypes,_,_ = atypes.Insert([]byte("_BYTEA"),4)
	atypes,_,_ = atypes.Insert([]byte("_BOOL"),5)
}

// HACK! Rename this type so it doesn't clashes with method name!
type ntype sqlv.Type
type hstype struct {
	ntype
}
func (hstype) SQL(i interface{}) sqltypes.Value {
	mp := i.(hstore.Hstore).Map
	if mp==nil {
		return sqlv.JSON.SQL(nil)
	}
	nmp := make(map[string]interface{})
	for k,v := range mp {
		if v.Valid {
			nmp[k] = v.String
		} else {
			nmp[k] = nil
		}
	}
	return sqlv.JSON.SQL(nmp)
}

type PqConverter struct {
	my2any.Converter
}
func (p PqConverter) Convert(nct *sql.ColumnType) (col *sqlv.Column,scan interface{}) {
	if nct.ScanType().Kind() == reflect.Interface {
		name := nct.DatabaseTypeName()
		i,ok := atypes.Get([]byte(name))
		if ok {
		} else if strings.HasPrefix(name,"_INT") {
			i = 1
		} else if strings.HasPrefix(name,"_FLOAT") {
			i = 2
		} else if name=="" {
			i = 6
		}
		if i==nil { i = 0 }
		switch i.(int) {
		case 1:
			col = &sqlv.Column{Name:nct.Name(),Type:sqlv.JSON}
			scan = new(pq.Int64Array)
			return
		case 2:
			col = &sqlv.Column{Name:nct.Name(),Type:sqlv.JSON}
			scan = new(pq.Float64Array)
			return
		case 3:
			col = &sqlv.Column{Name:nct.Name(),Type:sqlv.JSON}
			scan = new(pq.StringArray)
			return
		case 4:
			col = &sqlv.Column{Name:nct.Name(),Type:sqlv.JSON}
			scan = new(pq.ByteaArray)
			return
		case 5:
			col = &sqlv.Column{Name:nct.Name(),Type:sqlv.JSON}
			scan = new(pq.BoolArray)
			return
		case 6:
			col = &sqlv.Column{Name:nct.Name(),Type:hstype{sqlv.JSON}}
			scan = new(hstore.Hstore)
			return
		}
	}
	return p.Converter.Convert(nct)
}

type PgSpecialFeatures struct {
	my2any.SpecialFeatures
}
func (p PgSpecialFeatures) Perform(db my2any.GenericDB,cmd string,args ...string) (*sql.Rows,error) {
	switch cmd {
	case "show.tables":
		return db.Query(`SELECT tablename::text FROM pg_catalog.pg_tables WHERE schemaname = $1`,args[0])
	case "show.columns":
		return db.Query(`
SELECT
	a.attname::text AS "Field",
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Type",
	CASE
		WHEN a.attnotnull THEN 'NO'
		ELSE 'YES'
	END::text AS "Null",
	CASE
		WHEN 'p' IN (SELECT contype FROM pg_catalog.pg_constraint b WHERE (a.attrelid = b.conrelid AND array[a.attnum] <@ b.conkey )) THEN 'PRI'
		WHEN 'u' IN (SELECT contype FROM pg_catalog.pg_constraint b WHERE (a.attrelid = b.conrelid AND array[a.attnum] <@ b.conkey )) THEN 'UNIQUE'
		ELSE ''
	END::text AS "Key",
	CASE
		WHEN 'a' IN (SELECT 'a'::char FROM pg_catalog.pg_attrdef b WHERE (a.attrelid = b.adrelid AND a.attnum = b.adnum ) AND adsrc LIKE 'nextval%') THEN 'NULL'
		WHEN 'd' IN (SELECT 'd'::char FROM pg_catalog.pg_attrdef b WHERE (a.attrelid = b.adrelid AND a.attnum = b.adnum )) THEN (SELECT adsrc FROM pg_catalog.pg_attrdef b WHERE (a.attrelid = b.adrelid AND a.attnum = b.adnum ) LIMIT 1)
		ELSE 'NULL'
	END::text AS "Default",
	CASE
		WHEN 'a' IN (SELECT 'a'::char FROM pg_catalog.pg_attrdef b WHERE (a.attrelid = b.adrelid AND a.attnum = b.adnum ) AND adsrc LIKE 'nextval%') THEN 'auto_increment'
		ELSE ''
	END::text AS "Extra"
	FROM pg_catalog.pg_attribute a
WHERE
a.attrelid = (
	SELECT cls.oid
	FROM pg_catalog.pg_class cls
	JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace=nsp.oid
	WHERE nspname = $1 AND relname = $2
)
AND a.attnum > 0
		`,args[0],args[1])
	}
	return p.SpecialFeatures.Perform(db,cmd,args...)
}
/*
SELECT
	a.attname::text AS "InsertID"
	FROM pg_catalog.pg_attribute a
WHERE
a.attrelid = (
	SELECT cls.oid
	FROM pg_catalog.pg_class cls
	JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace=nsp.oid
	WHERE relname = 'aitest' AND nspname = 'public'
)
AND a.attnum > 0
AND 'a' IN (SELECT 'a'::char FROM pg_catalog.pg_attrdef b WHERE (a.attrelid = b.adrelid AND a.attnum = b.adnum ) AND adsrc LIKE 'nextval%')
AND 'p' IN (SELECT contype FROM pg_catalog.pg_constraint b WHERE (a.attrelid = b.conrelid AND array[a.attnum] <@ b.conkey ))
*/
func (p PgSpecialFeatures) Rewrite(db my2any.GenericDB,ast sqlparser.Statement,pvp *int) (string,bool) {
	i,ok := ast.(*sqlparser.Insert)
	if !ok { return "",false }
	rs,err := db.Query(`
SELECT
	a.attname::text AS "InsertID"
	FROM pg_catalog.pg_attribute a
WHERE
a.attrelid = (
	SELECT cls.oid
	FROM pg_catalog.pg_class cls
	JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace=nsp.oid
	WHERE relname = $1 AND nspname = $2
)
AND a.attnum > 0
AND 'a' IN (SELECT 'a'::char FROM pg_catalog.pg_attrdef b WHERE (a.attrelid = b.adrelid AND a.attnum = b.adnum ) AND adsrc LIKE 'nextval%')
AND 'p' IN (SELECT contype FROM pg_catalog.pg_constraint b WHERE (a.attrelid = b.conrelid AND array[a.attnum] <@ b.conkey ))
	`,i.Table.Name.String(),i.Table.Qualifier.String())
	if err!=nil { return "",false }
	defer rs.Close()
	buf := sqlparser.NewTrackedBuffer(PgFormatter)
	buf.Myprintf("%v returning ",ast)
	comma := false
	var s string
	for rs.Next() {
		rs.Scan(&s)
		if comma {
			/*fmt.Fprintf(buf,",%q",s) */
			break /* XXX: We only use one element and ignore others */
		} else {
			fmt.Fprintf(buf,"%q",s)
			comma = true
		}
	}
	if !comma{ return "",false }
	*pvp = my2any.StmtxInsertReturning
	return buf.String(),true
}
