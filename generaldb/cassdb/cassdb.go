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


package cassdb

import "github.com/gocql/gocql"
import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "github.com/a-mail-group/yoursql/generaldb/utils"
import "github.com/a-mail-group/yoursql/generaldb/querier"
import "github.com/a-mail-group/yoursql/generaldb/matcher"
import "reflect"
import "fmt"
import "io"
import "bytes"

var Cassandra querier.TypeUniverse = &querier.TypeUniverseImpl{"Cassandra"}

func GetSchema(sess *gocql.Session,table string) ([]gocql.ColumnInfo,error) {
	i := sess.Query(fmt.Sprintf(`SELECT * FROM %q LIMIT 1`,table)).Iter()
	cols := i.Columns()
	err := i.Close()
	if len(cols)==0 {
		if err==nil { err = fmt.Errorf("No such table: %q",table) }
		return nil,err
	}
	return cols,nil
}

type Column struct {
	Sql sql.Column
	Scantype reflect.Type
	Conv *utils.Conversion
}

func buildType(nt reflect.Type,ct gocql.TypeInfo, col *Column) (stp sql.Type) {
	switch nt.Kind() {
	case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32: return sql.Uint32
	case reflect.Uint64: return sql.Uint64
	case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32: return sql.Int32
	case reflect.Int64: return sql.Int64
	case reflect.Float32: return sql.Float32
	case reflect.Float64: return sql.Float64
	case reflect.Slice:
		stp = sql.Array(buildType(nt.Elem(),ct,col))
		if col.Conv!=nil { *col.Conv = col.Conv.Slice() }
		return
	case reflect.Map: return sql.JSON
	case reflect.Ptr,reflect.Struct:
		switch nt.String() {
		case "*time.Time":
			if ct.Type()==gocql.TypeDate {
				return sql.Date
			} else {
				return sql.Timestamp
			}
		case "*big.Int","*inf.Dec","*gocql.UUID":
			col.Conv = new(utils.Conversion)
			*col.Conv = utils.StringConversion(nt)
			return sql.Text
		}
	}
	
	// Unknown types...
	col.Conv = new(utils.Conversion)
	*col.Conv = utils.StringConversion(nt)
	return sql.Text
}

func CreateColumn(colinf *gocql.ColumnInfo) (col Column) {
	col.Sql.Name = colinf.Name
	col.Scantype = reflect.ValueOf(colinf.TypeInfo.New()).Elem().Type()
	col.Sql.Type = buildType(col.Scantype,colinf.TypeInfo,&col)
	return
}
func (col *Column) ScanType() (scan interface{},cp func(), cell reflect.Value) {
	var pre reflect.Value
	pre = reflect.New(col.Scantype)
	if col.Conv==nil {
		cell = pre.Elem()
		cp = func() {  }
	} else {
		cc := col.Conv.Conv
		prec := pre.Elem()
		post := reflect.New(col.Conv.Dst)
		cell = post.Elem()
		cp = func() { cc(prec,cell) }
	}
	scan = pre.Interface()
	return
}

type CqlRowIter struct {
	Scan   []interface{}
	Copier []func()
	Cells  []reflect.Value
	Iter   *gocql.Iter
}
func CreateCqlRowIter(cols []Column,iter *gocql.Iter) (ri *CqlRowIter,e error) {
	if len(iter.Columns())==0 {
		err := iter.Close()
		if err==nil { err = fmt.Errorf("invalid resultset") }
		return nil,err
	}
	ri = new(CqlRowIter)
	ri.Scan = make([]interface{},len(cols))
	ri.Copier = make([]func(),len(cols))
	ri.Cells = make([]reflect.Value,len(cols))
	for i := range cols {
		ri.Scan[i],ri.Copier[i],ri.Cells[i] = cols[i].ScanType()
	}
	ri.Iter = iter
	return
}
func (ri *CqlRowIter) Close() error { return ri.Iter.Close() }
func (ri *CqlRowIter) Next() (sql.Row, error) {
	if !ri.Iter.Scan(ri.Scan...) { return nil,io.EOF }
	rw := make(sql.Row,len(ri.Copier))
	for _,cp := range ri.Copier { cp() }
	for i := range ri.Copier { rw[i] = ri.Cells[i].Interface() }
	return rw,nil
}
func (ri *CqlRowIter) GetValue(index int,tu querier.TypeUniverse) (interface{},error) {
	if tu==nil {
		return ri.Cells[index].Interface(),nil
	}
	if tu==Cassandra {
		// TODO: more efficient please!
		return reflect.ValueOf(ri.Scan[index]).Elem().Interface(),nil
	}
	return nil,fmt.Errorf("unsupported TU: %v",tu)
}

type CqlTable struct {
	ItsColumns []Column
	ItsSchema  sql.Schema
	ItsSession *gocql.Session
	ItsName    string
}

func NewCqlTable(sess *gocql.Session, name string, colinfs []gocql.ColumnInfo) (c *CqlTable) {
	c = new(CqlTable)
	c.ItsColumns = make([]Column,len(colinfs))
	c.ItsSchema  = make(sql.Schema,len(colinfs))
	c.ItsSession = sess
	c.ItsName = name
	
	for i := range colinfs {
		c.ItsColumns[i] = CreateColumn(&colinfs[i])
		c.ItsSchema[i] = &c.ItsColumns[i].Sql
	}
	return
}
func (c *CqlTable) sql() *bytes.Buffer {
	buf := new(bytes.Buffer)
	s := "select %q"
	for _,sch := range c.ItsSchema {
		fmt.Fprintf(buf,s,sch.Name)
		s = ",%q"
	}
	fmt.Fprintf(buf," from %q",c.ItsName)
	
	return buf
}
func (c *CqlTable) Schema() sql.Schema { return c.ItsSchema }
func (c *CqlTable) Prepare() (querier.InstanceOfTable,error) { return &CqlInstance{Table:c,Query:c.sql().String()},nil }

type CqlInstance struct {
	Table *CqlTable
	Query string
	args  []interface{}
	exprs []sql.Expression
}
func (c *CqlInstance) GetSupportedOutputs() []querier.TypeUniverse { return []querier.TypeUniverse{Cassandra} }
func (c *CqlInstance) SetHints(hints []sql.Expression) {
	strset := make(map[string]int)
	strmap := make(map[string]interface{})
	//for _,hint := range hints {
	//	expression.Walk(depfinder,hint)
	//}
	for _,hint := range hints {
		if nh := matcher.MatchEqual(hint); nh!=nil {
			if strset[nh.Column]>=10 { continue }
			strmap[nh.Column] = nh
			strset[nh.Column] = 10
			continue
		}
		if nh := matcher.MatchIn(hint); nh!=nil {
			if strset[nh.Column]>=9 { continue }
			strmap[nh.Column] = nh
			strset[nh.Column] = 9
			continue
		}
	}
	ch := 0
	buf := c.Table.sql()
	var args []sql.Expression
	pre := " where "
	for _,e := range strmap {
		switch v := e.(type) {
		case *matcher.Equal:
			fmt.Fprint(buf,pre)
			fmt.Fprintf(buf,"%q = ?",v.Column)
			args = append(args,v.Value)
			handleExpression(v.Value)
		case *matcher.In:
			fmt.Fprint(buf,pre)
			fmt.Fprintf(buf,"%q in (",v.Column)
			comma := ""
			for _,value := range v.Values {
				fmt.Fprintf(buf,"%s?",comma)
				comma = ","
				args = append(args,value)
				handleExpression(value)
			}
			fmt.Fprint(buf,")")
		default: continue
		}
		pre = " and "
		ch++
	}
	if ch==0 { return }
	fmt.Fprint(buf," allow filtering")
	//fmt.Println(buf,args)
	
	c.Query = buf.String()
	c.args = make([]interface{},len(args))
	c.exprs = args
	//fmt.Println(strmap,hints)
}
func (c *CqlInstance) BackendScan(ctx *sql.Context) (querier.BackendScan, error) {
	for i,expr := range c.exprs {
		v,e := expr.Eval(ctx,nil)
		if e!=nil { return nil,e }
		c.args[i] = v
	}
	//fmt.Println(c.Query,c.exprs,c.args)
	
	return CreateCqlRowIter(c.Table.ItsColumns,c.Table.ItsSession.Query(c.Query,c.args...).Iter())
}


type CqlDB struct{
	ItsSession *gocql.Session
}
func (cql *CqlDB) GetTable(ns,name string) (querier.BackendTable,error) {
	sch,err := GetSchema(cql.ItsSession,name)
	if err!=nil { return nil,err }
	tab := NewCqlTable(cql.ItsSession,name,sch)
	return tab,nil
}

func handleExpression(expr sql.Expression) {
	switch v := expr.(type) {
	case *querier.ForeignField:
		for _,tu := range v.Remote.IOT.GetSupportedOutputs() {
			if tu==Cassandra { v.Tu = Cassandra }
		}
	}
}

type depfind struct{}
var depfinder expression.Visitor = (*depfind)(nil)
func (*depfind) Visit(expr sql.Expression) expression.Visitor {
	switch v := expr.(type) {
	case *querier.ForeignField:
		for _,tu := range v.Remote.IOT.GetSupportedOutputs() {
			if tu==Cassandra { v.Tu = Cassandra }
		}
	}
	return depfinder
}



