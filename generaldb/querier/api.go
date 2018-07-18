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


package querier

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "fmt"
import "io"

type localSchIdx map[string]int
func mkLcSchIdx(s sql.Schema) localSchIdx {
	idx := make(localSchIdx)
	for i,col := range s {
		idx[col.Name] = i
	}
	return idx
}

type Table struct{
	name  string
	orig  sqlparser.TableName
	inner sql.Schema
	index localSchIdx
	hints []sql.Expression
	IOT   InstanceOfTable
	Scan  *TableScan
}
func (*Table) Resolved() bool { return true }
func (t *Table) Name() string { return t.name }
func (t *Table) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) { return f(t) }
func (t *Table) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) { return t, nil }
func (t *Table) Children() []sql.Node { return nil }
func (t *Table) Schema() sql.Schema {
	ret := make(sql.Schema,len(t.inner))
	for i,pcol := range t.inner {
		col := *pcol
		col.Source = t.name
		ret[i] = &col
	}
	return ret
}
func (t *Table) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	if t.Scan!=nil {
		t.Scan.Close()
	}
	
	ri,err := t.IOT.BackendScan(ctx)
	if err!=nil { return nil,err }
	
	scan := &TableScan{ri,t,nil}
	t.Scan = scan
	return scan,nil
}
func (t *Table) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Table %s (%s)",t.name,sqlparser.String(t.orig))
	clds := make([]string,len(t.hints))
	for i,hint := range t.hints {
		clds[i] = fmt.Sprint("Hint: ",hint)
	}
	if len(clds)>0 {
		_ = pr.WriteChildren(clds...)
	}
	return pr.String()
}

type TableScan struct{
	BackendScan
	Table *Table
	Error error
}
func (ti *TableScan) Next() (sql.Row,error) {
	err := ti.Error
	if err!=nil { return nil,err }
	return ti.BackendScan.Next()
}
func (ti *TableScan) Close() error {
	if ti.BackendScan==nil { return nil }
	ti.BackendScan.Close()
	ti.Error = io.EOF
	ti.BackendScan = nil
	if ti.Table.Scan == ti {
		ti.Table.Scan = nil
	}
	return nil
}

var test_tables = map[string]sql.Schema{
	"myusers":{
		{Name:"id",Type:sql.Text},
		{Name:"givname",Type:sql.Text},
		{Name:"surname",Type:sql.Text},
	},
	"myuattrs":{
		{Name:"user_id",Type:sql.Text},
		{Name:"attnam",Type:sql.Text},
		{Name:"attdata",Type:sql.Text},
	},
}

type DataSource struct{
	Count   uint
	Backend Backend
	Default string
}
func (d *DataSource) GetTable(t sqlparser.TableName) (*Table,error) {
	d.Count++
	
	name := t.Name.String()
	ns := t.Qualifier.String()
	if len(ns)==0 { ns = d.Default }
	bt,err := d.Backend.GetTable(ns,name)
	if err!=nil { return nil,err }
	
	sch := bt.Schema()
	iot,err := bt.Prepare()
	if err!=nil { return nil,err }
	
	return &Table{
		fmt.Sprintf("temp%d",d.Count),
		t,
		sch,
		nil,
		nil,
		iot,
		nil,
	},nil
}


