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


package generaldb

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "context"

import "io"
import "fmt"

import "github.com/a-mail-group/yoursql/generaldb/querier"

var ETodo error = fmt.Errorf("Todo")

type UpdateBackend interface{
	PerformUpdate(ns, name string, upd *sqlparser.Update) (uint64,uint64,error) /* Rows-Affected, LastID, Error*/
}

/*
This interface is composed of smaller interfaces.
Instead of trying to implement all of them, implementors should focus on the
sub-interfaces and use CreateBackend!
*/
type Backend interface{
	querier.Backend
	UpdateBackend
}

type BackendImpl struct{
	QueryBackend querier.Backend
	UpdateBackend
}
func (b *BackendImpl) GetTable(ns, name string) (querier.BackendTable, error) {
	if b.QueryBackend==nil { return nil,ETodo }
	return b.QueryBackend.GetTable(ns,name)
}
func (b *BackendImpl) PerformUpdate(ns, name string, upd *sqlparser.Update) (uint64,uint64,error) {
	if b.UpdateBackend==nil { return 0,0,ETodo }
	return b.UpdateBackend.PerformUpdate(ns,name,upd)
}

func CreateBackend(i interface{}) Backend {
	if b,ok := i.(Backend) ; ok { return b }
	b := &BackendImpl{}
	if e,ok := i.(querier.Backend); ok { b.QueryBackend = e }
	if e,ok := i.(UpdateBackend); ok { b.UpdateBackend = e }
	return b
}

type Result struct{
	io.Closer
	Head sql.Schema
	Body sql.RowIter
	RowsAffected uint64
	LastInsertId uint64
}

type PerClient struct{
	b      Backend
	ds     *querier.DataSource
	ana    *analyzer.Analyzer
	sqlctx *sql.Context
}
func NewPerClient(b Backend) *PerClient {
	return &PerClient{ b:b, ds: &querier.DataSource{Backend:b} , ana: querier.CreateAnalyzer() , sqlctx: sql.NewContext(context.Background()) }
}

func (p *PerClient) Destroy() {
}
func (p *PerClient) Query(def,query string) (*Result, error) {
	pv := sqlparser.Preview(query)
	
	stmt,err := sqlparser.Parse(query)
	if err!=nil { return nil,err }
	
	switch pv {
	case sqlparser.StmtUpdate:
		upd,ok := stmt.(*sqlparser.Update)
		if !ok { return nil,ESorry }
		
		if len(upd.TableExprs)>1 { return nil,fmt.Errorf("too many tables") }
		if len(upd.TableExprs)<1 { return nil,fmt.Errorf("table is missing") }
		te,ok := upd.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(upd.TableExprs[0])) }
		
		tn,ok := te.Expr.(sqlparser.TableName)
		if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(te.Expr)) }
		
		ns := tn.Qualifier.String()
		if ns=="" { ns = def }
		
		name := tn.Name.String()
		
		ra,id,err := p.b.PerformUpdate(def, name, upd)
		if err!=nil { return nil,err }
		
		rs := new(Result)
		rs.RowsAffected = ra
		rs.LastInsertId = id
		return rs,nil
	case sqlparser.StmtSelect:
		p.ds.Default = def
		p.ds.Count = 0
		node,err := querier.ProcessQuery(stmt,p.ds,p.ana,p.sqlctx)
		
		if err!=nil { return nil,err }
		
		iter,err := node.RowIter(p.sqlctx)
		
		if err!=nil { return nil,err }
		
		rs := new(Result)
		rs.Closer = iter
		rs.Head = node.Schema()
		rs.Body = iter
		return rs,nil
	}
	return nil,ESorry
}

