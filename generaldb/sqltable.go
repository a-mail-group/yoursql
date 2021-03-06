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

type PreparedUpdate interface{
	Perform(i []interface{}) error
	Close() error
}

type UpdateBackend interface{
	PerformUpdate(ns, name string, upd *sqlparser.Update) (uint64,uint64,error) /* Rows-Affected, LastID, Error*/
	PrepareUpdate(ns, name string, upd *sqlparser.Update) (PreparedUpdate,error)
}

type InsertBackend interface{
	PerformInsert(ns, name string, ins *sqlparser.Insert) (uint64,uint64,error) /* Rows-Affected, LastID, Error*/
}

type DeleteBackend interface{
	PerformDelete(ns, name string, del *sqlparser.Delete) (uint64,uint64,error) /* Rows-Affected, LastID, Error*/
	PrepareDelete(ns, name string, del *sqlparser.Delete) (PreparedUpdate,error)
}

type DDLBackend interface{
	PerformDDL(ns, name string, ddl *sqlparser.DDL) (uint64,uint64,error)
}

/*
This interface is composed of smaller interfaces.

Itr is subject to changes, so instead of trying to implement all of them,
implementors should focus on the sub-interfaces and use CreateBackend() !
*/
type Backend interface{
	querier.Backend
	UpdateBackend
	InsertBackend
	DeleteBackend
	DDLBackend
}

type BackendImpl struct{
	QueryBackend querier.Backend
	UpdateBackend
	InsertBackend
	DeleteBackend
	DDLBackend
}
func (b *BackendImpl) GetTable(ns, name string) (querier.BackendTable, error) {
	if b.QueryBackend==nil { return nil,ETodo }
	return b.QueryBackend.GetTable(ns,name)
}
func (b *BackendImpl) PerformUpdate(ns, name string, upd *sqlparser.Update) (uint64,uint64,error) {
	if b.UpdateBackend==nil { return 0,0,ETodo }
	return b.UpdateBackend.PerformUpdate(ns,name,upd)
}
func (b *BackendImpl) PrepareUpdate(ns, name string, upd *sqlparser.Update) (PreparedUpdate,error) {
	if b.UpdateBackend==nil { return nil,ETodo }
	return b.UpdateBackend.PrepareUpdate(ns,name,upd)
}
func (b *BackendImpl) PerformInsert(ns, name string, ins *sqlparser.Insert) (uint64,uint64,error) {
	if b.InsertBackend==nil { return 0,0,ETodo }
	return b.InsertBackend.PerformInsert(ns,name,ins)
}
func (b *BackendImpl) PerformDelete(ns, name string, del *sqlparser.Delete) (uint64,uint64,error) {
	if b.DeleteBackend==nil { return 0,0,ETodo }
	return b.DeleteBackend.PerformDelete(ns,name,del)
}
func (b *BackendImpl) PrepareDelete(ns, name string, del *sqlparser.Delete) (PreparedUpdate,error) {
	if b.DeleteBackend==nil { return nil,ETodo }
	return b.DeleteBackend.PrepareDelete(ns,name,del)
}
func (b *BackendImpl) PerformDDL(ns, name string, ddl *sqlparser.DDL) (uint64,uint64,error) {
	if b.DDLBackend==nil { return 0,0,ETodo }
	return b.DDLBackend.PerformDDL(ns,name,ddl)
}
var _ Backend = (*BackendImpl)(nil)

type BackendSelector interface{
	SelectBackend(ns, name string) (Backend,bool)
}
type BackendWithSelector struct{
	Default  Backend
	Selector BackendSelector
}
func (b *BackendWithSelector) get(ns,name string) Backend {
	be,ok := b.Selector.SelectBackend(ns,name)
	if ok { return be }
	return b.Default
}
func (b *BackendWithSelector) GetTable(ns, name string) (querier.BackendTable, error) {
	return b.get(ns,name).GetTable(ns,name)
}
func (b *BackendWithSelector) PerformUpdate(ns, name string, upd *sqlparser.Update) (uint64,uint64,error) {
	return b.get(ns,name).PerformUpdate(ns,name,upd)
}
func (b *BackendWithSelector) PrepareUpdate(ns, name string, upd *sqlparser.Update) (PreparedUpdate,error) {
	return b.get(ns,name).PrepareUpdate(ns,name,upd)
}
func (b *BackendWithSelector) PerformInsert(ns, name string, ins *sqlparser.Insert) (uint64,uint64,error) {
	return b.get(ns,name).PerformInsert(ns,name,ins)
}
func (b *BackendWithSelector) PerformDelete(ns, name string, del *sqlparser.Delete) (uint64,uint64,error) {
	return b.get(ns,name).PerformDelete(ns,name,del)
}
func (b *BackendWithSelector) PrepareDelete(ns, name string, del *sqlparser.Delete) (PreparedUpdate,error) {
	return b.get(ns,name).PrepareDelete(ns,name,del)
}
func (b *BackendWithSelector) PerformDDL(ns, name string, ddl *sqlparser.DDL) (uint64,uint64,error) {
	return b.get(ns,name).PerformDDL(ns,name,ddl)
}

var _ Backend = (*BackendWithSelector)(nil)

type DatabaseMap map[string]Backend
func (dbm DatabaseMap) SelectBackend(ns, name string) (b Backend,ok bool) { b,ok = dbm[ns]; return  }

func CreateBackend(i interface{}) Backend {
	if b,ok := i.(Backend) ; ok { return b }
	b := &BackendImpl{}
	if e,ok := i.(querier.Backend); ok { b.QueryBackend = e }
	if e,ok := i.(UpdateBackend); ok { b.UpdateBackend = e }
	if e,ok := i.(InsertBackend); ok { b.InsertBackend = e }
	if e,ok := i.(DeleteBackend); ok { b.DeleteBackend = e }
	if e,ok := i.(DDLBackend); ok { b.DDLBackend = e }
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
	case sqlparser.StmtDDL:
		ddl,ok := stmt.(*sqlparser.DDL)
		if !ok { return nil,ESorry }
		var ns,name string
		switch ddl.Action {
		case sqlparser.CreateStr:
			name = ddl.NewName.Name.String()
			ns = ddl.NewName.Qualifier.String()
		default:
			name = ddl.Table.Name.String()
			ns = ddl.Table.Qualifier.String()
		}
		
		if ns=="" { ns = def }
		
		ra,id,err := p.b.PerformDDL(ns,name,ddl)
		if err!=nil { return nil,err }
		
		rs := new(Result)
		rs.RowsAffected = ra
		rs.LastInsertId = id
		return rs,nil
	case sqlparser.StmtInsert,sqlparser.StmtReplace:
		ins,ok := stmt.(*sqlparser.Insert)
		if !ok { return nil,ESorry }
		
		_,ok = ins.Rows.(sqlparser.Values)
		if !ok { return nil,fmt.Errorf("unsupported syntax: expected values(...), got %s",sqlparser.String(ins.Rows)) }
		
		name := ins.Table.Name.String()
		ns := ins.Table.Qualifier.String()
		if ns=="" { ns = def }
		
		ra,id,err := p.b.PerformInsert(ns, name, ins)
		if err!=nil { return nil,err }
		
		rs := new(Result)
		rs.RowsAffected = ra
		rs.LastInsertId = id
		return rs,nil
	case sqlparser.StmtUpdate:
		upd,ok := stmt.(*sqlparser.Update)
		if !ok { return nil,ESorry }
		
		if len(upd.TableExprs)<1 { return nil,fmt.Errorf("table is missing") }
		
		if updateIsScan(upd) {
			return p.specialUpdate(def,upd)
		}
		
		if len(upd.TableExprs)>1 { return nil,fmt.Errorf("too many tables") }
		te,ok := upd.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(upd.TableExprs[0])) }
		
		tn,ok := te.Expr.(sqlparser.TableName)
		if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(te.Expr)) }
		
		ns := tn.Qualifier.String()
		if ns=="" { ns = def }
		
		name := tn.Name.String()
		
		ra,id,err := p.b.PerformUpdate(ns, name, upd)
		if err!=nil { return nil,err }
		
		rs := new(Result)
		rs.RowsAffected = ra
		rs.LastInsertId = id
		return rs,nil
	case sqlparser.StmtDelete:
		upd,ok := stmt.(*sqlparser.Delete)
		if !ok { return nil,ESorry }
		
		if len(upd.TableExprs)<1 { return nil,fmt.Errorf("table is missing") }
		
		if updateIsScan(upd) {
			return p.specialDelete(def,upd)
		}
		
		if len(upd.TableExprs)>1 { return nil,fmt.Errorf("too many tables") }
		te,ok := upd.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(upd.TableExprs[0])) }
		
		tn,ok := te.Expr.(sqlparser.TableName)
		if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(te.Expr)) }
		
		ns := tn.Qualifier.String()
		if ns=="" { ns = def }
		
		name := tn.Name.String()
		
		ra,id,err := p.b.PerformDelete(ns, name, upd)
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

