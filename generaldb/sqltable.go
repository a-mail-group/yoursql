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

type Result struct{
	io.Closer
	Head sql.Schema
	Body sql.RowIter
	RowsAffected uint64
	LastInsertId uint64
}

type PerClient struct{
	ds     *querier.DataSource
	ana    *analyzer.Analyzer
	sqlctx *sql.Context
}
func NewPerClient(b querier.Backend) *PerClient {
	return &PerClient{ ds: &querier.DataSource{Backend:b} , ana: querier.CreateAnalyzer() , sqlctx: sql.NewContext(context.Background()) }
}

func (p *PerClient) Destroy() {
}
func (p *PerClient) Query(def,query string) (*Result, error) {
	pv := sqlparser.Preview(query)
	
	stmt,err := sqlparser.Parse(query)
	if err!=nil { return nil,err }
	
	switch pv {
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

