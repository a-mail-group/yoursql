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

import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "github.com/a-mail-group/yoursql/generaldb/srcutils"
import "github.com/a-mail-group/yoursql/generaldb/querier"

import "strings"
import "fmt"
import "io"

//func updateIsScan(stmt *sqlparser.Update) bool
func updateIsScan(estmt sqlparser.Statement) bool {
	var te sqlparser.TableExprs
	var wh *sqlparser.Where
	switch stmt := estmt.(type) {
	case *sqlparser.Update: te,wh = stmt.TableExprs,stmt.Where
	case *sqlparser.Delete: te,wh = stmt.TableExprs,stmt.Where
	}
	
	if wh==nil { return false }
	if len(te)<1 { return false }
	clauses := srcutils.FlattenAnd(wh.Expr)
	for _,clause := range clauses {
		switch v := clause.(type) {
		case *sqlparser.FuncExpr:
			switch strings.ToLower(fmt.Sprintf("%v.%v",v.Qualifier,v.Name)) {
			case "gdb.via","gdb.ident","gdb.rowident","gdb.row_ident": return true
			}
		}
	}
	return false
}
func updateScanSplit(stmt *sqlparser.Update) (*sqlparser.Update,*sqlparser.Select,error) {
	clauses := srcutils.FlattenAnd(stmt.Where.Expr)
	var wheres sqlparser.Exprs
	var primary_keys sqlparser.SelectExprs
	for _,clause := range clauses {
		switch v := clause.(type) {
		case *sqlparser.FuncExpr:
			
			switch strings.ToLower(v.Qualifier.String()) {
			case "gdb":
				//switch strings.ToLower(fmt.Sprintf("%v",v.Name))
				switch strings.ToLower(v.Name.String()) {
				case "via","ident","rowident","row_ident":
					primary_keys = append(primary_keys,v.Exprs...)
				}
			}
		default:
			wheres = append(wheres,clause)
		}
	}
	
	uwheres := make(sqlparser.Exprs,len(primary_keys))
	
	for i,pk := range primary_keys {
		var ok bool
		uwheres[i],primary_keys[i],ok = srcutils.MatcherFromSelectExpression(pk)
		if !ok { return nil,nil,fmt.Errorf("Cannot decompose primary key field: %s",sqlparser.String(pk)) }
	}
	
	nup := &sqlparser.Update{
		/*
		 * Only the first table is being updated!
		 */
		TableExprs: stmt.TableExprs[:1],
		
		Exprs: stmt.Exprs,
		Where: sqlparser.NewWhere("where",srcutils.ComposeAnd(uwheres)),
	}
	sel := &sqlparser.Select{
		SelectExprs: primary_keys,
		From: stmt.TableExprs,
		Where: sqlparser.NewWhere("where",srcutils.ComposeAnd(wheres)),
		OrderBy: stmt.OrderBy,
		Limit: stmt.Limit,
	}
	return nup,sel,nil
}


func (p *PerClient) specialUpdate(def string,stmt *sqlparser.Update) (*Result, error) {
	nup,sel,err := updateScanSplit(stmt)
	
	if err!=nil { return nil,err }
	
	te,ok := nup.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(nup.TableExprs[0])) }
	
	tn,ok := te.Expr.(sqlparser.TableName)
	if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(te.Expr)) }
	
	ns := tn.Qualifier.String()
	if ns=="" { ns = def }
	
	name := tn.Name.String()
	
	p.ds.Default = def
	p.ds.Count = 0
	node,err := querier.ProcessQuery(sel,p.ds,p.ana,p.sqlctx)
	
	if err!=nil { return nil,err }
	
	ri,err := node.RowIter(p.sqlctx)
	
	defer ri.Close()
	
	if err!=nil { return nil,err }
	
	dest,err := p.b.PrepareUpdate(ns, name, nup)
	
	if err!=nil { return nil,err }
	
	defer dest.Close()
	
	rs := new(Result)
	
	for {
		row,err := ri.Next()
		if err==io.EOF { break }
		if err!=nil { return nil,err }
		err = dest.Perform(row)
		if err!=nil { return nil,err }
		rs.RowsAffected++
	}
	
	return rs,nil
}

func deleteScanSplit(stmt *sqlparser.Delete) (*sqlparser.Delete,*sqlparser.Select,error) {
	clauses := srcutils.FlattenAnd(stmt.Where.Expr)
	var wheres sqlparser.Exprs
	var primary_keys sqlparser.SelectExprs
	for _,clause := range clauses {
		switch v := clause.(type) {
		case *sqlparser.FuncExpr:
			
			switch strings.ToLower(v.Qualifier.String()) {
			case "gdb":
				//switch strings.ToLower(fmt.Sprintf("%v",v.Name))
				switch strings.ToLower(v.Name.String()) {
				case "via","ident","rowident","row_ident":
					primary_keys = append(primary_keys,v.Exprs...)
				}
			}
		default:
			wheres = append(wheres,clause)
		}
	}
	
	uwheres := make(sqlparser.Exprs,len(primary_keys))
	
	for i,pk := range primary_keys {
		var ok bool
		uwheres[i],primary_keys[i],ok = srcutils.MatcherFromSelectExpression(pk)
		if !ok { return nil,nil,fmt.Errorf("Cannot decompose primary key field: %s",sqlparser.String(pk)) }
	}
	
	nup := &sqlparser.Delete{
		/*
		 * Only the first table is being deleted from!
		 */
		TableExprs: stmt.TableExprs[:1],
		
		Where: sqlparser.NewWhere("where",srcutils.ComposeAnd(uwheres)),
	}
	sel := &sqlparser.Select{
		SelectExprs: primary_keys,
		From: stmt.TableExprs,
		Where: sqlparser.NewWhere("where",srcutils.ComposeAnd(wheres)),
		OrderBy: stmt.OrderBy,
		Limit: stmt.Limit,
	}
	return nup,sel,nil
}


func (p *PerClient) specialDelete(def string,stmt *sqlparser.Delete) (*Result, error) {
	nup,sel,err := deleteScanSplit(stmt)
	
	if err!=nil { return nil,err }
	
	te,ok := nup.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(nup.TableExprs[0])) }
	
	tn,ok := te.Expr.(sqlparser.TableName)
	if !ok { return nil,fmt.Errorf("invalid table expression %s",sqlparser.String(te.Expr)) }
	
	ns := tn.Qualifier.String()
	if ns=="" { ns = def }
	
	name := tn.Name.String()
	
	p.ds.Default = def
	p.ds.Count = 0
	node,err := querier.ProcessQuery(sel,p.ds,p.ana,p.sqlctx)
	
	if err!=nil { return nil,err }
	
	ri,err := node.RowIter(p.sqlctx)
	
	defer ri.Close()
	
	if err!=nil { return nil,err }
	
	dest,err := p.b.PrepareDelete(ns, name, nup)
	
	if err!=nil { return nil,err }
	
	defer dest.Close()
	
	rs := new(Result)
	
	for {
		row,err := ri.Next()
		if err==io.EOF { break }
		if err!=nil { return nil,err }
		err = dest.Perform(row)
		if err!=nil { return nil,err }
		rs.RowsAffected++
	}
	
	return rs,nil
}
