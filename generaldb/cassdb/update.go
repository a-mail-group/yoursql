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

import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "fmt"
import "strings"

func updateFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch v := node.(type) {
	case sqlparser.TableIdent: fmt.Fprintf(buf,"%q",v.String()); return
	case sqlparser.ColIdent  : fmt.Fprintf(buf,"%q",v.String()); return
	case *sqlparser.AliasedExpr:
		buf.Myprintf("%v",v.Expr)
		return
	case *sqlparser.UpdateExpr:
		switch v2 := v.Expr.(type) {
		case *sqlparser.FuncExpr:
			switch strings.ToLower(v2.Name.String()) {
			case "put":
				if v.Name.Name.String()==v2.Qualifier.String() {
					if len(v2.Exprs)<2 { break }
					buf.Myprintf("%v[%v] = %v",v.Name,v2.Exprs[0],v2.Exprs[1])
					return
				}
			}
		}
	case *sqlparser.FuncExpr:
		switch strings.ToLower(v.Name.String()) {
		case "kvmap":
			buf.Myprintf("{")
			l := len(v.Exprs)
			l ^= (l&1)
			for i,e := range v.Exprs[:l] {
				if i==0 {
					buf.Myprintf("%v",e)
				} else if (i&1)==0 {
					buf.Myprintf(",%v",e)
				} else {
					buf.Myprintf(":%v ",e)
				}
			}
			buf.Myprintf("}")
			return
		case "vset":
			buf.Myprintf("{")
			for i,e := range v.Exprs {
				if i==0 {
					buf.Myprintf("%v",e)
				} else {
					buf.Myprintf(",%v",e)
				}
			}
			buf.Myprintf("}")
			return
		case "vlist":
			buf.Myprintf("[")
			for i,e := range v.Exprs {
				if i==0 {
					buf.Myprintf("%v",e)
				} else {
					buf.Myprintf(",%v",e)
				}
			}
			buf.Myprintf("]")
			return
		}
	}
	node.Format(buf)
}
func flatten(node sqlparser.Expr) (nodes []sqlparser.Expr){
	var f func(node sqlparser.Expr)
	f = func(node sqlparser.Expr) {
		switch v := node.(type) {
		case *sqlparser.AndExpr:
			f(v.Left)
			f(v.Right)
		default:
			nodes = append(nodes,node)
		}
	}
	f(node)
	return
}

func (cql *CqlDB) PerformUpdate(ns, name string, upd *sqlparser.Update) (uint64,uint64,error) {
	if upd.Limit!=nil || len(upd.OrderBy)!=0 { return 0,0,fmt.Errorf("invalid syntax ") }
	if upd.Where==nil { return 0,0,fmt.Errorf("invalid syntax ") }
	buf := sqlparser.NewTrackedBuffer(updateFormatter)
	buf.Myprintf("update %s set %v ",fmt.Sprintf("%q",name),upd.Exprs)
	clauses := flatten(upd.Where.Expr)
	
	var others,pkc,cond []sqlparser.Expr
	transact := true
	
	for _,clause := range clauses {
		switch v := clause.(type) {
		case *sqlparser.FuncExpr:
			switch strings.ToLower(fmt.Sprintf("%v.%v",v.Qualifier,v.Name)) {
			case ".row","cql.row":
				for _,e := range v.Exprs {
					ae,ok := e.(*sqlparser.AliasedExpr)
					if !ok { continue }
					pkc = append(pkc,ae.Expr)
					break
				}
			case ".cond","cql.cond":
				for _,e := range v.Exprs {
					ae,ok := e.(*sqlparser.AliasedExpr)
					if !ok { continue }
					cond = append(cond,ae.Expr)
					break
				}
			case ".anycase","cql.anycase":
				transact = false
			default:
				others = append(others,clause)
			}
		default:
			others = append(others,clause)
		}
	}
	if len(others)!=0 {
		if len(pkc)==0 {
			pkc = others
		} else if len(cond)==0 {
			cond = others
		} else {
			cond = append(cond,others...)
		}
		others = nil
	}
	px := "where"
	for _,clause := range pkc {
		buf.Myprintf("%s %v ",px,clause)
		px = "and"
	}
	if len(cond)!=0 {
		px = "if"
		for _,clause := range cond {
			buf.Myprintf("%s %v ",px,clause)
			px = "and"
		}
	} else if transact {
		buf.Myprintf("if exists ")
	}
	
	qry := cql.ItsSession.Query(buf.String())
	err := qry.Exec()
	qry.Release()
	return 0,0,err
}
