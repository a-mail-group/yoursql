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

func insertFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
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

func (cql *CqlDB) PerformInsert(ns, name string, ins *sqlparser.Insert) (uint64,uint64,error) {
	if len(ins.Columns)==0 {
		return 0,0,fmt.Errorf("Column names must be specified!")
	}
	buf := sqlparser.NewTrackedBuffer(insertFormatter)
	buf.Myprintf("insert into %s %v%v",fmt.Sprintf("%q",name),ins.Columns, ins.Rows)
	
	if ins.Action!="replace" {
		buf.Myprintf(" if not exists")
	}
	
	qry := cql.ItsSession.Query(buf.String())
	err := qry.Exec()
	qry.Release()
	return 1,0,err
}

