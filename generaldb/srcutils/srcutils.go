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


package srcutils

import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"

func FlattenAnd(node sqlparser.Expr) (nodes []sqlparser.Expr) {
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
func ComposeAnd(list []sqlparser.Expr) (expr sqlparser.Expr) {
	for i,part := range list {
		if i==0 {
			expr = part
		} else {
			expr = &sqlparser.AndExpr{expr,part}
		}
	}
	return
}

func Name(n string) sqlparser.Expr {
	return &sqlparser.ColName{Name:sqlparser.NewColIdent(n)}
}

/*
"a" -> "a = ?","a",true

"a as b" -> "b = ?","a",true

Errornous input -> nil,nil,false
*/
func MatcherFromSelectExpression(e sqlparser.SelectExpr) (sqlparser.Expr,sqlparser.SelectExpr,bool) {
	a,ok := e.(*sqlparser.AliasedExpr)
	if !ok { return nil,nil,false }
	name := a.As.String()
	if name=="" {
		c,ok := a.Expr.(*sqlparser.ColName)
		if !ok { return nil,nil,false }
		name = c.Name.String()
	}
	
	cond := &sqlparser.ComparisonExpr{
		Operator: "=",
		Left: Name(name),
		Right: sqlparser.NewValArg([]byte("?")),
	}
	return cond,&sqlparser.AliasedExpr{Expr:a.Expr},true
}

func Unpack(nodes sqlparser.SelectExprs) (dst sqlparser.Exprs) {
	dst = make(sqlparser.Exprs,0,len(nodes))
	for _,node := range nodes {
		a,ok := node.(*sqlparser.AliasedExpr)
		if !ok { continue }
		dst = append(dst,a.Expr)
	}
	return
}
