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

/*
Optimizes parsed SQL statements, possibly at the cost of correctnes.

Used for custom SQL engines.
*/
package sqloptim

import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"

func FlattenAndOrList(expr sqlparser.Expr) sqlparser.Expr {
	const (
		snone = iota
		sor
		sand
	)
	state := snone
	var and,or sqlparser.Expr
	push := func(expr sqlparser.Expr) {
		if state == sand {
			if _,ok := or.(*sqlparser.OrExpr) ; ok { or = &sqlparser.ParenExpr{or} }
			if and==nil {
				and = or
			} else {
				and = &sqlparser.AndExpr{and,or}
			}
			or = expr
			return
		}
		if or==nil {
			or = expr
		} else {
			or = &sqlparser.OrExpr{or,expr}
		}
	}
	var rec func(expr sqlparser.Expr)
	rec = func(expr sqlparser.Expr){
		switch v := expr.(type){
		case *sqlparser.AndExpr:
			rec(v.Left)
			state = sand
			rec(v.Right)
		case *sqlparser.OrExpr:
			rec(v.Left)
			state = sor
			rec(v.Right)
		default: push(expr)
		}
	}
	rec(expr)
	if or!=nil {
		state = sand
		push(nil)
	}
	return and
}
