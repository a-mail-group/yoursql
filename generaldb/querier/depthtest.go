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
import "gopkg.in/src-d/go-mysql-server.v0/sql/plan"

type depTest struct{
	child *depTest
}
func (d *depTest) Visit(node sql.Node) plan.Visitor {
	if _,ok := node.(*plan.SubqueryAlias); ok {
		if d.child == nil { d.child = new(depTest) }
		return d.child
	}
	if _,ok := node.(*plan.Project); ok {
		if d.child == nil { d.child = new(depTest) }
		return d.child
	}
	return d
}
func (d *depTest) count() (i int) {
	for d!=nil {
		d = d.child
		i++
	}
	return
}

func countDepth(node sql.Node) int {
	t := new(depTest)
	plan.Walk(t,node)
	return t.count()
}

type hintlist struct{
	cond sql.Expression
	next *hintlist
}
var hintsetter plan.Visitor = (*hintlist)(nil)
func (h *hintlist) Visit(node sql.Node) plan.Visitor {
	switch v := node.(type) {
	case *Hint: return &hintlist{v.Expression,h}
	case *plan.Filter: return &hintlist{v.Expression,h}
	case *Table:
		var el []sql.Expression
		for h!=nil {
			el = append(el,clauses(h.cond)...)
			h = h.next
		}
		v.hints = el
		return nil
	}
	return hintsetter
}


