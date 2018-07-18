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


package matcher

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "github.com/a-mail-group/yoursql/generaldb/querier"

import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "fmt"


func traverseOr(o *expression.Or) (r []sql.Expression) {
	var trav func(e sql.Expression)
	trav = func(e sql.Expression) {
		switch v := e.(type) {
		case *expression.Or:
			trav(v.Left)
			trav(v.Right)
		}
	}
	trav(o.Left)
	trav(o.Right)
	return
}



type Equal struct {
	Column string
	Value  sql.Expression
}
func (e Equal) String() string {
	return fmt.Sprintf("{%s eq %v}",e.Column,e.Value)
}

type checkSOb struct {
	isSelf bool
}
func (x *checkSOb) Visit(expr sql.Expression) expression.Visitor {
	switch expr.(type) {
	case *querier.SelfField:
		x.isSelf = true
	}
	return x
}
func checkS(e sql.Expression) bool {
	cob := &checkSOb{}
	expression.Walk(cob,e)
	return cob.isSelf
}

func MatchEqual(e sql.Expression) *Equal {
	switch v := e.(type) {
	case *expression.Equals:
		
		switch l := v.Left().(type) {
		case *querier.SelfField:
			if checkS(v.Right()) { return nil }
			return &Equal{l.ItsName,v.Right()}
		}
		switch r := v.Right().(type) {
		case *querier.SelfField:
			if checkS(v.Left()) { return nil }
			return &Equal{r.ItsName,v.Left()}
		}
	}
	return &Equal{}
}

type In struct {
	Column string
	Values []sql.Expression
}
func (e In) String() string {
	return fmt.Sprintf("{%s in %v}",e.Column,e.Values)
}

func MatchIn(e sql.Expression) *In {
	switch v := e.(type) {
	case *expression.In:
		switch l := v.Left().(type) {
		case *querier.SelfField:
			tu,ok := v.Right().(expression.Tuple)
			if !ok { return nil }
			return &In{l.ItsName,tu}
		}
	case *expression.Or:
		var expr []sql.Expression
		var s string
		fu := func(o string) bool {
			if s=="" { s = o }
			return s!=o
		}
		for _,raw := range traverseOr(v) {
			if eq := MatchEqual(raw); eq!=nil {
				if fu(eq.Column) { return nil }
				expr = append(expr,eq.Value)
				continue
			}
			if eq := MatchIn(raw); eq!=nil {
				if fu(eq.Column) { return nil }
				expr = append(expr,eq.Values...)
				continue
			}
			return nil
		}
		return &In{s,expr}
	}
	return nil
}

