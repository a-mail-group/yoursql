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
import "io"
import "fmt"

func sxJoinDbg(i ...interface{}){
	fmt.Println(i...)
}

func crossToStrict(n sql.Node) (sql.Node, error) {
	switch v := n.(type) {
	case *plan.CrossJoin:
		return &StrictXJoin{v.BinaryNode},nil
	}
	return n,nil
}

/* UNUSED! use plan.CrossJoin instead*/
type StrictXJoin struct {
	plan.BinaryNode
}
/* UNUSED! use plan.NewCrossJoin(left,right) instead */
func NewStrictXJoin(left sql.Node, right sql.Node) *StrictXJoin {
	return &StrictXJoin{
		BinaryNode: plan.BinaryNode{
			Left:  left,
			Right: right,
		},
	}
}
func (p *StrictXJoin) Schema() sql.Schema {
	return append(p.Left.Schema(), p.Right.Schema()...)
}
func (p *StrictXJoin) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}
func (p *StrictXJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	left, err := p.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewStrictXJoin(left, right))
}
func (p *StrictXJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("StrictXJoin")
	_ = pr.WriteChildren(p.Left.String(), p.Right.String())
	return pr.String()
}

// TransformExpressionsUp implements the Transformable interface.
func (p *StrictXJoin) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	left, err := p.Left.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewStrictXJoin(left, right), nil
}


func (p *StrictXJoin) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	driver,err := p.Left.RowIter(ctx)
	if err!=nil { return nil,err }
	
	return &strictXJoinIter{
		driver: driver,
		driven: p.Right,
		ctx: ctx,
	},nil
}

type strictXJoinIter struct{
	driver sql.RowIter
	driven sql.Node
	ctx    *sql.Context
	// ------------------
	tongue bool
	prefix sql.Row
	adhoc  sql.RowIter
	// ------------------
	err    error
}
func (s *strictXJoinIter) Close() error {
	var e1,e2 error
	e1 = s.driver.Close()
	if s.adhoc!=nil { e2 = s.adhoc.Close() }
	
	if e1==nil { e1 = e2 }
	return e1
}
func (s *strictXJoinIter) Next() (sql.Row, error) {
	var err error
	var right,row sql.Row
	if s.err!=nil { return nil,s.err }
	
	restart:
	
	if !s.tongue {
		var pref sql.Row
		pref,s.err = s.driver.Next()
		if s.err!=nil { sxJoinDbg("Left Driver NEXT ",s.err," ",s.err!=nil); return nil,s.err }
		s.prefix = pref
		s.tongue = true
		s.adhoc,s.err = s.driven.RowIter(s.ctx)
		if s.err!=nil { sxJoinDbg("Right Driven SCAN ",s.err," ",s.err!=nil); return nil,s.err }
	}
	
	right,err = s.adhoc.Next()
	if err!=nil {
		sxJoinDbg("Right Driven NEXT ",err," ",err!=nil)
		if err!=io.EOF { s.err = err ; return nil,err }
		s.adhoc.Close()
		s.tongue = false
		s.adhoc = nil
		goto restart
	}
	
	row = append(row, s.prefix...)
	row = append(row, right...)
	
	return row,nil
}

