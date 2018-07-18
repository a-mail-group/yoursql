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
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"

import "fmt"

var EIsPlaceHolder = fmt.Errorf("Is Place-Holder")
var EIsVoid = fmt.Errorf("Is Void")

type Hint struct {
	plan.UnaryNode
	Expression sql.Expression
}

// NewHint creates a new hint node.
func NewHint(expression sql.Expression, child sql.Node) *Hint {
	return &Hint{
		UnaryNode:  plan.UnaryNode{Child: child},
		Expression: expression,
	}
}

func (p *Hint) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := p.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewHint(p.Expression, child))
}

func (p *Hint) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	expr, err := p.Expression.TransformUp(f)
	if err != nil {
		return nil, err
	}

	child, err := p.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewHint(expr, child), nil
}

func (p *Hint) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Hint(%s)", p.Expression)
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p *Hint) Expressions() []sql.Expression {
	return []sql.Expression{p.Expression}
}

func (p *Hint) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	e, err := p.Expression.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return NewHint(e, p.Child), nil
}
func (p *Hint) RowIter(ctx *sql.Context) (sql.RowIter, error) { return p.Child.RowIter(ctx) }

var _ sql.Node = (*Hint)(nil)

type SelfField struct{
	ItsType sql.Type
	ItsNullable bool
	ItsName string
	ItsIndex int
}
func (s *SelfField) Resolved() bool { return true }
func (s *SelfField) Type() sql.Type { return s.ItsType }
func (s *SelfField) IsNullable() bool { return s.ItsNullable }
func (s *SelfField) Eval(*sql.Context, sql.Row) (interface{}, error) { return nil,EIsPlaceHolder }
func (s *SelfField) TransformUp(f sql.TransformExprFunc) (sql.Expression, error){ return f(s) }
func (s *SelfField) Children() []sql.Expression { return nil }
func (s *SelfField) String() string { return fmt.Sprintf("#->%s",s.ItsName) }

type ForeignField struct{
	SelfField
	Remote *Table
	Tu TypeUniverse
}
func (s *ForeignField) String() string { return fmt.Sprintf("${%s}->%s",s.Remote.name,s.ItsName) }
var ERemoteScanNil = fmt.Errorf("s.Remote.Scan = nil")
var EBackendScanNil = fmt.Errorf("s.Remote.Scan.BackendScan = nil")
func (s *ForeignField) Eval(*sql.Context, sql.Row) (interface{}, error) {
	sc := s.Remote.Scan
	if sc==nil { return nil,ERemoteScanNil }
	bs := sc.BackendScan
	if bs==nil { return nil,EBackendScanNil }
	return bs.GetValue(s.ItsIndex,s.Tu)
}

func processTable(tab *Table,m map[string]sql.Table) error {
	transform := func(expr sql.Expression) (sql.Expression,error) {
		switch v := expr.(type) {
		case *expression.GetField:
			name := v.Table()
			field := v.Name()
			sf := &SelfField{v.Type(),v.IsNullable(),field,0}
			if tab.name == name {
				sf.ItsIndex = tab.index[field]
				return sf,nil
			} else {
				f,ok := m[name]
				if !ok { return nil,fmt.Errorf("Table not found %q",name) }
				ntab,ok := f.(*Table)
				if !ok { return nil,fmt.Errorf("wrong table type %q",name) }
				sf.ItsIndex = ntab.index[field]
				return &ForeignField{*sf,ntab,nil},nil
			}
		}
		return expr,nil
	}
	for i,h := range tab.hints {
		nh,err := h.TransformUp(transform)
		if err!=nil { return err }
		tab.hints[i] = nh
	}
	return nil
}

func processTables(m map[string]sql.Table) error {
	for _,tab := range m {
		t,ok := tab.(*Table)
		if !ok { continue }
		t.index = mkLcSchIdx(t.inner)
	}
	for _,tab := range m {
		rtab,ok := tab.(*Table)
		if !ok { continue }
		err := processTable(rtab,m)
		if err!=nil { return err }
	}
	return nil
}


