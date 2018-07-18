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

import "gopkg.in/src-d/go-mysql-server.v0/mem"
import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "gopkg.in/src-d/go-mysql-server.v0/sql/plan"
import "gopkg.in/src-d/go-mysql-server.v0/sql/parse"
import "gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
import "gopkg.in/src-d/go-mysql-server.v0/sql/expression"
import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "fmt"

func clearAliasExpr(e sql.Expression) (sql.Expression, error) {
	switch v := e.(type){
	case *expression.Alias:
		return v.Child,nil
	}
	return e,nil
}
func clearAliasExprList(e []sql.Expression) ([]sql.Expression, error) {
	ne := make([]sql.Expression,len(e))
	var err error
	for i,ee := range e {
		ne[i],err = ee.TransformUp(clearAliasExpr)
		if err!=nil { return nil,err }
	}
	return ne,nil
}

func cloneSet(s map[string]bool) map[string]bool {
	n := make(map[string]bool)
	for k,v := range s { n[k] = v }
	return n
}

func clauses(e sql.Expression) (l []sql.Expression) {
	var traverse func(e sql.Expression)
	traverse = func(e sql.Expression) {
		switch v := e.(type) {
		case *expression.And:
			traverse(v.Left)
			traverse(v.Right)
		default:
			l = append(l,e)
		}
	}
	traverse(e)
	return
}

type schemaIndex map[[2]string]int
func mkIndex(s sql.Schema) schemaIndex {
	idx := make(schemaIndex)
	for i,col := range s {
		idx[[2]string{col.Source,col.Name}] = i
	}
	return idx
}

func (si schemaIndex) transform(ex sql.Expression) (sql.Expression, error) {
	switch v := ex.(type) {
	case *expression.GetField:
		i,ok := si[[2]string{v.Table(),v.Name()}]
		if !ok { return nil,fmt.Errorf("Field not found: %s",v) }
		return v.WithIndex(i),nil
		//return expression.NewGetFieldWithTable(i,v.Type(),v.Table(),v.Name(),v.IsNullable()),nil
		//return expression.NewGetFieldWithTable(i,v.Type(),v.Table(),v.Name(),v.IsNullable()),nil
	}
	return ex,nil
}

type planviz func(node sql.Node)
func (v planviz) Visit(node sql.Node) plan.Visitor { v(node); return v }

type visitor func(expr sql.Expression) expression.Visitor
func (v visitor) Visit(expr sql.Expression) expression.Visitor { return v(expr) }

/*
Is a condition satisfied with the given tables?
*/
func isSatisfied(n sql.Expression,tables map[string]bool) (ok bool) {
	var visit visitor
	ok = true
	visit = func(expr sql.Expression) expression.Visitor {
		switch v := expr.(type) {
		case *expression.GetField:
			if !tables[v.Table()] { ok = false }
		}
		//fmt.Println("expr",expr,ok,expr.Children())
		return visit
	}
	expression.Walk(visit,n)
	//visit(n)
	return
}

/*
Modifies a TransformNodeFunc to work across SubqueryAlias.
*/
func skipSQ(f func(n sql.Node) (sql.Node, error)) (func(n sql.Node) (sql.Node, error)) {
	var ch func(n sql.Node) (sql.Node, error)
	ch = func(n sql.Node) (sql.Node, error){
		switch v := n.(type) {
		case *plan.SubqueryAlias:
			nc,err := v.Child.TransformUp(ch)
			if err!=nil { return nil,err }
			n = plan.NewSubqueryAlias(v.Name(),nc)
		}
		return f(n)
	}
	return ch
}

/*
This TransformNodeFunc pushes down Filters.
*/
func pushdown(n sql.Node) (sql.Node, error) {
	f,ok := n.(*plan.Filter)
	if !ok { return n,nil }
	exprs := clauses(f.Expression)
	bits := make([]bool,len(exprs))
	cld,err := f.Child.TransformUp(func(n sql.Node) (sql.Node, error){
		tabs := make(map[string]bool)
		var subst []sql.Expression
		switch v := n.(type) {
		case *plan.Filter:
			f,ok := v.Child.(*plan.Filter)
			if !ok { return n,nil }
			return plan.NewFilter(expression.NewAnd(v.Expression,f.Expression),f.Child),nil
		case sql.Table: tabs[v.Name()] = true
		default:
			for _,col := range n.Schema() {
				tabs[col.Source] = true
			}
		}
		for i,expr := range exprs {
			if bits[i] { continue }
			if !isSatisfied(expr,tabs) { continue }
			//fmt.Println(expr,tabs,isSatisfied(expr,tabs))
			subst = append(subst,expr)
			bits[i] = true
		}
		if len(subst)!=0 {
			nd,err := expression.JoinAnd(subst...).TransformUp(mkIndex(n.Schema()).transform)
			if err!=nil { return nil,err }
			return plan.NewFilter(nd,n),nil
		}
		return n,nil
	})
	if err!=nil { return nil,err }
	var subst []sql.Expression
	for i,expr := range exprs {
		if bits[i] { continue }
		subst = append(subst,expr)
	}
	if len(subst)!=0 {
		return plan.NewFilter(expression.JoinAnd(subst...),cld),nil
	}
	return cld,nil
}

/*
This TransformNodeFunc pushes down filters across the boundaries of
 - SubqueryAlias
 - Project
*/
func pushdown_sq(n sql.Node) (sql.Node, error) {
	f,ok := n.(*plan.Filter)
	if !ok { return n,nil }
	switch v := f.Child.(type) {
	case *plan.SubqueryAlias:
		csch := v.Child.Schema()
		
		ne,err := f.Expression.TransformUp(func(ex sql.Expression) (sql.Expression, error) {
			switch v := ex.(type){
			case *expression.GetField:
				col := csch[v.Index()]
				return expression.NewGetFieldWithTable(v.Index(),v.Type(),col.Source,col.Name,v.IsNullable()),nil
			}
			return ex,nil
		})
		if err!=nil { return nil,err }
		
		return plan.NewSubqueryAlias(v.Name(),plan.NewFilter(ne,v.Child)),nil
	case *plan.Project:
		proj,err := clearAliasExprList(v.Projections)
		if err!=nil { return nil,err }
		ne,err := f.Expression.TransformUp(func(ex sql.Expression) (sql.Expression, error) {
			switch v := ex.(type){
			case *expression.GetField:
				return proj[v.Index()],nil
			}
			return ex,nil
		})
		if err!=nil { return nil,err }
		np := *v
		np.Child = plan.NewFilter(ne,v.Child)
		return &np,nil
	}
	return n,nil
}

/*
This TransformNodeFunc creates and pushes down, hint-annotations.
*/
func pushdown2(n sql.Node) (sql.Node, error) {
	
	/* We are interested in Filter */
	f,ok := n.(*plan.Filter)
	if !ok { return n,nil }
	
	var left,right sql.Node
	
	var constr func(sql.Node) sql.Node
	/* We are interested in Filter->Join */
	switch v := f.Child.(type) {
	case *plan.InnerJoin:
		left = v.Left
		right = v.Right
		constr = func(s sql.Node) sql.Node { return plan.NewInnerJoin(v.Left,s,v.Cond) }
	case *plan.CrossJoin:
		left = v.Left
		right = v.Right
		constr = func(s sql.Node) sql.Node { return plan.NewCrossJoin(v.Left,s) }
	default: return n,nil
	}
	
	exprs := clauses(f.Expression)
	bits := make([]bool,len(exprs))
	mtabs := make(map[string]bool)
	for _,col := range left.Schema() {
		mtabs[col.Source] = true
	}
	cld,err := right.TransformUp(func(n sql.Node) (sql.Node, error){
		tabs := cloneSet(mtabs)
		var subst []sql.Expression
		switch v := n.(type) {
		case *Hint:
			f,ok := v.Child.(*Hint)
			if !ok { return n,nil }
			return NewHint(expression.NewAnd(v.Expression,f.Expression),f.Child),nil
		case sql.Table: tabs[v.Name()] = true
		default:
			for _,col := range n.Schema() {
				tabs[col.Source] = true
			}
		}
		for i,expr := range exprs {
			if bits[i] { continue }
			if !isSatisfied(expr,tabs) { continue }
			//fmt.Println(expr,tabs,isSatisfied(expr,tabs))
			subst = append(subst,expr)
			bits[i] = true
		}
		if len(subst)!=0 {
			return NewHint(expression.JoinAnd(subst...),n),nil
		}
		return n,nil
	})
	if err!=nil { return nil,err }
	return plan.NewFilter(f.Expression,constr(cld)),nil
}

func cleanup(n sql.Node) (sql.Node,error) {
	var ch func(n sql.Node) (sql.Node, error)
	ch = func(n sql.Node) (sql.Node, error){
		switch v := n.(type) {
		case *plan.TableAlias: return v.Child,nil
		case *plan.InnerJoin:
			return plan.NewFilter(v.Cond,&plan.CrossJoin{v.BinaryNode}),nil
		}
		return n,nil
	}
	return n.TransformUp(skipSQ(ch))
}
func mergeFilter(n sql.Node) (sql.Node,error) {
	var ch func(n sql.Node) (sql.Node, error)
	ch = func(n sql.Node) (sql.Node, error){
		switch v := n.(type) {
		case *plan.Filter:
			f2,ok := v.Child.(*plan.Filter)
			if !ok { return n,nil }
			return plan.NewFilter(
				expression.NewAnd(v.Expression,f2.Expression),
				f2.Child,
			),nil
		}
		return n,nil
	}
	return n.TransformUp(skipSQ(ch))
}
func rmAnnotation(n sql.Node) (sql.Node,error) {
	switch v := n.(type){
	case *Hint: return v.Child,nil
	}
	return n,nil
}

func ParseAndProcessQuery(query string,ds *DataSource,ana *analyzer.Analyzer,ctx *sql.Context) (sql.Node,error) {
	stmt,err := sqlparser.Parse(query)
	if err!=nil { return nil,err }
	return ProcessQuery(stmt,ds,ana,ctx)
}
func ProcessQuery(stmt sqlparser.Statement,ds *DataSource,ana *analyzer.Analyzer,ctx *sql.Context) (sql.Node,error) {
	db := mem.NewDatabase("temp")
	if len(ana.Catalog.Databases)>0 {
		ana.Catalog.Databases[0] = db
	} else {
		ana.Catalog.Databases = sql.Databases{db}
	}
	ana.CurrentDatabase = "temp"
	
	sel,ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil,fmt.Errorf("expected SELECT statement, got: %s",sqlparser.String(stmt))
	}
	
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error){
		kontinue = true
		switch v := node.(type) {
		case *sqlparser.AliasedTableExpr:
			tn,ok := v.Expr.(sqlparser.TableName)
			if !ok { break }
			tab,err := ds.GetTable(tn)
			if err!=nil { return false,err }
			v.Expr = sqlparser.TableName{Name:sqlparser.NewTableIdent(tab.Name())}
			db.AddTable(tab.Name(),tab)
			if v.As.IsEmpty() { v.As = tn.Name }
			kontinue = false
		}
		return
	},sel)
	
	if err!=nil { return nil,err }
	
	node,err := parse.Parse(ctx,sqlparser.String(sel))
	
	if err!=nil { return nil,err }
	
	node,err = ana.Analyze(ctx,node)
	
	if err!=nil { return nil,err }
	
	node,err = cleanup(node)
	
	if err!=nil { return nil,err }
	
	depth := countDepth(node)
	
	for {
		node,err = node.TransformUp(skipSQ(pushdown))
		if err!=nil { return nil,err }
		depth--
		if depth<1 { break }
		node,err = node.TransformUp(skipSQ(pushdown_sq))
		if err!=nil { return nil,err }
		node,err = mergeFilter(node)
		if err!=nil { return nil,err }
	}
	
	/*
	plan.Walk(planviz(func(node sql.Node){
		if a,ok := node.(*plan.SubqueryAlias) ; ok {
			fmt.Println(a)
			for _,col := range a.Schema() {
				fmt.Println("-",col.Source,col.Name)
			}
		}
	}),node)
	*/
	
	
	node,err = node.TransformUp(skipSQ(pushdown2))
	
	if err!=nil { return nil,err }
	
	plan.Walk(hintsetter,node)
	
	node,err = node.TransformUp(skipSQ(rmAnnotation))
	
	if err!=nil { return nil,err }
	
	err = processTables(db.Tables())
	
	if err!=nil { return nil,err }
	
	for _,tab := range db.Tables() {
		ntab,ok := tab.(*Table)
		if !ok { continue }
		ntab.IOT.SetHints(ntab.hints)
	}
	
	//node,err = node.TransformUp(skipSQ(crossToStrict))
	//
	//if err!=nil { return nil,err }
	
	return node,nil
}

