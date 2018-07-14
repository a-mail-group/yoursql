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


package my2any

import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"

func decodeSql(s string) (sqlparser.Statement, error) {
	return sqlparser.Parse(s)
}
func encodeSql(stmt sqlparser.Statement) string {
	return sqlparser.String(stmt)
}

type Syntaxer interface{
	Preprocess(ast sqlparser.Statement, schema string)
	/* This method is used if no other case matches. */
	EncodeAny(ast sqlparser.Statement) string
	//EncodeInsert(ast sqlparser.Statement) string
}

type DefaultSyntaxerClass struct{}
func (DefaultSyntaxerClass) Preprocess(ast sqlparser.Statement, schema string) {}
func (DefaultSyntaxerClass) EncodeAny(ast sqlparser.Statement) string { return sqlparser.String(ast) }
func (DefaultSyntaxerClass) EncodeInsert(ast sqlparser.Statement) string { return sqlparser.String(ast) }
var DefaultSyntaxer Syntaxer = DefaultSyntaxerClass{}

func Qualify(stmt sqlparser.Statement, schema string) error {
	g := func(tn *sqlparser.TableName) {
		if tn.Name.IsEmpty() { return }
		if tn.Qualifier.IsEmpty() {
			tn.Qualifier = sqlparser.NewTableIdent(schema)
		}
	}
	f := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		kontinue = true
		switch v := node.(type) {
		case *sqlparser.AliasedTableExpr:
			if tn,ok := v.Expr.(sqlparser.TableName); ok {
				if !tn.Name.IsEmpty() && tn.Qualifier.IsEmpty() {
					if v.As.IsEmpty() {
						v.As = tn.Name
					}
					tn.Qualifier = sqlparser.NewTableIdent(schema)
					v.Expr = tn
				}
			}
		case *sqlparser.Insert:
			g(&v.Table)
		case *sqlparser.DDL:
			g(&v.Table)
			g(&v.NewName)
		}
		return
	}
	return sqlparser.Walk(f,stmt)
}
