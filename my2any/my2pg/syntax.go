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


package my2pg

import "github.com/a-mail-group/yoursql/my2any"
import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "fmt"
import "regexp"
import "strings"

var nnarg = regexp.MustCompile(`^\:v`)
var ai_int = regexp.MustCompile(`^(int4?|integer|serial)`)
var ai_bigint = regexp.MustCompile(`^(int8|bigint|bigserial)`)

func binop(buf *sqlparser.TrackedBuffer, op string, exprs sqlparser.SelectExprs) {
	if len(exprs)==0 { buf.WriteString("NULL") }
	for i,se := range exprs {
		if i==0 {
			buf.Myprintf("%v",se)
		} else {
			buf.Myprintf(" %s %v",op,se)
		}
	}
}

func PgFormatter (buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch v := node.(type) {
	case sqlparser.ColIdent: fmt.Fprintf(buf,"%q",v.String())
	case sqlparser.TableIdent: fmt.Fprintf(buf,"%q",v.String())
	case *sqlparser.ColIdent: fmt.Fprintf(buf,"%q",v.String())
	case *sqlparser.TableIdent: fmt.Fprintf(buf,"%q",v.String())
	case *sqlparser.Limit:
		if v==nil {
			node.Format(buf)
		} else if v.Offset==nil {
			node.Format(buf)
		} else {
			buf.Myprintf(" offset %v limit %v",v.Offset,v.Rowcount)
		}
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.ValArg:
			if nnarg.Match(v.Val) {
				buf.Myprintf("$%s",v.Val[2:])
			} else {
				// XXX: this is a stop-gap solution!
				sqlparser.NewStrVal(v.Val).Format(buf)
			}
		default:
			node.Format(buf)
		}
	case *sqlparser.FuncExpr:
		switch strings.ToLower(fmt.Sprintf("%s.%s",v.Qualifier,v.Name)) {
		case "hstore.union": binop(buf,"||",v.Exprs)
		case "hstore.lookup": binop(buf,"->",v.Exprs)
		case "hstore.pair": binop(buf,"=>",v.Exprs)
		case "hstore.contains": binop(buf,"?",v.Exprs)
		case "hstore.all": binop(buf,"?&",v.Exprs)
		case "hstore.any": binop(buf,"?|",v.Exprs)
		case "hstore.super": binop(buf,"@>",v.Exprs)
		case "hstore.sub": binop(buf,"<@",v.Exprs)
		case "hstore.remove": binop(buf,"-",v.Exprs)
		default:
			switch strings.ToLower(v.Qualifier.String()) {
			case "pg_cast":
				if len(v.Exprs)==0 { buf.WriteString("NULL") }
				for i,se := range v.Exprs {
					if i==0 {
						buf.Myprintf("(%v)::%s",se,v.Name.String())
					}
				}
			default:
				node.Format(buf)
			}
		}
	default:
		node.Format(buf)
	}
}

type PgSyntaxer struct {
	my2any.Syntaxer
}
func (PgSyntaxer) Preprocess(ast sqlparser.Statement, schema string) {
	if schema!="" {
		my2any.Qualify(ast,schema)
	}
	if ddl,ok := ast.(*sqlparser.DDL); ok {
		switch ddl.Action {
		case "create":
			for _,col := range ddl.TableSpec.Columns {
				if !col.Type.Autoincrement { continue }
				if ai_int.MatchString(col.Type.Type) {
					col.Type.Type = "serial"
				} else if ai_bigint.MatchString(col.Type.Type) {
					col.Type.Type = "bigserial"
				}
				col.Type.Autoincrement = false
			}
		}
	}
	if del,ok := ast.(*sqlparser.Delete); ok {
		if del.Limit!=nil {
			sel := new(sqlparser.Select)
			sel.From = del.TableExprs
			sel.Where = del.Where
			sel.OrderBy = del.OrderBy
			sel.Limit = del.Limit
			
			if len(del.Targets)==0 {
				sel.SelectExprs = sqlparser.SelectExprs{ &sqlparser.AliasedExpr{
					Expr:&sqlparser.ColName{Name:sqlparser.NewColIdent("ctid")},
				}}
				del.Where = &sqlparser.Where{"where",
					&sqlparser.ComparisonExpr{
						Operator:"in",
						Left:&sqlparser.ColName{Name:sqlparser.NewColIdent("ctid")},
						Right:&sqlparser.Subquery{sel},
					},
				}
				del.Limit = nil
				del.OrderBy = nil
			} else {
				crit := make([]sqlparser.Expr,len(del.Targets))
				for i,targ := range del.Targets {
					sc := new(sqlparser.Select)
					*sc = *sel
					sc.SelectExprs = sqlparser.SelectExprs{ &sqlparser.AliasedExpr{
						Expr:&sqlparser.ColName{Name:sqlparser.NewColIdent("ctid"),Qualifier:targ},
					}}
					crit[i] = &sqlparser.ComparisonExpr{
						Operator:"in",
						Left:&sqlparser.ColName{Name:sqlparser.NewColIdent("ctid"),Qualifier:targ},
						Right:&sqlparser.Subquery{sc},
					}
				}
				var expr sqlparser.Expr
				for i,c := range crit {
					if i==0 {
						expr = c
					} else {
						expr = &sqlparser.AndExpr{expr,c}
					}
				}
				del.Where = &sqlparser.Where{"where",expr}
				del.Limit = nil
				del.OrderBy = nil
			}
		} else {
			del.OrderBy = nil
		}
	}
}
func (PgSyntaxer) EncodeAny(ast sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(PgFormatter)
	buf.Myprintf("%v",ast)
	return buf.String()
}

