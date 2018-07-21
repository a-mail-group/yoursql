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
import "github.com/a-mail-group/yoursql/generaldb/srcutils"

func ddlFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch v := node.(type) {
	case sqlparser.TableIdent: fmt.Fprintf(buf,"%q",v.String()); return
	case sqlparser.ColIdent  : fmt.Fprintf(buf,"%q",v.String()); return
	}
	node.Format(buf)
}



func (cql *CqlDB) PerformDDL(ns, name string, ddl *sqlparser.DDL) (uint64,uint64,error) {
	buf := sqlparser.NewTrackedBuffer(ddlFormatter)
	switch ddl.Action {
	case sqlparser.CreateStr:
		if ddl.TableSpec==nil { return 0,0,fmt.Errorf("Incomplete syntax: create table <<name>> EOF") }
		srcutils.PreprocessTableSpec(ddl.TableSpec)
		buf.Myprintf("create table %v %v",sqlparser.NewTableIdent(name),ddl.TableSpec)
	case sqlparser.DropStr:
		buf.Myprintf("drop   table %v",sqlparser.NewTableIdent(name))
	default:
		return 0,0,fmt.Errorf("DDL command not supported: %q",ddl.Action)
	}
	return 0,0,cql.ItsSession.Query(buf.String()).Exec()
	//fmt.Println(buf.String())
	//return 0,0,fmt.Errorf("DDL command not supported: %q",ddl.Action)
}

