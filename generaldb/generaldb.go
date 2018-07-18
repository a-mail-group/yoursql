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


package generaldb

import "gopkg.in/src-d/go-vitess.v0/mysql"
import "gopkg.in/src-d/go-vitess.v0/sqltypes"
import "gopkg.in/src-d/go-vitess.v0/vt/proto/query"
import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "fmt"
import "io"

var ESorry = fmt.Errorf("Sorry!")

type Gateway struct{
	B Backend
}
func (g *Gateway) NewConnection(c *mysql.Conn) {
	c.ClientData = NewPerClient(g.B)
}
func (g *Gateway) ConnectionClosed(c *mysql.Conn) {
	c.ClientData.(*PerClient).Destroy()
	c.ClientData = nil
}
func (g *Gateway) ComQuery(c *mysql.Conn,query string,callback func(*sqltypes.Result) error) error {
	r,err := c.ClientData.(*PerClient).Query(c.SchemaName,query)
	if err!=nil { return err }
	if r.Closer!=nil { defer r.Close() }
	
	sr := new(sqltypes.Result)
	
	cnt := 0
	sr.RowsAffected = r.RowsAffected
	sr.InsertID = r.LastInsertId
	sr.Fields = schemaToFields(r.Head)
	
	if r.Body==nil { goto eofun }
	
	for {
		row, err := r.Body.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}
		
		sr.Rows = append(sr.Rows, rowToSQL(r.Head, row))
		sr.RowsAffected++
		cnt++
		
		if cnt>=256 {
			if err = callback(sr); err!=nil { return err }
			sr = new(sqltypes.Result)
			sr.Fields = schemaToFields(r.Head)
		}
	}
	
	eofun:
	
	return callback(sr)
}
func rowToSQL(s sql.Schema, row []interface{}) []sqltypes.Value {
	o := make([]sqltypes.Value, len(row))
	for i, v := range row {
		o[i] = s[i].Type.SQL(v)
	}

	return o
}

func schemaToFields(s sql.Schema) []*query.Field {
	fields := make([]*query.Field, len(s))
	for i, c := range s {
		fields[i] = &query.Field{
			Name: c.Name,
			Type: c.Type.Type(),
		}
	}

	return fields
}

