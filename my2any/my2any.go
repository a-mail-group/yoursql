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

import "database/sql"
import "gopkg.in/src-d/go-vitess.v0/mysql"
import "gopkg.in/src-d/go-vitess.v0/sqltypes"
import "gopkg.in/src-d/go-vitess.v0/vt/proto/query"
import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import sqlv "gopkg.in/src-d/go-mysql-server.v0/sql"
import "regexp"
import "reflect"
import "strings"
import "time"

import "fmt"

var descRx = regexp.MustCompile(`^[dD][eE][sS][cC](?:[rR][iI][bB][eE])?\s+(\S+)`)

type Converter interface{
	Convert(nct *sql.ColumnType) (col *sqlv.Column,scan interface{})
}

func deref(i interface{}) interface{} {
	switch v := i.(type) {
	case *uint32: return *v
	case *uint64: return *v
	case *int32: return *v
	case *int64: return *v
	case *float32: return *v
	case *float64: return *v
	case *string: return *v
	case *[]byte: return *v
	case *time.Time: return *v
	case *interface{}: return *v
	}
	return reflect.ValueOf(i).Elem().Interface()
}

func combine(a *sql.ColumnType) (nt reflect.Type,it sqlv.Type) {
	nt = a.ScanType()
	switch nt.Kind() {
	case reflect.Uint8,reflect.Uint16:
		nt = reflect.ValueOf(uint32(0)).Type()
		fallthrough
	case reflect.Uint32: it = sqlv.Uint32
	case reflect.Uint:
		nt = reflect.ValueOf(uint64(0)).Type()
		fallthrough
	case reflect.Uint64: it = sqlv.Uint64
	case reflect.Int8,reflect.Int16:
		nt = reflect.ValueOf(int32(0)).Type()
		fallthrough
	case reflect.Int32: it = sqlv.Int32
	case reflect.Int:
		nt = reflect.ValueOf(int64(0)).Type()
		fallthrough
	case reflect.Int64: it = sqlv.Int64
	case reflect.Float32: it = sqlv.Float32
	case reflect.Float64: it = sqlv.Float64
	case reflect.Bool: it = sqlv.Boolean
	case reflect.String: it = sqlv.Text
	case reflect.Interface: it = sqlv.JSON
	case reflect.Slice:
		if nt.Elem().Kind()==reflect.Uint8 {
			it = sqlv.Blob
			break
		}
		fallthrough
	default:
		switch nt.String() {
		case "time.Time":
			switch strings.ToLower(a.DatabaseTypeName()) {
			case "date": it = sqlv.Date
			default: it = sqlv.Timestamp
			}
		default:
			it = sqlv.Null
		}
	}
	return
}

type DefaultConverterClass struct{}
func (DefaultConverterClass) Convert(nct *sql.ColumnType) (col *sqlv.Column,scan interface{}) {
	col = new(sqlv.Column)
	col.Name = nct.Name()
	st,ct := combine(nct)
	col.Type = ct
	scan = reflect.New(st).Interface()
	return
}

const (
	StmtxInsertReturning = 128+iota
)

var DefaultConverter Converter = DefaultConverterClass{}

type SpecialFeatures interface{
	Perform(db GenericDB,cmd string,args ...string) (*sql.Rows,error)
	Rewrite(db GenericDB,ast sqlparser.Statement,pvp *int) (string,bool)
}
type DefaultSpecialFeaturesClass struct{}
func (DefaultSpecialFeaturesClass) Perform(db GenericDB,cmd string,args ...string) (*sql.Rows,error) {
	return nil,fmt.Errorf("Sorry!")
}
func (DefaultSpecialFeaturesClass) Rewrite(db GenericDB,ast sqlparser.Statement,pvp *int) (string,bool) { return "",false }
var DefaultSpecialFeatures SpecialFeatures = DefaultSpecialFeaturesClass{}


type ClientData struct{
	Tx *sql.Tx
}
func (c *ClientData) Destroy() {
	if c.Tx!=nil {
		c.Tx.Rollback()
	}
}

type Gateway struct{
	DB  *sql.DB
	CC  Converter
	Syn Syntaxer
	SF  SpecialFeatures
}
func (g *Gateway) NewConnection(c *mysql.Conn) {
	c.ClientData = new(ClientData)
}
func (g *Gateway) ConnectionClosed(c *mysql.Conn) {
	cd := c.ClientData.(*ClientData)
	c.ClientData = nil
	cd.Destroy()
}
func (g *Gateway) getDB(c *mysql.Conn) GenericDB {
	tx := c.ClientData.(*ClientData).Tx
	if tx==nil { return g.DB }
	return tx
}
func (g *Gateway) ComQuery(c *mysql.Conn,query string,callback func(*sqltypes.Result) error) error {
	pv := sqlparser.Preview(query)
	switch pv {
	case sqlparser.StmtBegin:
		cd := c.ClientData.(*ClientData)
		if cd.Tx!=nil {
			return &mysql.SQLError{
				mysql.ERCantDoThisDuringAnTransaction,
				mysql.SSCantDoThisDuringAnTransaction,
				"Cant begin a transaction during a transaction.",
				query,
			}
		}
		tx,err := g.DB.Begin()
		if err!=nil { return err }
		cd.Tx = tx
		return callback(new(sqltypes.Result))
	case sqlparser.StmtCommit:
		cd := c.ClientData.(*ClientData)
		if cd.Tx==nil {
			return fmt.Errorf("transaction required")
		}
		err := cd.Tx.Commit()
		if err!=nil {
			cd.Tx.Rollback()
		}
		cd.Tx = nil
		if err==nil { err = callback(new(sqltypes.Result)) }
		return err
	case sqlparser.StmtRollback:
		cd := c.ClientData.(*ClientData)
		if cd.Tx==nil {
			return fmt.Errorf("transaction required")
		}
		err := cd.Tx.Rollback()
		cd.Tx = nil
		if err==nil { err = callback(new(sqltypes.Result)) }
		return err
	case sqlparser.StmtShow:
		return g.show(c,query,callback)
	case sqlparser.StmtOther:
		if descRx.MatchString(query) {
			rs,err := g.SF.Perform(g.getDB(c),"show.columns",c.SchemaName,descRx.FindStringSubmatch(query)[1])
			if err!=nil { return err }
			return g.streamRows(c,rs,callback)
		}
	}
	
	st,err := decodeSql(query)
	if err!=nil { return err }
	g.Syn.Preprocess(st,c.SchemaName)
	
	var nq string
	
	if nnq,ok := g.SF.Rewrite(g.getDB(c),st,&pv) ; ok {
		nq = nnq
	} else {
		nq = g.Syn.EncodeAny(st)
	}
	
	switch pv {
	case sqlparser.StmtDDL:
		//fmt.Println(nq)
		return g.executeScript(c,nq,callback)
	case sqlparser.StmtInsert,sqlparser.StmtUpdate,sqlparser.StmtDelete:
		return g.executeScript(c,nq,callback)
	case sqlparser.StmtSelect:
		return g.executeQuery(c,nq,callback)
	case StmtxInsertReturning:
		return g.executeScriptReturning(c,nq,callback)
	}
	return fmt.Errorf("Sorry!")
}

func (g *Gateway) executeScriptReturning(c *mysql.Conn,query string,callback func(*sqltypes.Result) error) error {
	rs,err := g.getDB(c).Query(query)
	if err!=nil { return err }
	return g.sendResultReturning(c,rs,callback)
}
func (g *Gateway) sendResultReturning(c *mysql.Conn,rs *sql.Rows,callback func(*sqltypes.Result) error) error {
	defer rs.Close()
	
	sr := new(sqltypes.Result)
	
	for rs.Next() {
		rs.Scan(&sr.InsertID)
		sr.RowsAffected++
	}
	
	return callback(sr)
}


func (g *Gateway) show(c *mysql.Conn,query string,callback func(*sqltypes.Result) error) error {
	st,err := decodeSql(query)
	if err!=nil { return err }
	sh := st.(*sqlparser.Show)
	switch sh.Type {
	case "tables":
		rs,err := g.SF.Perform(g.getDB(c),"show.tables",c.SchemaName)
		if err!=nil { return err }
		return g.streamRows(c,rs,callback)
	}
	return fmt.Errorf("Sorry!")
}
func (g *Gateway) executeScript(c *mysql.Conn,query string,callback func(*sqltypes.Result) error) error {
	rs,err := g.getDB(c).Exec(query)
	if err!=nil { return err }
	return g.sendResult(c,rs,callback)
}
func (g *Gateway) sendResult(c *mysql.Conn,res sql.Result,callback func(*sqltypes.Result) error) error {
	sr := new(sqltypes.Result)
	
	lid,_ := res.LastInsertId()
	rad,_ := res.RowsAffected()
	
	sr.InsertID = uint64(lid)
	sr.RowsAffected = uint64(rad)
	
	return callback(sr)
}
func (g *Gateway) executeQuery(c *mysql.Conn,query string,callback func(*sqltypes.Result) error) error {
	rs,err := g.getDB(c).Query(query)
	if err!=nil { return err }
	return g.streamRows(c,rs,callback)
}
func (g *Gateway) streamRows(c *mysql.Conn,rs *sql.Rows,callback func(*sqltypes.Result) error) error {
	defer rs.Close()
	
	cts,err := rs.ColumnTypes()
	if err!=nil { return err }
	lcts := len(cts)
	sch := make(sqlv.Schema,lcts)
	sca := make([]interface{},len(cts))
	vls := make([]interface{},len(cts))
	
	for i,ct := range cts {
		sch[i],sca[i] = g.CC.Convert(ct)
	}
	
	
	sr := new(sqltypes.Result)
	sr.Fields = schemaToFields(sch)
	
	chunk := 0
	
	for rs.Next() {
		err = rs.Scan(sca...)
		if err!=nil { return err }
		for i,scav := range sca {
			vls[i] = deref(scav)
		}
		
		if chunk>1024 {
			err = callback(sr)
			if err!=nil { return err }
			sr = new(sqltypes.Result)
			sr.Fields = schemaToFields(sch)
		}
		sr.Rows = append(sr.Rows,rowToSQL(sch,vls))
		sr.RowsAffected++
		
		chunk += lcts
	}
	
	return callback(sr)
}

func rowToSQL(s sqlv.Schema, row []interface{}) []sqltypes.Value {
	o := make([]sqltypes.Value, len(row))
	for i, v := range row {
		o[i] = s[i].Type.SQL(v)
	}

	return o
}

func schemaToFields(s sqlv.Schema) []*query.Field {
	fields := make([]*query.Field, len(s))
	for i, c := range s {
		fields[i] = &query.Field{
			Name: c.Name,
			Type: c.Type.Type(),
		}
	}

	return fields
}

