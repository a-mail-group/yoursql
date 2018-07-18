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


package utils

import "gopkg.in/src-d/go-mysql-server.v0/sql"
import "reflect"
import "gopkg.in/src-d/go-vitess.v0/vt/proto/query"
import "gopkg.in/src-d/go-vitess.v0/sqltypes"
import "fmt"
type SqlType sql.Type

type TextType struct{
	SqlType
	InternalType query.Type
}
func (t *TextType) Type() query.Type {
	return t.InternalType
}
func (t *TextType) SQL(i interface{}) sqltypes.Value {
	return t.SqlType.SQL(fmt.Sprint(i))
}
func (t *TextType) Compare(a, b interface{}) (int, error) {
	return t.SqlType.Compare(fmt.Sprint(a),fmt.Sprint(b))
}
func (t *TextType) Convert(i interface{}) (interface{}, error) {
	return t.SqlType.Convert(fmt.Sprint(i))
}

var Decimal = &TextType{sql.Text,query.Type_DECIMAL}

type Converter func(src,dst reflect.Value)
type Conversion struct {
	Conv Converter
	Src,Dst reflect.Type
}
func (c Conversion) Slice() (d Conversion) {
	d.Conv = func(src,dst reflect.Value) {
		l := src.Len()
		ds := reflect.MakeSlice(d.Dst,l,l)
		dst.Set(ds)
		for i:=0 ; i<l ; i++ {
			c.Conv(src.Index(i),dst.Index(i))
		}
	}
	d.Src = reflect.SliceOf(c.Src)
	d.Dst = reflect.SliceOf(c.Dst)
	return
}
func stringConverter(src,dst reflect.Value) {
	dst.SetString(fmt.Sprint(src.Interface()))
}
func StringConversion(t reflect.Type) (d Conversion) {
	d.Conv = stringConverter
	d.Dst = reflect.TypeOf("")
	d.Src = t
	return
}
