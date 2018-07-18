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
import "fmt"

type TypeUniverse interface{ IsTypeUniverse() }
type TypeUniverseImpl struct{ N string }
func (*TypeUniverseImpl) IsTypeUniverse(){}
func (t *TypeUniverseImpl) String() string {
	if t.N=="" { return fmt.Sprintf("TypeUniverse(%p)",t) }
	return fmt.Sprintf("TypeUniverse(%s)",t.N)
}

type BackendScan interface{
	sql.RowIter
	
	/*
	Returns a value from the last returned row.
	*/
	GetValue(index int,tu TypeUniverse) (interface{},error)
}
type InstanceOfTable interface{
	GetSupportedOutputs() []TypeUniverse
	
	SetHints([]sql.Expression)
	
	BackendScan(*sql.Context) (BackendScan, error)
}

type BackendTable interface{
	Schema() sql.Schema
	Prepare() (InstanceOfTable,error)
}

type Backend interface{
	GetTable(ns,name string) (BackendTable,error)
}

