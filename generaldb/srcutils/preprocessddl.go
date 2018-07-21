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


package srcutils

import "gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
import "strings"
import "regexp"

var findstrs = regexp.MustCompile(`([^']+|'(?:\\.|[^'])*')`)
var sep = regexp.MustCompile(`,\s`)

var replace_column = regexp.MustCompile(`(?i:\s*replace\s+column\s+)(\w+)='([^']*)'`)

func PreprocessTableSpec(ts *sqlparser.TableSpec) {
	options := ts.Options
	var opts []string
	var lastop string
	
	for _,s := range findstrs.FindAllString(options,-1) {
		if strings.HasPrefix(s,"'") {
			lastop += s
		} else {
			for i,e := range sep.Split(s,-1) {
				if i!=0 {
					opts = append(opts,lastop)
					lastop = ""
				}
				lastop += e
			}
		}
	}
	
	opts = append(opts,lastop)
	for _,opt := range opts {
		if sm := replace_column.FindStringSubmatch(opt); len(sm)>0 {
			for _,col := range ts.Columns {
				if col.Name.String()!=sm[1] { continue }
				col.Type.Type = sm[2]
				break
			}
		} else {
		}
	}
	ts.Options = ""
}
