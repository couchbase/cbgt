//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"net/http"

	"github.com/gorilla/mux"
)

func MuxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

func DocIDLookup(req *http.Request) string {
	return MuxVariableLookup(req, "docID")
}

func IndexNameLookup(req *http.Request) string {
	return MuxVariableLookup(req, "indexName")
}

func PIndexNameLookup(req *http.Request) string {
	return MuxVariableLookup(req, "pindexName")
}
