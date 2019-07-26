//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/couchbase/cbgt"
)

// DiagGetHandler is a REST handler that retrieves diagnostic
// information for a node.
type DiagGetHandler struct {
	versionMain string
	mgr         *cbgt.Manager
	mr          *cbgt.MsgRing
	assetDir    func(name string) ([]string, error)
	asset       func(name string) ([]byte, error)
}

func NewDiagGetHandler(versionMain string,
	mgr *cbgt.Manager, mr *cbgt.MsgRing,
	assetDir func(name string) ([]string, error),
	asset func(name string) ([]byte, error)) *DiagGetHandler {
	return &DiagGetHandler{
		versionMain: versionMain,
		mgr:         mgr,
		mr:          mr,
		assetDir:    assetDir,
		asset:       asset,
	}
}

func (h *DiagGetHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	handlers := []cbgt.DiagHandler{
		{Name: "/api/cfg", Handler: NewCfgGetHandler(h.mgr), HandlerFunc: nil},
		{Name: "/api/index", Handler: NewListIndexHandler(h.mgr), HandlerFunc: nil},
		{Name: "/api/log", Handler: NewLogGetHandler(h.mgr, h.mr),
			HandlerFunc: nil},
		{Name: "/api/manager", Handler: NewManagerHandler(h.mgr), HandlerFunc: nil},
		{Name: "/api/managerMeta", Handler: NewManagerMetaHandler(h.mgr, nil),
			HandlerFunc: nil},
		{Name: "/api/pindex", Handler: NewListPIndexHandler(h.mgr),
			HandlerFunc: nil},
		{Name: "/api/runtime", Handler: NewRuntimeGetHandler(h.versionMain, h.mgr),
			HandlerFunc: nil},
		{Name: "/api/runtime/args", Handler: nil, HandlerFunc: RESTGetRuntimeArgs},
		{Name: "/api/runtime/stats", Handler: nil,
			HandlerFunc: RESTGetRuntimeStats},
		{Name: "/api/runtime/statsMem", Handler: nil,
			HandlerFunc: RESTGetRuntimeStatsMem},
		{Name: "/api/stats", Handler: NewStatsHandler(h.mgr), HandlerFunc: nil},
		{Name: "/debug/pprof/block?debug=1", Handler: nil,
			HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "block", 2)
			}},
		{Name: "/debug/pprof/goroutine?debug=2", Handler: nil,
			HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "goroutine", 2)
			}},
		{Name: "/debug/pprof/heap?debug=1", Handler: nil,
			HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "heap", 1)
			}},
		{Name: "/debug/pprof/threadcreate?debug=1", Handler: nil,
			HandlerFunc: func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "threadcreate", 1)
			}},
	}

	for _, t := range cbgt.PIndexImplTypes {
		for _, h := range t.DiagHandlers {
			handlers = append(handlers, h)
		}
	}

	w.Write(cbgt.JsonOpenBrace)
	for i, handler := range handlers {
		if i > 0 {
			w.Write(cbgt.JsonComma)
		}
		w.Write([]byte(fmt.Sprintf(`"%s":`, handler.Name)))
		if handler.Handler != nil {
			handler.Handler.ServeHTTP(w, req)
		}
		if handler.HandlerFunc != nil {
			handler.HandlerFunc.ServeHTTP(w, req)
		}
	}

	var first = true
	var visit func(path string, f os.FileInfo, err error) error
	visit = func(path string, f os.FileInfo, err error) error {
		if f != nil {
			m := map[string]interface{}{
				"Path":    path,
				"Name":    f.Name(),
				"Size":    f.Size(),
				"Mode":    f.Mode(),
				"ModTime": f.ModTime().Format(time.RFC3339Nano),
				"IsDir":   f.IsDir(),
			}
			if strings.HasPrefix(f.Name(), "PINDEX_") || // Matches PINDEX_xxx_META.
				strings.HasSuffix(f.Name(), "_META") || // Matches PINDEX_META.
				strings.HasSuffix(f.Name(), ".json") { // Matches index_meta.json.
				b, err2 := ioutil.ReadFile(path)
				if err2 == nil {
					m["Contents"] = string(b)
				}
			}
			buf, err := json.Marshal(m)
			if err == nil {
				if !first {
					w.Write(cbgt.JsonComma)
				}
				w.Write(buf)
				first = false
			}
		}
		return err
	}

	w.Write([]byte(`,"dataDir":[`))
	filepath.Walk(h.mgr.DataDir(), visit)
	w.Write([]byte(`]`))

	if h.assetDir != nil {
		entries, err := h.assetDir("staticx/dist")
		if err == nil {
			for _, name := range entries {
				// Ex: "staticx/dist/manifest.txt".
				a, err := h.asset("staticx/dist/" + name)
				if err == nil {
					j, err := json.Marshal(strings.TrimSpace(string(a)))
					if err == nil {
						w.Write([]byte(`,"`))
						w.Write([]byte("/staticx/dist/" + name))
						w.Write([]byte(`":`))
						w.Write(j)
					}
				}
			}
		}
	}

	w.Write(cbgt.JsonCloseBrace)
}

func DiagGetPProf(w http.ResponseWriter, profile string, debug int) {
	var b bytes.Buffer
	pprof.Lookup(profile).WriteTo(&b, debug)
	MustEncode(w, b.String())
}
