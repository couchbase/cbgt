//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package rest

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/cbgt"
)

func TestMustEncode(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected must encode panic and recover")
		}
	}()
	MustEncode(&bytes.Buffer{}, func() {})
	t.Errorf("expected must encode panic")
}

// Implements ManagerEventHandlers interface.
type TestMEH struct {
	lastPIndex *cbgt.PIndex
	lastCall   string
	ch         chan bool
}

func (meh *TestMEH) OnRegisterPIndex(pindex *cbgt.PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnRegisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func (meh *TestMEH) OnUnregisterPIndex(pindex *cbgt.PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnUnregisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func TestNewRESTRouter(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	ring, err := cbgt.NewMsgRing(nil, 1)

	cfg := cbgt.NewCfgMem()
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	r, meta, err := NewRESTRouter("v0", mgr, emptyDir, "", ring,
		AssetDir, Asset)
	if r == nil || meta == nil || err != nil {
		t.Errorf("expected no errors")
	}

	mgr = cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		[]string{"queryer", "anotherTag"},
		"", 1, "", ":1000", emptyDir, "some-datasource", nil)
	r, meta, err = NewRESTRouter("v0", mgr, emptyDir, "", ring,
		AssetDir, Asset)
	if r == nil || meta == nil || err != nil {
		t.Errorf("expected no errors")
	}
}

type RESTHandlerTest struct {
	Desc          string
	Path          string
	Method        string
	Params        url.Values
	Body          []byte
	Status        int
	ResponseBody  []byte
	ResponseMatch map[string]bool

	Before func()
	After  func()
}

func (test *RESTHandlerTest) check(t *testing.T,
	record *httptest.ResponseRecorder) {
	desc := test.Desc
	if desc == "" {
		desc = test.Path + " " + test.Method
	}

	if got, want := record.Code, test.Status; got != want {
		t.Errorf("%s: response code = %d, want %d", desc, got, want)
		t.Errorf("%s: response body = %s", desc, record.Body)
	}
	got := bytes.TrimRight(record.Body.Bytes(), "\n")
	if test.ResponseBody != nil {
		if !reflect.DeepEqual(got, test.ResponseBody) {
			t.Errorf("%s: expected: '%s', got: '%s'",
				desc, test.ResponseBody, got)
		}
	}
	for pattern, shouldMatch := range test.ResponseMatch {
		didMatch := bytes.Contains(got, []byte(pattern))
		if didMatch != shouldMatch {
			t.Errorf("%s: expected match %t for pattern %s, got %t",
				desc, shouldMatch, pattern, didMatch)
			t.Errorf("%s: response body was: %s", desc, got)
		}
	}
}

func testRESTHandlers(t *testing.T,
	tests []*RESTHandlerTest, router *mux.Router) {
	for _, test := range tests {
		if test.Before != nil {
			test.Before()
		}
		if test.Method != "NOOP" {
			req := &http.Request{
				Method: test.Method,
				URL:    &url.URL{Path: test.Path},
				Form:   test.Params,
				Body:   ioutil.NopCloser(bytes.NewBuffer(test.Body)),
			}
			record := httptest.NewRecorder()
			router.ServeHTTP(record, req)
			test.check(t, record)
		}
		if test.After != nil {
			test.After()
		}
	}
}

func TestHandlersForRuntimeOps(t *testing.T) {
	emptyDir, err := ioutil.TempDir("./tmp", "test")
	if err != nil {
		t.Errorf("tempdir err: %v", err)
	}
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	err = mgr.Start("wanted")
	if err != nil {
		t.Errorf("expected no start err, got: %v", err)
	}

	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr,
		AssetDir, Asset)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Path:   "/api/runtime",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				"arch":       true,
				"go":         true,
				"GOMAXPROCS": true,
				"GOROOT":     true,
			},
		},
		{
			Path:          "/api/runtime/args",
			Method:        "GET",
			Params:        nil,
			Body:          nil,
			Status:        http.StatusOK,
			ResponseMatch: map[string]bool{
			// Actual production args are different from "go test" context.
			},
		},
		{
			Path:         "/api/runtime/gc",
			Method:       "POST",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(nil),
		},
		{
			Path:   "/api/runtime/statsMem",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				"Alloc":      true,
				"TotalAlloc": true,
			},
		},
		{
			Path:   "/api/runtime/profile/cpu",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				"incorrect or missing secs parameter": true,
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersForEmptyManager(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	err := mgr.Start("wanted")
	if err != nil {
		t.Errorf("expected start ok")
	}
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	mgr.AddEvent([]byte(`"fizz"`))
	mgr.AddEvent([]byte(`"buzz"`))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr,
		AssetDir, Asset)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:         "log on empty msg ring",
			Path:         "/api/log",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"messages":["hello","world"],"events":["fizz","buzz"]}`),
		},
		{
			Desc:   "cfg on empty manaager",
			Path:   "/api/cfg",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok"`:       true,
				`"indexDefs":null`:    true,
				`"nodeDefsKnown":{`:   true,
				`"nodeDefsWanted":{`:  true,
				`"planPIndexes":null`: true,
			},
		},
		{
			Desc:   "cfg refresh on empty, unchanged manager",
			Path:   "/api/cfgRefresh",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "manager kick on empty, unchanged manager",
			Path:   "/api/managerKick",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "manager meta",
			Path:   "/api/managerMeta",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok"`:    true,
				`"startSamples":{`: true,
			},
		},
		{
			Desc:   "feed stats when no feeds",
			Path:   "/api/stats",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{`: true,
				`}`: true,
			},
		},
		{
			Desc:         "list empty indexes",
			Path:         "/api/index",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"status":"ok","indexDefs":null}`),
		},
		{
			Desc:         "try to get a nonexistent index",
			Path:         "/api/index/NOT-AN-INDEX",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       400,
			ResponseBody: []byte(`not an index`),
		},
		{
			Desc:   "try to create a default index with no params",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`rest_create_index: indexType is required`: true,
			},
		},
		{
			Desc:   "try to create a default index with no sourceType",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: url.Values{
				"indexType": []string{"no-care"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`rest_create_index: sourceType is required`: true,
			},
		},
		{
			Desc:   "try to create a default index with bad indexType",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"no-care"},
				"sourceType": []string{"couchbase"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`unknown indexType`: true,
			},
		},
		{
			Desc:   "try to delete a nonexistent index when no indexes",
			Path:   "/api/index/NOT-AN-INDEX",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`no indexes`: true,
			},
		},
		{
			Desc:   "try to count a nonexistent index when no indexes",
			Path:   "/api/index/NOT-AN-INDEX/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`could not get indexDefs`: true,
			},
		},
		{
			Desc:   "try to query a nonexistent index when no indexes",
			Path:   "/api/index/NOT-AN-INDEX/query",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`could not get indexDefs`: true,
			},
		},
		{
			Desc:   "create an index with bogus indexType",
			Path:   "/api/index/idxBogusIndexType",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"not-a-real-index-type"},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error`: true,
			},
		},
	}

	testRESTHandlers(t, tests, router)
}
