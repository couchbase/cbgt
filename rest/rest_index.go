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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

// ListIndexHandler is a REST handler for list indexes.
type ListIndexHandler struct {
	mgr *cbgt.Manager
}

func NewListIndexHandler(mgr *cbgt.Manager) *ListIndexHandler {
	return &ListIndexHandler{mgr: mgr}
}

func (h *ListIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexDefs, _, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, "could not retrieve index defs", http.StatusInternalServerError)
		return
	}

	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	MustEncode(w, rv)
}

// ---------------------------------------------------

// GetIndexHandler is a REST handler for retrieving an index
// definition.
type GetIndexHandler struct {
	mgr *cbgt.Manager
}

func NewGetIndexHandler(mgr *cbgt.Manager) *GetIndexHandler {
	return &GetIndexHandler{mgr: mgr}
}

func (h *GetIndexHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index definition to be retrieved."
}

func (h *GetIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, "could not retrieve index defs", http.StatusInternalServerError)
		return
	}

	indexDef, exists := indexDefsByName[indexName]
	if !exists || indexDef == nil {
		ShowError(w, req, "index not found", http.StatusBadRequest)
		return
	}

	indexUUID := req.FormValue("indexUUID")
	if indexUUID != "" && indexUUID != indexDef.UUID {
		ShowError(w, req, "wrong index UUID", http.StatusBadRequest)
		return
	}

	planPIndexes, planPIndexesByName, err :=
		h.mgr.GetPlanPIndexes(false)
	if err != nil {
		ShowError(w, req,
			fmt.Sprintf("rest_index: GetPlanPIndexes, err: %v",
				err), http.StatusBadRequest)
		return
	}

	planPIndexesForIndex := []*cbgt.PlanPIndex(nil)
	if planPIndexesByName != nil {
		planPIndexesForIndex = planPIndexesByName[indexName]
	}

	planPIndexesWarnings := []string(nil)
	if planPIndexes != nil && planPIndexes.Warnings != nil {
		planPIndexesWarnings = planPIndexes.Warnings[indexName]
	}

	MustEncode(w, struct {
		Status       string             `json:"status"`
		IndexDef     *cbgt.IndexDef     `json:"indexDef"`
		PlanPIndexes []*cbgt.PlanPIndex `json:"planPIndexes"`
		Warnings     []string           `json:"warnings"`
	}{
		Status:       "ok",
		IndexDef:     indexDef,
		PlanPIndexes: planPIndexesForIndex,
		Warnings:     planPIndexesWarnings,
	})
}

// ---------------------------------------------------

// CountHandler is a REST handler for counting documents/entries in an
// index.
type CountHandler struct {
	mgr *cbgt.Manager
}

func NewCountHandler(mgr *cbgt.Manager) *CountHandler {
	return &CountHandler{mgr: mgr}
}

func (h *CountHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index whose count is to be retrieved."
}

func (h *CountHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	pindexImplType, err :=
		cbgt.PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Count == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: Count,"+
			" no pindexImplType, indexName: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	count, err :=
		pindexImplType.Count(h.mgr, indexName, indexUUID)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: Count,"+
			" indexName: %s, err: %v",
			indexName, err), http.StatusInternalServerError)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  count,
	}
	MustEncode(w, rv)
}

// ---------------------------------------------------

// QueryHandler is a REST handler for querying an index.
type QueryHandler struct {
	mgr *cbgt.Manager

	slowQueryLogTimeout time.Duration

	pathStats *RESTPathStats
}

func NewQueryHandler(mgr *cbgt.Manager, pathStats *RESTPathStats) *QueryHandler {
	slowQueryLogTimeout := time.Duration(0)
	slowQueryLogTimeoutV := mgr.Options()["slowQueryLogTimeout"]
	if slowQueryLogTimeoutV != "" {
		d, err := time.ParseDuration(slowQueryLogTimeoutV)
		if err == nil {
			slowQueryLogTimeout = d
		}
	}

	return &QueryHandler{
		mgr:                 mgr,
		slowQueryLogTimeout: slowQueryLogTimeout,
		pathStats:           pathStats,
	}
}

func (h *QueryHandler) RESTOpts(opts map[string]string) {
	indexTypes := []string(nil)
	for indexType, t := range cbgt.PIndexImplTypes {
		if t.QuerySamples != nil {
			s := "For index type ```" + indexType + "```:\n\n"

			for _, sample := range t.QuerySamples() {
				if sample.Text != "" {
					s = s + sample.Text + "\n\n"
				}

				if sample.JSON != nil {
					s = s + "    " +
						cbgt.IndentJSON(sample.JSON, "    ", "  ") +
						"\n"
				}
			}

			indexTypes = append(indexTypes, s)
		}
	}
	sort.Strings(indexTypes)

	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index to be queried."
	opts[""] =
		"The request's POST body depends on the index type:\n\n" +
			strings.Join(indexTypes, "\n")
}

func (h *QueryHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	startTime := time.Now()

	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: Query,"+
			" could not read request body, indexName: %s",
			indexName), http.StatusBadRequest)
		return
	}

	pindexImplType, err :=
		cbgt.PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Query == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: Query,"+
			" no pindexImplType, indexName: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	err = pindexImplType.Query(h.mgr, indexName, indexUUID, requestBody, w)

	if h.slowQueryLogTimeout > time.Duration(0) {
		d := time.Since(startTime)
		if d > h.slowQueryLogTimeout {
			log.Printf("slow-query:"+
				" index: %s, query: %s, duration: %v, err: %v",
				indexName, string(requestBody), d, err)

			if h.pathStats != nil {
				focusStats := h.pathStats.FocusStats(indexName)
				if focusStats != nil {
					atomic.AddUint64(&focusStats.TotRequestSlow, 1)
				}
			}
		}
	}

	if err != nil {
		focusStats := h.pathStats.FocusStats(indexName)
		if focusStats != nil {
			atomic.AddUint64(&focusStats.TotRequestErr, 1)

			if err == cbgt.ErrPIndexQueryTimeout {
				atomic.AddUint64(&focusStats.TotRequestTimeout, 1)
			}
		}

		if showConsistencyError(err, "Query", indexName, requestBody, w, req) {
			return
		}

		ShowError(w, req, fmt.Sprintf("rest_index: Query,"+
			" indexName: %s, requestBody: %s, req: %#v, err: %v",
			indexName, requestBody, req, err), http.StatusBadRequest)
		return
	}
}

// ---------------------------------------------------

// IndexControlHandler is a REST handler for processing admin control
// requests on an index.
type IndexControlHandler struct {
	mgr        *cbgt.Manager
	control    string
	allowedOps map[string]bool
}

func NewIndexControlHandler(mgr *cbgt.Manager, control string,
	allowedOps map[string]bool) *IndexControlHandler {
	return &IndexControlHandler{
		mgr:        mgr,
		control:    control,
		allowedOps: allowedOps,
	}
}

func (h *IndexControlHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index whose control values will be modified."
}

func (h *IndexControlHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	op := MuxVariableLookup(req, "op")
	if !h.allowedOps[op] {
		ShowError(w, req, fmt.Sprintf("rest_index: IndexControl,"+
			" error: unsupported op: %s", op), http.StatusBadRequest)
		return
	}

	err := fmt.Errorf("rest_index: unknown op")
	if h.control == "read" {
		err = h.mgr.IndexControl(indexName, indexUUID, op, "", "")
	} else if h.control == "write" {
		err = h.mgr.IndexControl(indexName, indexUUID, "", op, "")
	} else if h.control == "planFreeze" {
		err = h.mgr.IndexControl(indexName, indexUUID, "", "", op)
	}
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: IndexControl,"+
			" control: %s, could not op: %s, err: %v",
			h.control, op, err), http.StatusBadRequest)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	MustEncode(w, rv)
}

// ------------------------------------------------------------------

// ListPIndexHandler is a REST handler for listing pindexes.
type ListPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewListPIndexHandler(mgr *cbgt.Manager) *ListPIndexHandler {
	return &ListPIndexHandler{mgr: mgr}
}

func (h *ListPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	_, pindexes := h.mgr.CurrentMaps()

	rv := struct {
		Status   string                  `json:"status"`
		PIndexes map[string]*cbgt.PIndex `json:"pindexes"`
	}{
		Status:   "ok",
		PIndexes: pindexes,
	}
	MustEncode(w, rv)
}

// ---------------------------------------------------

// GetPIndexHandler is a REST handler for retrieving information on a
// pindex.
type GetPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewGetPIndexHandler(mgr *cbgt.Manager) *GetPIndexHandler {
	return &GetPIndexHandler{mgr: mgr}
}

func (h *GetPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	pindexName := PIndexNameLookup(req)
	if pindexName == "" {
		ShowError(w, req, "rest_index: pindex name is required", http.StatusBadRequest)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: GetPIndex,"+
			" no pindex, pindexName: %s", pindexName), http.StatusBadRequest)
		return
	}

	MustEncode(w, struct {
		Status string       `json:"status"`
		PIndex *cbgt.PIndex `json:"pindex"`
	}{
		Status: "ok",
		PIndex: pindex,
	})
}

// ---------------------------------------------------

// CountPIndexHandler is a REST handler for counting the
// documents/entries in a pindex.
type CountPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewCountPIndexHandler(mgr *cbgt.Manager) *CountPIndexHandler {
	return &CountPIndexHandler{mgr: mgr}
}

func (h *CountPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	pindexName := PIndexNameLookup(req)
	if pindexName == "" {
		ShowError(w, req, "rest_index: pindex name is required", http.StatusBadRequest)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" no pindex, pindexName: %s", pindexName), http.StatusBadRequest)
		return
	}
	if pindex.Dest == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" no pindex.Dest, pindexName: %s", pindexName), http.StatusBadRequest)
		return
	}

	pindexUUID := req.FormValue("pindexUUID")
	if pindexUUID != "" && pindex.UUID != pindexUUID {
		ShowError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			pindexUUID, pindex.UUID, pindexName), http.StatusBadRequest)
		return
	}

	var cancelCh <-chan bool

	cn, ok := w.(http.CloseNotifier)
	if ok && cn != nil {
		cnc := cn.CloseNotify()
		if cnc != nil {
			cancelCh = cnc
		}
	}

	count, err := pindex.Dest.Count(pindex, cancelCh)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" pindexName: %s, req: %#v, err: %v",
			pindexName, req, err), http.StatusBadRequest)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  count,
	}
	MustEncode(w, rv)
}

// ---------------------------------------------------

// QueryPIndexHandler is a REST handler for querying a pindex.
type QueryPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewQueryPIndexHandler(mgr *cbgt.Manager) *QueryPIndexHandler {
	return &QueryPIndexHandler{mgr: mgr}
}

func (h *QueryPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	pindexName := PIndexNameLookup(req)
	if pindexName == "" {
		ShowError(w, req, "rest_index: pindex name is required", http.StatusBadRequest)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" no pindex, pindexName: %s", pindexName), http.StatusBadRequest)
		return
	}
	if pindex.Dest == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" no pindex.Dest, pindexName: %s", pindexName), http.StatusBadRequest)
		return
	}

	pindexUUID := req.FormValue("pindexUUID")
	if pindexUUID != "" && pindex.UUID != pindexUUID {
		ShowError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			pindexUUID, pindex.UUID, pindexName), http.StatusBadRequest)
		return
	}

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" could not read request body, pindexName: %s",
			pindexName), http.StatusBadRequest)
		return
	}

	var cancelCh <-chan bool

	cn, ok := w.(http.CloseNotifier)
	if ok && cn != nil {
		cnc := cn.CloseNotify()
		if cnc != nil {
			cancelCh = cnc
		}
	}

	err = pindex.Dest.Query(pindex, requestBody, w, cancelCh)
	if err != nil {
		if showConsistencyError(err, "QueryPIndex", pindexName, requestBody, w, req) {
			return
		}

		ShowError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" pindexName: %s, requestBody: %s, req: %#v, err: %v",
			pindexName, requestBody, req, err), http.StatusBadRequest)
		return
	}
}

func showConsistencyError(err error, methodName, itemName string,
	requestBody []byte, w http.ResponseWriter, req *http.Request) bool {
	if errCW, ok := err.(*cbgt.ErrorConsistencyWait); ok {
		rv := struct {
			Status       string              `json:"status"`
			Message      string              `json:"message"`
			StartEndSeqs map[string][]uint64 `json:"startEndSeqs"`
		}{
			Status: errCW.Status,
			Message: fmt.Sprintf("rest_index: %s,"+
				" name: %s, requestBody: %s, req: %#v, err: %v",
				methodName, itemName, requestBody, req, err),
			StartEndSeqs: errCW.StartEndSeqs,
		}
		buf, err := json.Marshal(rv)
		if err == nil && buf != nil {
			ShowError(w, req, string(buf), http.StatusPreconditionFailed)
			return true
		}
	}
	return false
}
