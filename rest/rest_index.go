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
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"os"

	"io"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/ncw/directio"
)

const CLUSTER_ACTION = "Internal-Cluster-Action"
const FTS_SCATTER_GATHER = "fts-scatter/gather"

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

	_, pindexImplType, err := h.mgr.GetIndexDef(indexName, false)
	if err != nil || pindexImplType.Query == nil {
		ShowError(w, req, fmt.Sprintf("rest_index: Query,"+
			" no pindexImplType, indexName: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	err = pindexImplType.Query(h.mgr, indexName, indexUUID, requestBody, w)

	//update the total client queries statistics.
	var focusStats *RESTFocusStats
	if h.pathStats != nil {
		focusStats = h.pathStats.FocusStats(indexName)
	}
	if FTS_SCATTER_GATHER != req.Header.Get(CLUSTER_ACTION) {
		if focusStats != nil {
			atomic.AddUint64(&focusStats.TotClientRequest, 1)

			atomic.AddUint64(&focusStats.TotClientRequestTimeNS,
				uint64(time.Now().Sub(startTime)))
		}
	}

	if h.slowQueryLogTimeout > time.Duration(0) {
		d := time.Since(startTime)
		if d > h.slowQueryLogTimeout {
			log.Printf("slow-query:"+
				" index: %s, query: %s, duration: %v, err: %v",
				indexName, string(requestBody), d, err)
			if focusStats != nil {
				atomic.AddUint64(&focusStats.TotRequestSlow, 1)
			}
		}
	}

	if err != nil {
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

	op := RequestVariableLookup(req, "op")
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

// ---------------------------------------------------

// PIndexLookUpHandler is a REST handler for looking up the
// PIndex for the given index name and document ID.
type PIndexLookUpHandler struct {
	mgr *cbgt.Manager
}

// target pindex details
type targetPIndexDetails struct {
	ID    string `json:"id,omitempty"`
	Error string `json:"error,omitempty"`
}

func NewPIndexLookUpHandler(mgr *cbgt.Manager) *PIndexLookUpHandler {
	return &PIndexLookUpHandler{mgr: mgr}
}

func (h *PIndexLookUpHandler) getTargetPIndex(docID string,
	inDef *cbgt.IndexDef,
	req *http.Request) map[string]*targetPIndexDetails {
	response := make(map[string]*targetPIndexDetails)
	feedType, exists := cbgt.FeedTypes[inDef.SourceType]
	if !exists || feedType == nil {
		response[inDef.Name] = &targetPIndexDetails{
			Error: "Non existing feed type " + inDef.SourceType +
				" for feedname " + inDef.SourceName}
		return response
	}
	if feedType.PartitionLookUp == nil {
		response[inDef.Name] = &targetPIndexDetails{
			Error: "PartitionLookUp operation not supported on feedtype " +
				inDef.SourceType}
		return response
	}
	partitionID, err := feedType.PartitionLookUp(docID, h.mgr.Server(),
		inDef, req)
	if err != nil {
		response[inDef.Name] = &targetPIndexDetails{
			Error: "No feed partition ID found for given index " +
				inDef.Name + " Err " + err.Error()}
		return response
	}
	planPIndexes, _, err := h.mgr.GetPlanPIndexes(true)
	if err != nil {
		response[inDef.Name] = &targetPIndexDetails{
			Error: "Failed GetPlanPIndexes for given index " +
				inDef.Name + " Err " + err.Error()}
		return response
	}
	for _, planPIndex := range planPIndexes.PlanPIndexes {
		if planPIndex.IndexName == inDef.Name {
			sp := strings.Split(planPIndex.SourcePartitions, ",")
			for _, v := range sp {
				if v == partitionID {
					response[inDef.Name] = &targetPIndexDetails{
						ID: planPIndex.Name}
					return response
				}
			}
		}
	}
	response[inDef.Name] =
		&targetPIndexDetails{Error: "No PIndex found for given index " +
			inDef.Name + " and document " + docID}
	return response
}

func (h *PIndexLookUpHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}
	docID := DocIDFromBody(req)
	if docID == "" {
		ShowError(w, req, "document id (docId) is missing ",
			http.StatusBadRequest)
		return
	}
	indexDefn, _, err := h.mgr.GetIndexDef(indexName, true)
	if err != nil {
		ShowError(w, req, err.Error(), http.StatusNotFound)
		return
	}
	response := h.getTargetPIndex(docID, indexDefn, req)
	rv := struct {
		Status   string                          `json:"status"`
		PIndexes map[string]*targetPIndexDetails `json:"pindexes"`
	}{
		Status:   "ok",
		PIndexes: response,
	}
	MustEncode(w, rv)
}

// ---------------------------------------------------------------------

// PIndexMoveHandler is a REST handler for retriving the archived, compressed
// PIndex contents
type PIndexMoveHandler struct {
	mgr *cbgt.Manager
}

// PIndexMoveRequest represent the PIndex move request
type PIndexMoveRequest struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// NewPIndexMoveHandler returns a PIndexMoveHandler
func NewPIndexMoveHandler(mgr *cbgt.Manager) *PIndexMoveHandler {
	return &PIndexMoveHandler{mgr: mgr}
}

func (h *PIndexMoveHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	piName := PIndexNameLookup(req)
	if piName == "" {
		ShowError(w, req, "rest_index: pindex name is required", http.StatusBadRequest)
		return
	}

	pi := h.mgr.GetPIndex(piName)
	if pi == nil {
		ShowError(w, req, "rest_index: no pindex found", http.StatusBadRequest)
		return
	}

	h.sendArchieve(h.mgr.PIndexPath(pi.Name), w, req)
}

func (h *PIndexMoveHandler) sendArchieve(rootPath string,
	w http.ResponseWriter, req *http.Request) {
	_, err := ioutil.ReadDir(rootPath)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: pindex dir read failed, "+
			" err: %+v", err), http.StatusBadRequest)
		return
	}

	size := cbgt.GetDirectorySize(rootPath)
	// Content-Length gets overridden by the "chunked" Transfer-Encoding.
	// Also avoiding Content-Length header since it was preventing  the
	// trailer headers.
	// By default the TE is "chunked"
	w.Header().Set("Content-Size", strconv.Itoa(int(size)))

	// fetch the encoding scheme requested by client
	// default is archived contents in tar form
	encodingScheme := req.Header.Get("Accept-Encoding")
	if strings.Contains(encodingScheme, "gzip") ||
		strings.Contains(encodingScheme, "zlib") ||
		encodingScheme == "" {
		streamTarArchive(rootPath, encodingScheme, w, req)
	} else if strings.Contains(encodingScheme, "zip") {
		streamZipArchive(rootPath, encodingScheme, w, req)
	}

}

func getWriter(scheme string, w http.ResponseWriter) io.WriteCloser {
	if strings.Contains(scheme, "gzip") {
		return gzip.NewWriter(w)
	} else if strings.Contains(scheme, "zlib") {
		return zlib.NewWriter(w)
	}
	return nil
}

func isCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// this can be removed.
func streamZipArchive(rootPath, encodingScheme string,
	w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	hash := md5.New()
	zw := zip.NewWriter(w)
	defer zw.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Encoding", "zip")

	compressionScheme := zip.Store
	if strings.Contains(encodingScheme, "deflate") {
		compressionScheme = zip.Deflate
	}

	buf := make([]byte, 10*1024*1024) // 10 MB?
	// navigate the directory
	filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if isCanceled(ctx) {
			log.Printf("rest_index: streamZipArchive canceled, err: %+v", ctx.Err())
			ShowError(w, req, "rest_index: zip streaming cancelled", http.StatusBadRequest)
			return ctx.Err()
		}

		if info.IsDir() {
			return nil
		}
		// remove base path, convert to forward slash.
		zipPath := path[len(rootPath):]
		zipPath = strings.TrimLeft(strings.Replace(zipPath, `\`, string(filepath.Separator), -1), `/`)

		ze, err := zw.CreateHeader(&zip.FileHeader{
			Name:   zipPath,
			Method: compressionScheme,
		})
		if err != nil {
			log.Printf("rest_index: streamZipArchive, "+
				"cannot create zip entry: %s, err: %+v ", zipPath, err)
			return err
		}

		file, err := directio.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			log.Printf("rest_index: streamZipArchive, "+
				"cannot open file: %s,  err: %+v", path, err)
			return err
		}

		if _, err := writeToStream(ctx, io.MultiWriter(ze, hash), file, buf); err != nil {
			log.Printf("rest_index: streamZipArchive file: %s, err: %+v", file.Name(), err)
			return err
		}
		file.Close()
		return nil
	})
	checkSum := hex.EncodeToString(hash.Sum(nil))
	w.Header().Set("Checksum", checkSum)
}

// refactor this to be a generic exposed utility under misc?
func streamTarArchive(rootPath, encodingScheme string,
	w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Trailer", "Checksum")

	var tw *tar.Writer
	wc := getWriter(encodingScheme, w)
	if wc != nil {
		defer wc.Close()
		tw = tar.NewWriter(wc)
		w.Header().Set("Content-Encoding", encodingScheme)
	} else {
		tw = tar.NewWriter(w)
	}
	defer tw.Close()

	hash := md5.New()
	// hash and archive together
	mw := io.MultiWriter(tw, hash)

	ctx := req.Context()
	buf := directio.AlignedBlock(directio.BlockSize * directio.BlockSize) // ~16 MB

	filepath.Walk(rootPath, func(file string, fi os.FileInfo, err error) error {
		if isCanceled(ctx) {
			log.Printf("rest_index: sendTarArchieve canceled, err: %+v", ctx.Err())
			ShowError(w, req, "rest_index: tar streaming cancelled", http.StatusBadRequest)
		}

		// return on any error
		if err != nil {
			return err
		}

		// return on directories since there will be no content to tar
		if fi.Mode().IsDir() {
			return nil
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		tarPath := file[len(rootPath):]
		tarPath = strings.TrimLeft(strings.Replace(tarPath, `\`, string(filepath.Separator), -1), `/`)
		header.Name = tarPath

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// open files for taring
		f, err := directio.OpenFile(file, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}

		// copy file data into tar writer
		if _, err := writeToStream(ctx, mw, f, buf); err != nil {
			log.Printf("rest_index: writeToStream file: %s, err: %+v", fi.Name(), err)
			return err
		}
		f.Close()
		return nil
	})

	checkSum := hex.EncodeToString(hash.Sum(nil))
	w.Header().Set("Checksum", checkSum)
}

func writeToStream(ctx context.Context, dst io.Writer,
	src io.Reader, buf []byte) (written int64, err error) {
	for {
		if isCanceled(ctx) {
			log.Printf("rest_index: writeToStream, err: %+v", ctx.Err())
			return written, ctx.Err()
		}

		nr, er := io.ReadFull(src, buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF && er != io.ErrUnexpectedEOF {
				err = er
			}
			break
		}
	}

	return written, err
}
