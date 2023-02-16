//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

var StartTime = time.Now()

// Time in seconds after a client can retry a request that received an error response.
var RetryAfter = "30"

// This function determines which http response codes deem a request as 'retry-able'
// by the client.
func isRetryableError(code int) bool {
	if code >= http.StatusInternalServerError {
		return true
	}
	return false
}

func ShowError(w http.ResponseWriter, req *http.Request, msg string, code int) {
	var requestBody []byte
	if req != nil && req.Body != nil {
		requestBody, _ = io.ReadAll(req.Body)
	}
	ShowErrorBody(w, requestBody, msg, code)
}

func ShowErrorBody(w http.ResponseWriter, requestBody []byte, msg string, code int) {
	log.Errorf("rest: error code: %d, msg: %s", code, msg)
	PropagateError(w, requestBody, msg, code)
}

func PropagateError(w http.ResponseWriter, requestBody []byte, msg string, code int) {
	if isRetryableError(code) {
		w.Header().Set("Retry-After", RetryAfter)
	}

	details := map[string]interface{}{
		"status": "fail",
		"error":  msg,
	}

	if requestBody != nil {
		requestBodyMap := map[string]interface{}{}
		err := json.Unmarshal(requestBody, &requestBodyMap)
		if err != nil {
			details["request"] = fmt.Sprintf("%v", string(requestBody))
		} else {
			details["request"] = requestBodyMap
		}
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		http.Error(w, msg, code)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(code)
		fmt.Fprintln(w, string(detailsJSON))
	}

}

func MustEncode(w io.Writer, i interface{}) {
	rw, rwOk := w.(http.ResponseWriter)
	if rwOk {
		h := rw.Header()
		if h != nil {
			h.Set("Cache-Control", "no-cache")
			if h.Get("Content-Type") == "" {
				h.Set("Content-Type", "application/json")
			}
		}
	}

	err := json.NewEncoder(w).Encode(i)
	if err != nil {
		if rwOk {
			crw, ok := rw.(*CountResponseWriter)
			if ok && crw.Wrote {
				return
			}
			PropagateError(rw, nil, fmt.Sprintf("rest: JSON encode, err: %v", err),
				http.StatusInternalServerError)
		}
	}
}

// -------------------------------------------------------

// MuxVariableLookup() uses gorilla.mux.Vars() to lookup a named
// request parameter from an http.Request.
func MuxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

// RequestVariableLookup function is used to lookup a named request
// parameter or variable from an http.Request.  It defaults to
// MuxVariableLookup, but can be overridden for cases like unit
// testing, etc.
var RequestVariableLookup func(req *http.Request, name string) string = MuxVariableLookup

// -------------------------------------------------------

type docIDHolder struct {
	DocID string `json:"docId"`
}

func DocIDFromBody(req *http.Request) string {
	var in docIDHolder
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&in)
	if err != nil {
		return ""
	}
	return in.DocID
}

// DocIDLookup returns the docID param from a http.Request.
func DocIDLookup(req *http.Request) string {
	return RequestVariableLookup(req, "docID")
}

// IndexNameLookup returns the fully qualified indexName from a http.Request.
func IndexNameLookup(req *http.Request) string {
	return scopedIndexPrefix(req) + RequestVariableLookup(req, "indexName")
}

// PIndexNameLookup returns the pindexName param from a http.Request.
func PIndexNameLookup(req *http.Request) string {
	return RequestVariableLookup(req, "pindexName")
}

// BucketNameLookup returns the bucketName param from a http.Request.
func BucketNameLookup(req *http.Request) string {
	return RequestVariableLookup(req, "bucketName")
}

// ScopeNameLookup returns the bucketName param from a http.Request.
func ScopeNameLookup(req *http.Request) string {
	return RequestVariableLookup(req, "scopeName")
}

// -------------------------------------------------------

// unscopedIndexNameLookup returns the indexName param from a http.Request.
func unscopedIndexNameLookup(req *http.Request) string {
	return RequestVariableLookup(req, "indexName")
}

// scopedIndexPrefix returns the bucketName.scopeName. prefix with details
// obtained from a http.Request, if available.
func scopedIndexPrefix(req *http.Request) string {
	bucketName := BucketNameLookup(req)
	scopeName := ScopeNameLookup(req)
	if len(bucketName) > 0 && len(scopeName) > 0 {
		return bucketName + "." + scopeName + "."
	}

	return ""
}

// -------------------------------------------------------

var pathFocusNameRE = regexp.MustCompile(`{([a-zA-Z]+)}`)

// PathFocusName return the focus name of path spec.  For example,
// given a path spec of "/api/index/{indexName}", the focus name
// result is "indexName".  A focus name of "" is valid.
func PathFocusName(path string) string {
	// Example path: "/api/index/{indexName}".
	// Example path: "/api/index/{indexName}/query".
	a := pathFocusNameRE.FindStringSubmatch(path)
	if len(a) <= 1 {
		return ""
	}
	return a[1]
}

// -------------------------------------------------------

// RESTMeta represents the metadata of a REST API endpoint and is used
// for auto-generated REST API documentation.
type RESTMeta struct {
	Path   string // The path spec, including any optional prefix.
	Method string
	Opts   map[string]string
}

// RESTOpts interface may be optionally implemented by REST API
// handlers to provide even more information for auto-generated REST
// API documentation.
type RESTOpts interface {
	RESTOpts(map[string]string)
}

var RESTMethodOrds = map[string]string{
	"GET":    "0",
	"POST":   "1",
	"PUT":    "2",
	"DELETE": "3",
}

// -------------------------------------------------------

// HandlerWithRESTMeta wrapper associates a http.Handler with
// RESTMeta information.
type HandlerWithRESTMeta struct {
	h        http.Handler
	RESTMeta *RESTMeta

	pathStats *RESTPathStats // May be nil.
	focusName string         // A path focus name, ex: "indexName", "pindexName".
}

func NewHandlerWithRESTMeta(h http.Handler, restMeta *RESTMeta,
	pathStats *RESTPathStats, focusName string) *HandlerWithRESTMeta {
	return &HandlerWithRESTMeta{
		h:         h,
		RESTMeta:  restMeta,
		pathStats: pathStats,
		focusName: focusName,
	}
}

func (h *HandlerWithRESTMeta) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	var focusStats *RESTFocusStats
	var startTime time.Time

	if h.pathStats != nil {
		var focusVal string
		if h.focusName != "" {
			focusVal = RequestVariableLookup(req, h.focusName)
		}
		focusStats = h.pathStats.FocusStats(focusVal)
		atomic.AddUint64(&focusStats.TotRequest, 1)
		startTime = time.Now()
	}

	crw := &CountResponseWriter{ResponseWriter: w}

	h.h.ServeHTTP(crw, req)

	if req.Header.Get(CLUSTER_ACTION) == "" {
		// account for query stats on the co-ordinating node only
		if focusStats != nil {
			atomic.AddUint64(&focusStats.TotRequestTimeNS,
				uint64(time.Now().Sub(startTime)))

			atomic.AddUint64(&focusStats.TotResponseBytes, crw.TotBytesWritten)
		}
	}
}

// -------------------------------------------------------

// RESTPathStats represents the stats for a REST path spec.  A REST
// path spec, like "/api/index" or "/api/index/{indexName}/query", can
// have an optional focusName (i.e., "indexName"), which can in turn
// have multiple runtime focus values, like "beer-sample",
// "indexForCRM", "".
type RESTPathStats struct {
	m sync.Mutex // Protects the fields that follow.

	// Keyed by a focus value, like "beer-sample", "pindex-12343234", "".
	focusStats map[string]*RESTFocusStats
}

// FocusStats returns the RESTFocusStats for a given focus value (like
// "beer-sample"), and is a concurrent safe method.  The returned
// RESTFocusStats should only be accessed via sync/atomic functions.
func (s *RESTPathStats) FocusStats(focusVal string) *RESTFocusStats {
	s.m.Lock()
	if s.focusStats == nil {
		s.focusStats = map[string]*RESTFocusStats{}
	}
	rv, exists := s.focusStats[focusVal]
	if !exists {
		rv = &RESTFocusStats{}
		s.focusStats[focusVal] = rv
	}
	s.m.Unlock()
	return rv
}

// FocusValues returns the focus value strings, like ["beer-sample",
// "indexForCRM", "wikiIndexTitles"].
func (s *RESTPathStats) FocusValues() (rv []string) {
	s.m.Lock()
	for focusVal := range s.focusStats {
		rv = append(rv, focusVal)
	}
	s.m.Unlock()
	return rv
}

func (s *RESTPathStats) ResetFocusStats(focusVal string) {
	s.m.Lock()
	if s.focusStats != nil {
		if _, exists := s.focusStats[focusVal]; exists {
			s.focusStats[focusVal] = &RESTFocusStats{}
		}
	}
	s.m.Unlock()
}

// -------------------------------------------------------

// RESTFocusStats represents stats for a targeted or "focused" REST
// endpoint, like "/api/index/beer-sample/query".
type RESTFocusStats struct {
	TotRequest               uint64
	TotRequestTimeNS         uint64
	TotRequestErr            uint64 `json:"TotRequestErr,omitempty"`
	TotRequestSlow           uint64 `json:"TotRequestSlow,omitempty"`
	TotRequestTimeout        uint64 `json:"TotRequestTimeout,omitempty"`
	TotResponseBytes         uint64 `json:"TotResponseBytes,omitempty"`
	TotClientRequest         uint64
	TotClientRequestTimeNS   uint64
	TotInternalRequest       uint64
	TotInternalRequestTimeNS uint64
}

// AtomicCopyTo copies stats from s to r (from source to result).
func (s *RESTFocusStats) AtomicCopyTo(r *RESTFocusStats) {
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			atomic.StoreUint64(rvefp.(*uint64),
				atomic.LoadUint64(svefp.(*uint64)))
		}
	}
}

// -------------------------------------------------------

// NewRESTRouter creates a mux.Router initialized with the REST API
// and web UI routes.  See also InitStaticRouter and InitRESTRouter if
// you need finer control of the router initialization.
func NewRESTRouter(versionMain string, mgr *cbgt.Manager,
	staticDir, staticETag string, mr *cbgt.MsgRing,
	assetDir func(name string) ([]string, error),
	asset func(name string) ([]byte, error)) (
	*mux.Router, map[string]RESTMeta, error) {
	prefix := mgr.Options()["urlPrefix"]

	r := mux.NewRouter()
	r.StrictSlash(true)

	r = InitStaticRouterEx(r,
		staticDir, staticETag, []string{
			prefix + "/indexes",
			prefix + "/nodes",
			prefix + "/monitor",
			prefix + "/manage",
			prefix + "/logs",
			prefix + "/debug",
		}, nil, mgr)

	return InitRESTRouter(r, versionMain, mgr,
		staticDir, staticETag, mr, assetDir, asset)
}

// InitRESTRouter initializes a mux.Router with REST API routes.
func InitRESTRouter(r *mux.Router, versionMain string,
	mgr *cbgt.Manager, staticDir, staticETag string,
	mr *cbgt.MsgRing,
	assetDir func(name string) ([]string, error),
	asset func(name string) ([]byte, error)) (
	*mux.Router, map[string]RESTMeta, error) {
	return InitRESTRouterEx(r, versionMain, mgr, staticDir,
		staticETag, mr, assetDir, asset, nil)
}

// InitRESTRouter initializes a mux.Router with REST API routes with
// extra option.
func InitRESTRouterEx(r *mux.Router, versionMain string,
	mgr *cbgt.Manager, staticDir, staticETag string,
	mr *cbgt.MsgRing,
	assetDir func(name string) ([]string, error),
	asset func(name string) ([]byte, error),
	options map[string]interface{}) (
	*mux.Router, map[string]RESTMeta, error) {
	var authHandler func(http.Handler) http.Handler

	mapRESTPathStats := map[string]*RESTPathStats{} // Keyed by path spec.

	if options != nil {
		if v, ok := options["auth"]; ok {
			authHandler, ok = v.(func(http.Handler) http.Handler)
			if !ok {
				return nil, nil, fmt.Errorf("rest: auth function invalid")
			}
		}

		if v, ok := options["mapRESTPathStats"]; ok {
			mapRESTPathStats, ok = v.(map[string]*RESTPathStats)
			if !ok {
				return nil, nil, fmt.Errorf("rest: mapRESTPathStats invalid")
			}
		}
	}

	prefix := mgr.Options()["urlPrefix"]

	PIndexTypesInitRouter(r, "manager.before", mgr)

	meta := map[string]RESTMeta{}

	handle := func(path string, method string, h http.Handler,
		opts map[string]string) {
		opts["_path"] = path
		if a, ok := h.(RESTOpts); ok {
			a.RESTOpts(opts)
		}
		prefixPath := prefix + path
		restMeta := RESTMeta{prefixPath, method, opts}
		meta[prefixPath+" "+RESTMethodOrds[method]+method] = restMeta
		h = &HandlerWithRESTMeta{
			h:         h,
			RESTMeta:  &restMeta,
			pathStats: mapRESTPathStats[path],
			focusName: PathFocusName(path),
		}
		if authHandler != nil {
			h = authHandler(h)
		}
		r.Handle(prefixPath, h).Methods(method).Name(prefixPath)
	}

	handle("/api/index", "GET", NewListIndexHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Returns all index definitions as JSON.`,
			"version introduced": "0.0.1",
		})
	handle("/api/bucket/{bucketName}/scope/{scopeName}/index", "GET",
		NewListIndexHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Returns all index definitions for bucketName.scopeName as JSON.`,
			"version introduced": "7.5.0",
		})
	handle("/api/index/{indexName}", "PUT", NewCreateIndexHandler(mgr, false),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Creates/updates an index definition.`,
			"version introduced": "0.0.1",
		})
	handle("/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}", "PUT",
		NewCreateIndexHandler(mgr, true),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Creates/updates an index definition for bucketName.scopeName.indexName.`,
			"version introduced": "7.5.0",
		})
	handle("/api/index/{indexName}", "DELETE", NewDeleteIndexHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Deletes an index definition.`,
			"version introduced": "0.0.1",
		})
	handle("/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}", "DELETE",
		NewDeleteIndexHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Deletes index definition for bucketName.scopeName.indexName.`,
			"version introduced": "7.5.0",
		})
	handle("/api/index/{indexName}", "GET", NewGetIndexHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Returns the definition of an index as JSON.`,
			"version introduced": "0.0.1",
		})
	handle("/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}", "GET",
		NewGetIndexHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index definition",
			"_about":             `Returns the definition of index 'bucketName.scopeName.indexName' as JSON.`,
			"version introduced": "7.5.0",
		})

	if mgr == nil || mgr.TagsMap() == nil || mgr.TagsMap()["queryer"] {
		handle("/api/index/{indexName}/count", "GET",
			NewCountHandler(mgr),
			map[string]string{
				"_category":          "Indexing|Index querying",
				"_about":             `Returns the count of indexed documents.`,
				"version introduced": "0.0.1",
			})
		handle("/api/index/{indexName}/query", "POST",
			NewQueryHandler(mgr,
				mapRESTPathStats["/api/index/{indexName}/query"]),
			map[string]string{
				"_category":          "Indexing|Index querying",
				"_about":             `Queries an index.`,
				"version introduced": "0.2.0",
			})
		handle("/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/query", "POST",
			NewQueryHandler(mgr,
				mapRESTPathStats["/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/query"]),
			map[string]string{
				"_category":          "Indexing|Index querying",
				"_about":             `Queries an index with name bucketName.scopeName.indexName.`,
				"version introduced": "7.5.0",
			})
	}

	handle("/api/index/{indexName}/status", "GET",
		NewIndexStatusHandler(mgr),
		map[string]string{
			"_category":          "Index create status",
			"_about":             `Creation status of an index.`,
			"version introduced": "7.1.0",
		})

	handle("/api/index/{indexName}/planFreezeControl/{op}", "POST",
		NewIndexControlHandler(mgr, "planFreeze", map[string]bool{
			"freeze":   true,
			"unfreeze": true,
		}),
		map[string]string{
			"_category": "Indexing|Index management",
			"_about":    `Freeze the assignment of index partitions to nodes.`,
			"param: op": "required, string, URL path parameter\n\n" +
				`Allowed values for op are "freeze" or "unfreeze".`,
			"version introduced": "0.0.1",
		})
	handle("/api/index/{indexName}/ingestControl/{op}", "POST",
		NewIndexControlHandler(mgr, "write", map[string]bool{
			"pause":  true,
			"resume": true,
		}),
		map[string]string{
			"_category": "Indexing|Index management",
			"_about": `Pause index updates and maintenance (no more
                          ingesting document mutations).`,
			"param: op": "required, string, URL path parameter\n\n" +
				`Allowed values for op are "pause" or "resume".`,
			"version introduced": "0.0.1",
		})
	handle("/api/index/{indexName}/queryControl/{op}", "POST",
		NewIndexControlHandler(mgr, "read", map[string]bool{
			"allow":    true,
			"disallow": true,
		}),
		map[string]string{
			"_category": "Indexing|Index management",
			"_about":    `Disallow queries on an index.`,
			"param: op": "required, string, URL path parameter\n\n" +
				`Allowed values for op are "allow" or "disallow".`,
			"version introduced": "0.0.1",
		})

	if mgr == nil || mgr.TagsMap() == nil || mgr.TagsMap()["pindex"] {
		handle("/api/pindex", "GET",
			NewListPIndexHandler(mgr),
			map[string]string{
				"_category":          "x/Advanced|x/Index partition definition",
				"version introduced": "0.0.1",
			})
		handle("/api/pindex/{pindexName}", "GET",
			NewGetPIndexHandler(mgr),
			map[string]string{
				"_category":          "x/Advanced|x/Index partition definition",
				"version introduced": "0.0.1",
			})
		handle("/api/pindex/{pindexName}/count", "GET",
			NewCountPIndexHandler(mgr),
			map[string]string{
				"_category":          "x/Advanced|x/Index partition querying",
				"version introduced": "0.0.1",
			})
		handle("/api/pindex/{pindexName}/query", "POST",
			NewQueryPIndexHandler(mgr),
			map[string]string{
				"_category":          "x/Advanced|x/Index partition querying",
				"version introduced": "0.2.0",
			})
		handle("/api/index/{indexName}/tasks", "POST",
			NewTaskRequestHandler(mgr),
			map[string]string{
				"_category":          "Indexing|Index level task requests",
				"_about":             `Index level task requests, eg: Compact an index storage.`,
				"version introduced": "7.0.0",
			})
	}
	handle("/api/index/{indexName}/pindexLookup", "POST", NewPIndexLookUpHandler(mgr),
		map[string]string{
			"_category":          "Indexing|PIndex lookup",
			"_about":             `Returns the PIndex ID.`,
			"version introduced": "5.0.0",
		})

	handle("/api/managerOptions", "PUT", NewManagerOptions(mgr),
		map[string]string{
			"_category":          "Node|Node configuration",
			"_about":             "Set the options for the manager",
			"version introduced": "4.2.0",
		})

	handle("/api/cfg", "GET", NewCfgGetHandler(mgr),
		map[string]string{
			"_category": "Node|Node configuration",
			"_about": `Returns the node's current view
                       of the cluster's configuration as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/cfgRefresh", "POST", NewCfgRefreshHandler(mgr),
		map[string]string{
			"_category": "Node|Node configuration",
			"_about": `Requests the node to refresh its configuration
                       from the configuration provider.`,
			"version introduced": "0.0.1",
		})

	handle("/api/cfgNodeDefs", "PUT", NewCfgNodeDefsHandler(mgr),
		map[string]string{
			"_category": "Node|Node configuration",
			"_about": `Sets the given nodeDefs configurations
                       to the Cfg.`,
			"version introduced": "5.0.0",
		})

	handle("/api/cfgPlanPIndexes", "PUT", NewCfgPlanPIndexesHandler(mgr),
		map[string]string{
			"_category": "Plan|Plan configuration",
			"_about": `Sets the given planPIndexes configurations
                       to the Cfg.`,
			"version introduced": "5.5.0",
		})

	handle("/api/log", "GET", NewLogGetHandler(mgr, mr),
		map[string]string{
			"_category": "Node|Node diagnostics",
			"_about": `Returns recent log messages
                       and key events for the node as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/manager", "GET", NewManagerHandler(mgr),
		map[string]string{
			"_category":          "Node|Node configuration",
			"_about":             `Returns runtime config information about this node.`,
			"version introduced": "0.4.0",
		})

	handle("/api/managerKick", "POST", NewManagerKickHandler(mgr),
		map[string]string{
			"_category": "Node|Node configuration",
			"_about": `Forces the node to replan resource assignments
                       (by running the planner, if enabled) and to update
                       its runtime state to reflect the latest plan
                       (by running the janitor, if enabled).`,
			"version introduced": "0.0.1",
		})

	handle("/api/managerMeta", "GET", NewManagerMetaHandler(mgr, meta),
		map[string]string{
			"_category": "Node|Node configuration",
			"_about": `Returns information on the node's capabilities,
                       including available indexing and storage options as JSON,
                       and is intended to help management tools and web UI's
                       to be more dynamically metadata driven.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime", "GET",
		NewRuntimeGetHandler(versionMain, mgr),
		map[string]string{
			"_category": "Node|Node diagnostics",
			"_about": `Returns information on the node's software,
                       such as version strings and slow-changing
                       runtime settings as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime/args", "GET",
		http.HandlerFunc(RESTGetRuntimeArgs),
		map[string]string{
			"_category": "Node|Node diagnostics",
			"_about": `Returns information on the node's command-line,
                       parameters, environment variables and
                       O/S process values as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/diag", "GET",
		NewDiagGetHandler(versionMain, mgr, mr, assetDir, asset),
		map[string]string{
			"_category": "Node|Node diagnostics",
			"_about": `Returns full set of diagnostic information
                        from the node in one shot as JSON.  That is, the
                        /api/diag response will be the union of the responses
                        from the other REST API diagnostic and monitoring
                        endpoints from the node, and is intended to make
                        production support easier.`,
			"version introduced": "0.0.1",
		})

	handle("/api/ping", "GET", &NoopHandler{},
		map[string]string{
			"_category":          "Node|Node diagnostics",
			"_about":             `Returns an empty body as a quick aliveness check.`,
			"version introduced": "5.0.0",
		})

	handle("/api/runtime/gc", "POST",
		http.HandlerFunc(RESTPostRuntimeGC),
		map[string]string{
			"_category":          "Node|Node management",
			"_about":             `Requests the node to perform a GC.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime/profile/cpu", "POST",
		http.HandlerFunc(RESTProfileCPU),
		map[string]string{
			"_category": "Node|Node diagnostics",
			"_about": `Requests the node to capture local
                       cpu usage profiling information.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime/profile/memory", "POST",
		http.HandlerFunc(RESTProfileMemory),
		map[string]string{
			"_category": "Node|Node diagnostics",
			"_about": `Requests the node to capture lcoal
                       memory usage profiling information.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime/stats", "GET",
		http.HandlerFunc(RESTGetRuntimeStats),
		map[string]string{
			"_category": "Node|Node monitoring",
			"_about": `Returns information on the node's
                       low-level runtime stats as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime/statsMem", "GET",
		http.HandlerFunc(RESTGetRuntimeStatsMem),
		map[string]string{
			"_category": "Node|Node monitoring",
			"_about": `Returns information on the node's
                       low-level GC and memory related runtime stats as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/stats", "GET", NewStatsHandler(mgr),
		map[string]string{
			"_category": "Indexing|Index monitoring",
			"_about": `Returns indexing and data related metrics,
                       timings and counters from the node as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/runtime/trace", "POST",
		http.HandlerFunc(RuntimeTrace),
		map[string]string{
			"_category":          "Node|Node diagnostics",
			"_about":             `Requests the node to go trace the program.`,
			"version introduced": "5.0.0",
		})

	// TODO: If we ever implement cluster-wide index stats, we should
	// have it under /api/index/{indexName}/stats GET endpoint.
	//
	handle("/api/stats/index/{indexName}", "GET", NewStatsHandler(mgr),
		map[string]string{
			"_category": "Indexing|Index monitoring",
			"_about": `Returns metrics, timings and counters
                       for a single index from the node as JSON.`,
			"version introduced": "0.0.1",
		})

	handle("/api/stats/sourceStats/{indexName}", "GET",
		NewSourceStatsHandler(mgr),
		map[string]string{
			"_category": "Indexing|Index monitoring",
			"_about": `Returns data source specific stats
                       for an index as JSON.`,
			"version introduced": "4.2.0",
		})

	handle("/api/stats/sourcePartitionSeqs/{indexName}", "GET",
		NewSourcePartitionSeqsHandler(mgr),
		map[string]string{
			"_category": "Indexing|Index monitoring",
			"_about": `Returns data source partiton seqs
                       for an index as JSON.`,
			"version introduced": "4.2.0",
		})

	handle("/api/indexes/source/{bucketName}", "GET",
		NewListIndexesForSourceHandler(mgr),
		map[string]string{
			"_category":          "Indexing|Index monitoring",
			"_about":             `Returns all index names for source as JSON.`,
			"version introduced": "5.6.0",
		})

	PIndexTypesInitRouter(r, "manager.after", mgr)

	return r, meta, nil
}

// PIndexTypesInitRouter initializes a mux.Router with the REST API
// routes provided by registered pindex types.
func PIndexTypesInitRouter(r *mux.Router, phase string,
	mgr *cbgt.Manager) {
	for _, t := range cbgt.PIndexImplTypes {
		if t.InitRouter != nil {
			t.InitRouter(r, phase, mgr)
		}
	}
}

// --------------------------------------------------------

// RuntimeGetHandler is a REST handler for runtime GET endpoint.
type RuntimeGetHandler struct {
	versionMain string
	mgr         *cbgt.Manager
}

func NewRuntimeGetHandler(
	versionMain string, mgr *cbgt.Manager) *RuntimeGetHandler {
	return &RuntimeGetHandler{versionMain: versionMain, mgr: mgr}
}

func (h *RuntimeGetHandler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	MustEncode(w, map[string]interface{}{
		"versionMain": h.versionMain,
		"versionData": h.mgr.Version(),
		"arch":        runtime.GOARCH,
		"os":          runtime.GOOS,
		"numCPU":      runtime.NumCPU(),
		"go": map[string]interface{}{
			"GOMAXPROCS": runtime.GOMAXPROCS(0),
			"GOROOT":     runtime.GOROOT(),
			"version":    runtime.Version(),
			"compiler":   runtime.Compiler,
		},
	})
}

func RESTGetRuntimeArgs(w http.ResponseWriter, r *http.Request) {
	flags := map[string]interface{}{}
	flag.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f.Value
	})

	env := []string(nil)
	for _, e := range os.Environ() {
		if !strings.Contains(e, "PASSWORD") &&
			!strings.Contains(e, "PSWD") &&
			!strings.Contains(e, "AUTH") {
			env = append(env, e)
		}
	}

	groups, groupsErr := os.Getgroups()
	hostname, hostnameErr := os.Hostname()
	user, userErr := user.Current()
	wd, wdErr := os.Getwd()

	MustEncode(w, map[string]interface{}{
		"args":  os.Args,
		"env":   env,
		"flags": flags,
		"process": map[string]interface{}{
			"euid":        os.Geteuid(),
			"gid":         os.Getgid(),
			"groups":      groups,
			"groupsErr":   cbgt.ErrorToString(groupsErr),
			"hostname":    hostname,
			"hostnameErr": cbgt.ErrorToString(hostnameErr),
			"pageSize":    os.Getpagesize(),
			"pid":         os.Getpid(),
			"ppid":        os.Getppid(),
			"user":        user,
			"userErr":     cbgt.ErrorToString(userErr),
			"wd":          wd,
			"wdErr":       cbgt.ErrorToString(wdErr),
		},
	})
}

func RESTPostRuntimeGC(w http.ResponseWriter, r *http.Request) {
	freeOSMemory := false
	restParams := r.URL.Query()
	if val, found := restParams["freeOSMemory"]; found {
		if val[0] == "true" {
			freeOSMemory = true
		}
	}

	if freeOSMemory {
		debug.FreeOSMemory()
	} else {
		runtime.GC()
	}
}

// RuntimeTrace starts a program trace
//
//	curl -X POST http://127.0.0.1:9200/api/runtime/trace -d secs=5
//
// To analyze a profiling...
//
//	go tool trace run-program.trace
func RuntimeTrace(w http.ResponseWriter, r *http.Request) {
	secs, err := strconv.Atoi(r.FormValue("secs"))
	if err != nil || secs <= 0 {
		PropagateError(w, nil, "incorrect or missing secs parameter",
			http.StatusBadRequest)
		return
	}
	fname := "./run-program.trace"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		PropagateError(w, nil, fmt.Sprintf("runtimeTrace:"+
			" couldn't create file: %s, err: %v",
			fname, err), http.StatusInternalServerError)
		return
	}
	log.Printf("runtimeTrace: start, file: %s", fname)
	err = trace.Start(f)
	if err != nil {
		PropagateError(w, nil, fmt.Sprintf("runtimeTrace:"+
			" couldn't start program trace, file: %s, err: %v", fname, err),
			http.StatusInternalServerError)
		return
	}
	go func() {
		time.Sleep(time.Duration(secs) * time.Second)
		trace.Stop()
		f.Close()
		log.Printf("runtimeTrace: end, file: %s", fname)
	}()
	w.WriteHeader(204)
}

// To start a cpu profiling...
//
//	curl -X POST http://127.0.0.1:9090/api/runtime/profile/cpu -d secs=5
//
// To analyze a profiling...
//
//	go tool pprof [program-binary] run-cpu.pprof
func RESTProfileCPU(w http.ResponseWriter, r *http.Request) {
	secs, err := strconv.Atoi(r.FormValue("secs"))
	if err != nil || secs <= 0 {
		PropagateError(w, nil, "incorrect or missing secs parameter",
			http.StatusBadRequest)
		return
	}
	fname := "./run-cpu.pprof"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		PropagateError(w, nil, fmt.Sprintf("profileCPU:"+
			" couldn't create file: %s, err: %v", fname, err),
			http.StatusInternalServerError)
		return
	}
	log.Printf("profileCPU: start, file: %s", fname)
	err = pprof.StartCPUProfile(f)
	if err != nil {
		PropagateError(w, nil, fmt.Sprintf("profileCPU:"+
			" couldn't start CPU profile, file: %s, err: %v", fname, err),
			http.StatusInternalServerError)
		return
	}
	go func() {
		time.Sleep(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
		log.Printf("profileCPU: end, file: %s", fname)
	}()
	w.WriteHeader(204)
}

// To grab a memory profiling...
//
//	curl -X POST http://127.0.0.1:9090/api/runtime/profile/memory
//
// To analyze a profiling...
//
//	go tool pprof [program-binary] run-memory.pprof
func RESTProfileMemory(w http.ResponseWriter, r *http.Request) {
	fname := "./run-memory.pprof"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		PropagateError(w, nil, fmt.Sprintf("profileMemory:"+
			" couldn't create file: %v, err: %v", fname, err),
			http.StatusInternalServerError)
		return
	}
	defer f.Close()
	pprof.WriteHeapProfile(f)
}

func RESTGetRuntimeStatsMem(w http.ResponseWriter, r *http.Request) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	MustEncode(w, memStats)
}

func RESTGetRuntimeStats(w http.ResponseWriter, r *http.Request) {
	MustEncode(w, map[string]interface{}{
		"currTime":  time.Now(),
		"startTime": StartTime,
		"go": map[string]interface{}{
			"numGoroutine":   runtime.NumGoroutine(),
			"numCgoCall":     runtime.NumCgoCall(),
			"memProfileRate": runtime.MemProfileRate,
		},
	})
}

// -------------------------------------------------------

// A CountResponseWriter is a wrapper of an http.ResponseWriter that
// counts the bytes written.
type CountResponseWriter struct {
	ResponseWriter  http.ResponseWriter
	TotBytesWritten uint64
	Wrote           bool
}

func (cw *CountResponseWriter) Header() http.Header {
	return cw.ResponseWriter.Header()
}

func (cw *CountResponseWriter) Write(p []byte) (n int, err error) {
	cw.Wrote = true
	cw.TotBytesWritten += uint64(len(p))
	return cw.ResponseWriter.Write(p)
}

func (cw *CountResponseWriter) WriteHeader(n int) {
	cw.Wrote = true
	cw.ResponseWriter.WriteHeader(n)
}

func (cw *CountResponseWriter) CloseNotify() <-chan bool {
	cn, ok := cw.ResponseWriter.(http.CloseNotifier)
	if ok && cn != nil {
		return cn.CloseNotify()
	}
	return nil
}
