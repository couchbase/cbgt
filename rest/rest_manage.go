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
	"net/http"
	"sort"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/cbgt"
)

// ---------------------------------------------------

// StatsHandler is a REST handler that provides stats/metrics for a
// node.
type StatsHandler struct {
	mgr *cbgt.Manager
}

func NewStatsHandler(mgr *cbgt.Manager) *StatsHandler {
	return &StatsHandler{mgr: mgr}
}

var statsFeedsPrefix = []byte("\"feeds\":{")
var statsPIndexesPrefix = []byte("\"pindexes\":{")
var statsManagerPrefix = []byte(",\"manager\":")
var statsNamePrefix = []byte("\"")
var statsNameSuffix = []byte("\":")

func (h *StatsHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := mux.Vars(req)["indexName"]

	feeds, pindexes := h.mgr.CurrentMaps()
	feedNames := make([]string, 0, len(feeds))
	for feedName := range feeds {
		feedNames = append(feedNames, feedName)
	}
	sort.Strings(feedNames)

	pindexNames := make([]string, 0, len(pindexes))
	for pindexName := range pindexes {
		pindexNames = append(pindexNames, pindexName)
	}
	sort.Strings(pindexNames)

	w.Write(cbgt.JsonOpenBrace)

	first := true
	w.Write(statsFeedsPrefix)
	for _, feedName := range feedNames {
		if indexName == "" || indexName == feeds[feedName].IndexName() {
			if !first {
				w.Write(cbgt.JsonComma)
			}
			first = false
			w.Write(statsNamePrefix)
			w.Write([]byte(feedName))
			w.Write(statsNameSuffix)
			feeds[feedName].Stats(w)
		}
	}
	w.Write(cbgt.JsonCloseBraceComma)

	first = true
	w.Write(statsPIndexesPrefix)
	for _, pindexName := range pindexNames {
		if indexName == "" || indexName == pindexes[pindexName].IndexName {
			if !first {
				w.Write(cbgt.JsonComma)
			}
			first = false
			w.Write(statsNamePrefix)
			w.Write([]byte(pindexName))
			w.Write(statsNameSuffix)
			pindexes[pindexName].Dest.Stats(w)
		}
	}
	w.Write(cbgt.JsonCloseBrace)

	if indexName == "" {
		w.Write(statsManagerPrefix)
		var mgrStats cbgt.ManagerStats
		h.mgr.StatsCopyTo(&mgrStats)
		mgrStatsJSON, err := json.Marshal(&mgrStats)
		if err == nil && len(mgrStatsJSON) > 0 {
			w.Write(mgrStatsJSON)
		} else {
			w.Write(cbgt.JsonNULL)
		}
	}

	w.Write(cbgt.JsonCloseBrace)
}

// ---------------------------------------------------

// ManagerKickHandler is a REST handler that processes a request to
// kick a manager.
type ManagerKickHandler struct {
	mgr *cbgt.Manager
}

func NewManagerKickHandler(mgr *cbgt.Manager) *ManagerKickHandler {
	return &ManagerKickHandler{mgr: mgr}
}

func (h *ManagerKickHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	h.mgr.Kick(req.FormValue("msg"))
	MustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

// ---------------------------------------------------

type RESTCfg struct {
	Status            string             `json:"status"`
	IndexDefs         *cbgt.IndexDefs    `json:"indexDefs"`
	IndexDefsCAS      uint64             `json:"indexDefsCAS"`
	IndexDefsErr      error              `json:"indexDefsErr"`
	NodeDefsWanted    *cbgt.NodeDefs     `json:"nodeDefsWanted"`
	NodeDefsWantedCAS uint64             `json:"nodeDefsWantedCAS"`
	NodeDefsWantedErr error              `json:"nodeDefsWantedErr"`
	NodeDefsKnown     *cbgt.NodeDefs     `json:"nodeDefsKnown"`
	NodeDefsKnownCAS  uint64             `json:"nodeDefsKnownCAS"`
	NodeDefsKnownErr  error              `json:"nodeDefsKnownErr"`
	PlanPIndexes      *cbgt.PlanPIndexes `json:"planPIndexes"`
	PlanPIndexesCAS   uint64             `json:"planPIndexesCAS"`
	PlanPIndexesErr   error              `json:"planPIndexesErr"`
}

// CfgGetHandler is a REST handler that retrieves the contents of the
// Cfg system.
type CfgGetHandler struct {
	mgr *cbgt.Manager
}

func NewCfgGetHandler(mgr *cbgt.Manager) *CfgGetHandler {
	return &CfgGetHandler{mgr: mgr}
}

func (h *CfgGetHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	// TODO: Might need to scrub auth passwords from this output.
	cfg := h.mgr.Cfg()
	indexDefs, indexDefsCAS, indexDefsErr :=
		cbgt.CfgGetIndexDefs(cfg)
	nodeDefsWanted, nodeDefsWantedCAS, nodeDefsWantedErr :=
		cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	nodeDefsKnown, nodeDefsKnownCAS, nodeDefsKnownErr :=
		cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	planPIndexes, planPIndexesCAS, planPIndexesErr :=
		cbgt.CfgGetPlanPIndexes(cfg)
	MustEncode(w, RESTCfg{
		Status:            "ok",
		IndexDefs:         indexDefs,
		IndexDefsCAS:      indexDefsCAS,
		IndexDefsErr:      indexDefsErr,
		NodeDefsWanted:    nodeDefsWanted,
		NodeDefsWantedCAS: nodeDefsWantedCAS,
		NodeDefsWantedErr: nodeDefsWantedErr,
		NodeDefsKnown:     nodeDefsKnown,
		NodeDefsKnownCAS:  nodeDefsKnownCAS,
		NodeDefsKnownErr:  nodeDefsKnownErr,
		PlanPIndexes:      planPIndexes,
		PlanPIndexesCAS:   planPIndexesCAS,
		PlanPIndexesErr:   planPIndexesErr,
	})
}

// ---------------------------------------------------

// CfgRefreshHandler is a REST handler that processes a request for
// the manager/node to refresh its cached snapshot of the Cfg system
// contents.
type CfgRefreshHandler struct {
	mgr *cbgt.Manager
}

func NewCfgRefreshHandler(mgr *cbgt.Manager) *CfgRefreshHandler {
	return &CfgRefreshHandler{mgr: mgr}
}

func (h *CfgRefreshHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	h.mgr.Cfg().Refresh()
	h.mgr.GetIndexDefs(true)
	h.mgr.GetPlanPIndexes(true)
	MustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}
