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
	"io"
	"io/ioutil"
	"net/http"
	"sort"

	"github.com/couchbase/cbgt"
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
	err := WriteManagerStatsJSON(h.mgr, w, IndexNameLookup(req))
	if err != nil {
		ShowError(w, req, err.Error(), 500)
	}
}

// WriteManagerStatsJSON writes JSON stats for a manager, and is
// optionally focus'able on a particular indexName.
func WriteManagerStatsJSON(mgr *cbgt.Manager, w io.Writer,
	indexName string) error {
	feeds, pindexes := mgr.CurrentMaps()
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

	feedStats := make(map[string][]byte)
	for _, feedName := range feedNames {
		var buf bytes.Buffer
		err := feeds[feedName].Stats(&buf)
		if err != nil {
			return fmt.Errorf("feed stats err: %v", err)
		}
		feedStats[feedName] = buf.Bytes()
	}

	pindexStats := make(map[string][]byte)
	for _, pindexName := range pindexNames {
		var buf bytes.Buffer
		err := pindexes[pindexName].Dest.Stats(&buf)
		if err != nil {
			return fmt.Errorf("pindex stats err: %v", err)
		}
		pindexStats[pindexName] = buf.Bytes()
	}

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
			w.Write(feedStats[feedName])
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
			w.Write(pindexStats[pindexName])
		}
	}
	w.Write(cbgt.JsonCloseBrace)

	if indexName == "" {
		w.Write(statsManagerPrefix)
		var mgrStats cbgt.ManagerStats
		mgr.StatsCopyTo(&mgrStats)
		mgrStatsJSON, err := json.Marshal(&mgrStats)
		if err == nil && len(mgrStatsJSON) > 0 {
			w.Write(mgrStatsJSON)
		} else {
			w.Write(cbgt.JsonNULL)
		}
	}

	w.Write(cbgt.JsonCloseBrace)

	return nil
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
	h.mgr.GetNodeDefs(cbgt.NODE_DEFS_KNOWN, true)
	h.mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, true)
	h.mgr.GetIndexDefs(true)
	h.mgr.GetPlanPIndexes(true)
	MustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

// ---------------------------------------------------

// ManagerHandler is a REST handler that returns runtime config
// information about a manager/node.
type ManagerHandler struct {
	mgr *cbgt.Manager
}

func NewManagerHandler(mgr *cbgt.Manager) *ManagerHandler {
	return &ManagerHandler{mgr: mgr}
}

func (h *ManagerHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	r := map[string]interface{}{
		"status": "ok",
		"mgr": map[string]interface{}{
			"startTime": h.mgr.StartTime(),
			"version":   h.mgr.Version(),
			"uuid":      h.mgr.UUID(),
			"tags":      h.mgr.Tags(),
			"container": h.mgr.Container(),
			"weight":    h.mgr.Weight(),
			"extras":    h.mgr.Extras(),
			"bindHttp":  h.mgr.BindHttp(),
			"dataDir":   h.mgr.DataDir(),
			"server":    h.mgr.Server(),
			"options":   h.mgr.Options(),
		},
	}

	MustEncode(w, r)
}

// ---------------------------------------------------

// ManagerOptions is a REST handler that sets the managerOptions
type ManagerOptions struct {
	mgr *cbgt.Manager
}

func NewManagerOptions(mgr *cbgt.Manager) *ManagerOptions {
	return &ManagerOptions{mgr: mgr}
}

func (h *ManagerOptions) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		msg := fmt.Sprintf("rest_manage:"+
			" could not read request body err: %v", err)
		http.Error(w, msg, 400)
		return
	}

	opt := h.mgr.Options()
	newOptions := map[string]string{}
	for k, v := range opt {
		newOptions[k] = v
	}
	err = json.Unmarshal(requestBody, newOptions)
	if err != nil {
		msg := fmt.Sprintf("rest_manage:"+
			" error in unmarshalling err: %v", err)
		http.Error(w, msg, 400)
		return
	}

	h.mgr.SetOptions(newOptions)
	MustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}
