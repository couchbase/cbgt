//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
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
	queryParams := req.URL.Query()
	params := queryParams.Get("partitions")
	includeSeqNos := queryParams.Get("seqno")
	if params == "true" {
		writePartitionStatsJSON(h.mgr, w, req, includeSeqNos)
		return
	}

	err := WriteManagerStatsJSON(h.mgr, w, IndexNameLookup(req))
	if err != nil {
		ShowError(w, req, err.Error(), http.StatusInternalServerError)
	}
}

// PartitionStatsWriter represents an advanced stats writer.
type PartitionStatsWriter interface {
	// VbStats would enable ways for providing a lean/trimmed
	// version of index stats in a resource friendly way
	// during operations like rebalance stats monitoring.
	VbStats() bool
	// Verbose hints that whether the caller is interested
	// in detailed index stats like that of the feeds and other
	// implementation specific stats.
	Verbose() bool
	// Returns the index definition of the pindex.
	IndexDef() *cbgt.IndexDef
	// Returns a map of source's partition high sequence numbers.
	SourcePartitionSeqs() map[string]cbgt.UUIDSeq

	Write([]byte) (n int, err error)
}

type statsWriter struct {
	w                   io.Writer
	vbstats             bool
	verbose             bool
	indexDef            *cbgt.IndexDef
	sourcePartitionSeqs map[string]cbgt.UUIDSeq
}

func (s *statsWriter) VbStats() bool {
	return s.vbstats
}

func (s *statsWriter) Verbose() bool {
	return s.verbose
}

func (s *statsWriter) IndexDef() *cbgt.IndexDef {
	return s.indexDef
}

func (s *statsWriter) SourcePartitionSeqs() map[string]cbgt.UUIDSeq {
	return s.sourcePartitionSeqs
}

func (s *statsWriter) Write(b []byte) (int, error) {
	return s.w.Write(b)
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
		if len(buf.Bytes()) == 0 {
			feedStats[feedName] = []byte("{}")
		} else {
			if !json.Valid(buf.Bytes()) {
				return fmt.Errorf("pindex stats err, invalid feedStats json: %v",
					string(buf.Bytes()))
			}
			feedStats[feedName] = buf.Bytes()
		}
	}

	// vbstats stats for rest endpoints or with explicit enabling.
	var vbstats bool
	if _, ok := w.(http.ResponseWriter); ok {
		vbstats = true
	} else if v, ok := mgr.Options()["enableVerboseLogging"]; ok && v == "true" {
		vbstats = true
	}

	// Map of source name -> partition -> UUIDSeq
	sourcePartitionSeqs := make(map[string]map[string]cbgt.UUIDSeq)

	pindexStats := make(map[string][]byte)
	for _, pindexName := range pindexNames {
		pindex := pindexes[pindexName]
		indexDef, _, err := mgr.GetIndexDef(pindex.IndexName, false)
		if err != nil {
			log.Warnf("writePartitionStatsJSON: couldn't obtain index"+
				" definition for pindex: `%s`, err: %v", pindexName, err)
		}

		sourceSeqNos, exists := sourcePartitionSeqs[pindex.SourceName]
		if !exists {
			feedType, _ := cbgt.FeedTypes[cbgt.SOURCE_GOCBCORE]
			partitionSeqs, err := feedType.PartitionSeqs(
				pindex.SourceType, pindex.SourceName, pindex.SourceUUID,
				pindex.SourceParams, mgr.Server(), mgr.Options())
			if err != nil {
				log.Warnf("writePartitionStatsJSON: couldn't obtain source"+
					" stats for pindex: `%s`, err: %v", pindexName, err)
			}
			sourcePartitionSeqs[pindex.SourceName] = partitionSeqs
			sourceSeqNos = partitionSeqs
		}

		var buf bytes.Buffer
		statsWriter := &statsWriter{
			w:                   &buf,
			verbose:             true,
			vbstats:             vbstats,
			indexDef:            indexDef,
			sourcePartitionSeqs: sourceSeqNos,
		}
		err = pindexes[pindexName].Dest.Stats(statsWriter)
		if err != nil {
			return fmt.Errorf("pindex stats err: %v", err)
		}
		if len(buf.Bytes()) == 0 {
			pindexStats[pindexName] = []byte("{}")
		} else {
			if !json.Valid(buf.Bytes()) {
				return fmt.Errorf("pindex stats err, invalid pindexStats json: %v",
					string(buf.Bytes()))
			}
			pindexStats[pindexName] = buf.Bytes()
		}
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

func writePartitionStatsJSON(mgr *cbgt.Manager, w http.ResponseWriter,
	req *http.Request, includeSeqNos string) {
	// includeSeqNos: param on whether to include seq nos or not
	_, pindexes := mgr.CurrentMaps()

	// Map of source name -> partition -> UUIDSeq
	sourcePartitionSeqs := map[string]map[string]cbgt.UUIDSeq{}

	w.Write(cbgt.JsonOpenBrace)
	w.Write(statsPIndexesPrefix)

	var buf bytes.Buffer
	firstEntry := true
	for pindexName, pindex := range pindexes {
		indexDef, _, err := mgr.GetIndexDef(pindex.IndexName, false)
		if err != nil {
			log.Warnf("writePartitionStatsJSON: couldn't obtain index"+
				" definition for pindex: `%s`, err: %v", pindexName, err)
		}

		sourceSeqNos, exists := sourcePartitionSeqs[pindex.SourceName]
		if !exists && includeSeqNos != "false" {
			feedType, _ := cbgt.FeedTypes[cbgt.SOURCE_GOCBCORE]
			partitionSeqs, err := feedType.PartitionSeqs(
				pindex.SourceType, pindex.SourceName, pindex.SourceUUID,
				pindex.SourceParams, mgr.Server(), mgr.Options())
			if err != nil {
				log.Warnf("writePartitionStatsJSON: couldn't obtain source"+
					" stats for pindex: `%s`, err: %v", pindexName, err)
			}
			sourcePartitionSeqs[pindex.SourceName] = partitionSeqs
			sourceSeqNos = partitionSeqs
		}

		if firstEntry {
			firstEntry = false
		} else {
			w.Write(cbgt.JsonComma)
		}

		w.Write(statsNamePrefix)
		w.Write([]byte(pindexName))
		w.Write(statsNameSuffix)

		statsWriter := &statsWriter{
			w:        &buf,
			vbstats:  true,
			indexDef: indexDef,
		}

		if includeSeqNos != "false" {
			statsWriter.sourcePartitionSeqs = sourceSeqNos
		}

		err = pindex.Dest.Stats(statsWriter)
		if err != nil || len(buf.Bytes()) == 0 || !json.Valid(buf.Bytes()) {
			log.Warnf("writePartitionStatsJSON: pindex stats invalid for `%s`, err: %v",
				pindexName, err)
			w.Write([]byte("{}"))
		} else {
			w.Write(buf.Bytes())
		}

		buf.Reset()
	}

	w.Write(cbgt.JsonCloseBrace)
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
	msg := req.FormValue("msg")

	if msg == "planner-force" {
		changed, err := cbgt.Plan(h.mgr.Cfg(), cbgt.CfgGetVersion(h.mgr.Cfg()), "",
			h.mgr.Server(), h.mgr.Options(), nil)
		if err != nil {
			msg = fmt.Sprintf("rest_manage: Plan, err: %v", err)
			PropagateError(w, nil, msg, http.StatusInternalServerError)
			return
		}
		if changed {
			log.Printf("rest_manage: the plans have changed")
		} else {
			log.Printf("rest_manage: no change in the plans")
		}
	} else {
		h.mgr.PlannerKick(msg)
	}

	h.mgr.JanitorKick(msg)

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

// CfgNodeDefsHandler is a REST handler that processes a request for
// the manager/node to set the given NodeDefs contents to the Cfg.
type CfgNodeDefsHandler struct {
	mgr *cbgt.Manager
}

func NewCfgNodeDefsHandler(mgr *cbgt.Manager) *CfgNodeDefsHandler {
	return &CfgNodeDefsHandler{mgr: mgr}
}

func (h *CfgNodeDefsHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		ShowErrorBody(w, nil, fmt.Sprintf("rest_manage:"+
			" could not read request body, err: %v", err), http.StatusBadRequest)
		return
	}
	if len(requestBody) == 0 {
		ShowErrorBody(w, nil, fmt.Sprintf("rest_manage:"+
			" no request body found"), http.StatusBadRequest)
		return
	}
	var defs struct {
		Kind string `json:"kind"`
	}
	err = json.Unmarshal(requestBody, &defs)
	if err != nil {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_manage:"+
			" could not unmarshal request json, err: %v", err), http.StatusBadRequest)
		return
	}
	if defs.Kind != cbgt.NODE_DEFS_KNOWN &&
		defs.Kind != cbgt.NODE_DEFS_WANTED {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_manage:"+
			" unknown nodeDefs kind: %s in request", defs.Kind),
			http.StatusBadRequest)
		return
	}
	nodeDefs := cbgt.NodeDefs{}
	err = json.Unmarshal(requestBody, &nodeDefs)
	if err != nil {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_manage:"+
			" could not unmarshal request json, err: %v", err), http.StatusBadRequest)
		return
	}

	nodeDefs.UUID = cbgt.NewUUID()
	_, err = cbgt.CfgSetNodeDefs(h.mgr.Cfg(), defs.Kind, &nodeDefs, cbgt.CFG_CAS_FORCE)
	if err != nil {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_manage: CfgSetNodeDefs "+
			"failed, err: %v", err), http.StatusBadRequest)
		return
	}

	MustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

// ---------------------------------------------------

// CfgPlanPIndexesHandler is a REST handler that processes a request for
// the manager/node to set the given planPIndexes contents to the Cfg.
type CfgPlanPIndexesHandler struct {
	mgr *cbgt.Manager
}

func NewCfgPlanPIndexesHandler(mgr *cbgt.Manager) *CfgPlanPIndexesHandler {
	return &CfgPlanPIndexesHandler{mgr: mgr}
}

func (h *CfgPlanPIndexesHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		ShowErrorBody(w, nil, fmt.Sprintf("rest_manage:"+
			" could not read request body, err: %v", err), http.StatusBadRequest)
		return
	}
	if len(requestBody) == 0 {
		ShowErrorBody(w, nil, fmt.Sprintf("rest_manage:"+
			" no request body found"), http.StatusBadRequest)
		return
	}

	planPIndexes := &cbgt.PlanPIndexes{}
	err = json.Unmarshal(requestBody, planPIndexes)
	if err != nil {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_manage:"+
			" could not unmarshal request json, err: %v", err), http.StatusBadRequest)
		return
	}

	planPIndexes.UUID = cbgt.NewUUID()
	_, err = cbgt.CfgSetPlanPIndexes(h.mgr.Cfg(), planPIndexes, cbgt.CFG_CAS_FORCE)
	if err != nil {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_manage: CfgSetPlanPIndexes "+
			"failed, err: %v", err), http.StatusBadRequest)
		return
	}

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
	mgr      *cbgt.Manager
	Validate func(opt map[string]string) (map[string]string, error)
}

func NewManagerOptions(mgr *cbgt.Manager) *ManagerOptions {
	return &ManagerOptions{
		mgr:      mgr,
		Validate: nil,
	}
}

func (h *ManagerOptions) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		msg := fmt.Sprintf("rest_manage:"+
			" could not read request body err: %v", err)
		PropagateError(w, nil, msg, http.StatusBadRequest)
		return
	}

	opt := h.mgr.Options()
	newOptions := map[string]string{}
	for k, v := range opt {
		newOptions[k] = v
	}

	err = json.Unmarshal(requestBody, &newOptions)
	if err != nil {
		msg := fmt.Sprintf("rest_manage:"+
			" error in unmarshalling err: %v", err)
		PropagateError(w, requestBody, msg, http.StatusBadRequest)
		return
	}

	if h.Validate != nil {
		newOptions, err = h.Validate(newOptions)
		if err != nil {
			msg := fmt.Sprintf("rest_manage: err: %v", err)
			PropagateError(w, requestBody, msg, http.StatusBadRequest)
			return
		}
	}

	err = cbgt.PublishSystemEvent(cbgt.NewSystemEvent(
		cbgt.SettingsUpdateEventID,
		"info",
		"Manager options updated",
		map[string]interface{}{
			"PrevSettings": opt,
			"NewSettings":  newOptions,
		}))
	if err != nil {
		log.Errorf("rest_manage: unexpected system_event error"+
			" err: %v", err)
	}
	h.mgr.SetOptions(newOptions)
	MustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}
