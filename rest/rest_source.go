//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"

	"github.com/couchbase/cbgt"
)

// ---------------------------------------------------

// SourcePartitionSeqsHandler is a REST handler for retrieving the
// partition seqs from the data source for an index.
type SourcePartitionSeqsHandler struct {
	mgr *cbgt.Manager
}

func NewSourcePartitionSeqsHandler(mgr *cbgt.Manager) *SourcePartitionSeqsHandler {
	return &SourcePartitionSeqsHandler{mgr: mgr}
}

func (h *SourcePartitionSeqsHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index whose partition seqs should be retrieved."
}

func (h *SourcePartitionSeqsHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, "could not retrieve index defs",
			http.StatusInternalServerError)
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

	if indexDef.SourceParams == "" {
		MustEncode(w, nil)
		return
	}

	feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
	if !exists || feedType == nil {
		ShowError(w, req, "unknown source type", http.StatusInternalServerError)
		return
	}

	if feedType.PartitionSeqs == nil {
		MustEncode(w, nil)
		return
	}

	partitionSeqs, err := feedType.PartitionSeqs(
		indexDef.SourceType, indexDef.SourceName, indexDef.SourceUUID,
		indexDef.SourceParams, h.mgr.Server(), h.mgr.Options())
	if err != nil {
		ShowError(w, req, "could not retrieve partition seqs",
			http.StatusInternalServerError)
		return
	}

	MustEncode(w, partitionSeqs)
}

// ---------------------------------------------------

// SourceStatsHandler is a REST handler for retrieving the
// partition seqs from the data source for an index.
type SourceStatsHandler struct {
	mgr *cbgt.Manager
}

func NewSourceStatsHandler(mgr *cbgt.Manager) *SourceStatsHandler {
	return &SourceStatsHandler{mgr: mgr}
}

func (h *SourceStatsHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index whose partition seqs should be retrieved."
	opts["param: statsKind"] =
		"optional, string\n\n" +
			"Optional source-specific string for kind of stats wanted."
}

func (h *SourceStatsHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, "could not retrieve index defs",
			http.StatusInternalServerError)
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

	feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
	if !exists || feedType == nil {
		ShowError(w, req, "unknown source type", http.StatusInternalServerError)
		return
	}

	if feedType.Stats == nil {
		MustEncode(w, nil)
		return
	}

	stats, err := feedType.Stats(
		indexDef.SourceType, indexDef.SourceName, indexDef.SourceUUID,
		indexDef.SourceParams, h.mgr.Server(), h.mgr.Options(),
		req.FormValue("statsKind"))
	if err != nil {
		ShowError(w, req, "could not retrieve stats", http.StatusInternalServerError)
		return
	}

	MustEncode(w, stats)
}
