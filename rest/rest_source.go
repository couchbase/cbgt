//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
		ShowError(w, req, "index name is required", 400)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, "could not retrieve index defs", 500)
		return
	}

	indexDef, exists := indexDefsByName[indexName]
	if !exists || indexDef == nil {
		ShowError(w, req, "index not found", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")
	if indexUUID != "" && indexUUID != indexDef.UUID {
		ShowError(w, req, "wrong index UUID", 400)
		return
	}

	if indexDef.SourceParams == "" {
		MustEncode(w, nil)
		return
	}

	feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
	if !exists || feedType == nil {
		ShowError(w, req, "unknown source type", 500)
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
		ShowError(w, req, "could not retreive partition seqs", 500)
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
		ShowError(w, req, "index name is required", 400)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, "could not retrieve index defs", 500)
		return
	}

	indexDef, exists := indexDefsByName[indexName]
	if !exists || indexDef == nil {
		ShowError(w, req, "index not found", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")
	if indexUUID != "" && indexUUID != indexDef.UUID {
		ShowError(w, req, "wrong index UUID", 400)
		return
	}

	if indexDef.SourceParams == "" {
		MustEncode(w, nil)
		return
	}

	feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
	if !exists || feedType == nil {
		ShowError(w, req, "unknown source type", 500)
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
		ShowError(w, req, "could not retreive stats", 500)
		return
	}

	MustEncode(w, stats)
}
