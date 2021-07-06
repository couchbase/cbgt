//  Copyright 2014-Present Couchbase, Inc.
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

// ManagerMetaHandler is a REST handler that returns metadata about a
// manager/node.
type ManagerMetaHandler struct {
	mgr  *cbgt.Manager
	meta map[string]RESTMeta
}

func NewManagerMetaHandler(mgr *cbgt.Manager,
	meta map[string]RESTMeta) *ManagerMetaHandler {
	return &ManagerMetaHandler{mgr: mgr, meta: meta}
}

// MetaDesc represents a part of the JSON of a ManagerMetaHandler REST
// response.
type MetaDesc struct {
	Description     string            `json:"description"`
	StartSample     interface{}       `json:"startSample"`
	StartSampleDocs map[string]string `json:"startSampleDocs"`
}

// MetaDescSource represents the source-type/feed-type parts of the
// JSON of a ManagerMetaHandler REST response.
type MetaDescSource MetaDesc

// MetaDescSource represents the index-type parts of
// the JSON of a ManagerMetaHandler REST response.
type MetaDescIndex struct {
	MetaDesc

	CanCount bool `json:"canCount"`
	CanQuery bool `json:"canQuery"`

	QuerySamples interface{} `json:"querySamples"`
	QueryHelp    string      `json:"queryHelp"`

	UI map[string]string `json:"ui"`
}

func (h *ManagerMetaHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	ps := cbgt.IndexPartitionSettings(h.mgr)

	startSamples := map[string]interface{}{
		"planParams": &cbgt.PlanParams{
			MaxPartitionsPerPIndex: ps.MaxPartitionsPerPIndex,
			IndexPartitions:        ps.IndexPartitions,
		},
	}

	// Key is sourceType, value is description.
	sourceTypes := map[string]*MetaDescSource{}
	for sourceType, f := range cbgt.FeedTypes {
		if f.Public {
			sourceTypes[sourceType] = &MetaDescSource{
				Description:     f.Description,
				StartSample:     f.StartSample,
				StartSampleDocs: f.StartSampleDocs,
			}
		}
	}

	// Key is indexType, value is description.
	indexTypes := map[string]*MetaDescIndex{}
	for indexType, t := range cbgt.PIndexImplTypes {
		mdi := &MetaDescIndex{
			MetaDesc: MetaDesc{
				Description: t.Description,
				StartSample: t.StartSample,
			},
			CanCount:  t.Count != nil,
			CanQuery:  t.Query != nil,
			QueryHelp: t.QueryHelp,
			UI:        t.UI,
		}

		if t.QuerySamples != nil {
			mdi.QuerySamples = t.QuerySamples()
		}

		indexTypes[indexType] = mdi
	}

	r := map[string]interface{}{
		"status":       "ok",
		"startSamples": startSamples,
		"sourceTypes":  sourceTypes,
		"indexNameRE":  cbgt.INDEX_NAME_REGEXP,
		"indexTypes":   indexTypes,
		"refREST":      h.meta,
	}

	for _, t := range cbgt.PIndexImplTypes {
		if t.MetaExtra != nil {
			t.MetaExtra(r)
		}
	}

	MustEncode(w, r)
}
