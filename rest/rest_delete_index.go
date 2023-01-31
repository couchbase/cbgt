//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

// DeleteIndexHandler is a REST handler that processes an index
// deletion request.
type DeleteIndexHandler struct {
	mgr *cbgt.Manager
}

func NewDeleteIndexHandler(mgr *cbgt.Manager) *DeleteIndexHandler {
	return &DeleteIndexHandler{mgr: mgr}
}

func (h *DeleteIndexHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] = "required, string, URL path parameter\n\n" +
		"The name of the index definition to be deleted."
}

func (h *DeleteIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "rest_delete_index: index name is required",
			http.StatusBadRequest)
		return
	}

	ca := req.Header.Get(CLUSTER_ACTION)
	if ca != "orchestrator-forwarded" {
		// defer all error handling to the default flow.
		indexDefs, _, _ := cbgt.CfgGetIndexDefs(h.mgr.Cfg())
		if indexDefs != nil {
			indexDef, _ := indexDefs.IndexDefs[indexName]
			if indexDef != nil && (indexDef.Type == "fulltext-index" ||
				indexDef.Type == "fulltext-alias") {
				// if there was successful proxying of the request to
				// the rebalance orchestrator node, then return early.
				if proxyOrchestratorNodeDone(w, req, h.mgr) {
					return
				}
			}
		}
	}

	log.Printf("rest_delete_index: delete index request received for %v", indexName)
	indexUUID, err := h.mgr.DeleteIndexEx(indexName, "")
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_delete_index:"+
			" error deleting index, err: %v", err), http.StatusBadRequest)
		return
	}

	MustEncode(w, struct {
		Status string `json:"status"`
		UUID   string `json:"uuid"`
	}{
		Status: "ok",
		UUID:   indexUUID,
	})
}
