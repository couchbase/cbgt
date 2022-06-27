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
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

// CreateIndexHandler is a REST handler that processes an index
// creation request.
type CreateIndexHandler struct {
	mgr *cbgt.Manager
}

func NewCreateIndexHandler(mgr *cbgt.Manager) *CreateIndexHandler {
	return &CreateIndexHandler{mgr: mgr}
}

func (h *CreateIndexHandler) RESTOpts(opts map[string]string) {
	indexTypes := []string(nil)
	for indexType, t := range cbgt.PIndexImplTypes {
		indexTypes = append(indexTypes,
			"```"+indexType+"```: "+
				strings.Split(t.Description, " - ")[1])
	}
	sort.Strings(indexTypes)

	indexParams := []string(nil)
	for _, indexDesc := range indexTypes {
		indexType := strings.Split(indexDesc, "```")[1]
		t := cbgt.PIndexImplTypes[indexType]
		if t.StartSample != nil {
			indexParams = append(indexParams,
				"For indexType ```"+indexType+"```"+
					", an example indexParams JSON:\n\n    "+
					cbgt.IndentJSON(t.StartSample, "    ", "  "))
		} else {
			indexParams = append(indexParams,
				"For indexType ```"+indexType+"```"+
					", the indexParams can be null.")
		}
	}

	sourceTypes := []string(nil)
	for sourceType, t := range cbgt.FeedTypes {
		if t.Public {
			sourceTypes = append(sourceTypes,
				"```"+sourceType+"```: "+
					strings.Split(t.Description, " - ")[1])
		}
	}
	sort.Strings(sourceTypes)

	sourceParams := []string(nil)
	for _, sourceDesc := range sourceTypes {
		sourceType := strings.Split(sourceDesc, "```")[1]
		t := cbgt.FeedTypes[sourceType]
		if t.StartSample != nil {
			sourceParams = append(sourceParams,
				"For sourceType ```"+sourceType+"```"+
					", an example sourceParams JSON:\n\n    "+
					cbgt.IndentJSON(t.StartSample, "    ", "  "))
		} else {
			sourceParams = append(sourceParams,
				"For sourceType ```"+sourceType+"```"+
					", the sourceParams can be null.")
		}
	}

	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the to-be-created/updated index definition,\n" +
			"validated with the regular expression of ```" +
			cbgt.INDEX_NAME_REGEXP + "```."
	opts["param: indexType"] =
		"required, string, form parameter\n\n" +
			"Supported indexType's:\n\n* " +
			strings.Join(indexTypes, "\n* ")
	opts["param: indexParams"] =
		"optional (depends on the value of the indexType)," +
			" JSON object, form parameter\n\n" +
			strings.Join(indexParams, "\n\n")
	opts["param: sourceType"] =
		"required, string, form parameter\n\n" +
			"Supported sourceType's:\n\n* " +
			strings.Join(sourceTypes, "\n* ")
	opts["param: sourceName"] =
		"optional, string, form parameter"
	opts["param: sourceUUID"] =
		"optional, string, form parameter"
	opts["param: sourceParams"] =
		"optional (depends on the value of the sourceType)," +
			" JSON object, form parameter\n\n" +
			strings.Join(sourceParams, "\n\n")
	opts["param: planParams"] =
		"optional, JSON object, form parameter"
	opts["param: prevIndexUUID / indexUUID"] =
		"optional, string, form parameter\n\n" +
			"Intended for clients that want to check that they are not " +
			"overwriting the index definition updates of concurrent clients."
	opts["result on error"] =
		`non-200 HTTP error code`
	opts["result on success"] =
		`HTTP 200 with body JSON of {"status": "ok"}` // TODO: Revisit 200 code.
}

func (h *CreateIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	// TODO: Need more input validation (check source UUID's, etc).
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "rest_create_index: index name is required",
			http.StatusBadRequest)
		return
	}

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		ShowErrorBody(w, nil, fmt.Sprintf("rest_create_index:"+
			" could not read request body, indexName: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	indexDef := cbgt.IndexDef{
		PlanParams: cbgt.NewPlanParams(h.mgr),
	}

	if len(requestBody) > 0 {
		err2 := json.Unmarshal(requestBody, &indexDef)
		if err2 != nil {
			ShowErrorBody(w, requestBody, fmt.Sprintf("rest_create_index:"+
				" could not unmarshal json, indexName: %s, err: %v",
				indexName, err2), http.StatusBadRequest)
			return
		}
	}

	indexType := req.FormValue("indexType")
	if indexType == "" {
		indexType = indexDef.Type
	}
	if indexType == "" {
		ShowErrorBody(w, requestBody,
			fmt.Sprintf("rest_create_index: index type is required, indexName: %s",
				indexName), http.StatusBadRequest)
		return
	}

	indexParams := req.FormValue("indexParams")
	if indexParams == "" {
		indexParams = indexDef.Params
	}

	sourceType, sourceName := ExtractSourceTypeName(req, &indexDef, indexName)
	if sourceType == "" {
		ShowErrorBody(w, requestBody,
			fmt.Sprintf("rest_create_index: sourceType is required, indexName: %s",
				indexName), http.StatusBadRequest)
		return
	}

	ca := req.Header.Get(CLUSTER_ACTION)
	if ca != "orchestrator-forwarded" &&
		(indexType == "fulltext-index" || indexType == "fulltext-alias") {
		// populate the body since we already read it.
		req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))
		// if there was successful proxying of the request to the rebalance
		// orchestrator node, then return early.
		if proxyOrchestratorNodeOnRebalanceDone(w, req, h.mgr) {
			return
		}
	}

	sourceUUID := req.FormValue("sourceUUID") // Defaults to "".
	if sourceUUID == "" {
		sourceUUID = indexDef.SourceUUID
	}

	sourceParams := req.FormValue("sourceParams") // Defaults to "".
	if sourceParams == "" {
		sourceParams = indexDef.SourceParams
	}

	planParams := cbgt.NewPlanParams(h.mgr)

	planParamsStr := req.FormValue("planParams")
	if planParamsStr != "" {
		err2 := json.Unmarshal([]byte(planParamsStr), &planParams)
		if err2 != nil {
			ShowErrorBody(w, requestBody, fmt.Sprintf("rest_create_index:"+
				" error parsing planParams: %s, indexName: %s, err: %v",
				planParamsStr, indexName, err2), http.StatusBadRequest)
			return
		}
	} else {
		planParams = indexDef.PlanParams
	}

	if indexType == "fulltext-index" {
		if planParams.IndexPartitions > 0 {
			numSourcePartitions, err := GetNumSourcePartitionsForBucket(h.mgr.Server(), sourceName)
			if err != nil {
				ShowErrorBody(w, requestBody, fmt.Sprintf("rest_create_index:"+
					" error obtaining vbucket count for bucket: %s, err: %v", sourceName, err),
					http.StatusInternalServerError)
				return
			}

			planParams.MaxPartitionsPerPIndex =
				int(math.Ceil(float64(numSourcePartitions) / float64(planParams.IndexPartitions)))

		} else if planParams.MaxPartitionsPerPIndex > 0 {
			numSourcePartitions, err := GetNumSourcePartitionsForBucket(h.mgr.Server(), sourceName)
			if err != nil {
				ShowErrorBody(w, requestBody, fmt.Sprintf("rest_create_index:"+
					" error obtaining vbucket count for bucket: %s, err: %v", sourceName, err),
					http.StatusInternalServerError)
				return
			}

			planParams.IndexPartitions =
				int(math.Ceil(float64(numSourcePartitions) / float64(planParams.MaxPartitionsPerPIndex)))
		}
	}

	prevIndexUUID := req.FormValue("prevIndexUUID")
	if prevIndexUUID == "" {
		prevIndexUUID = req.FormValue("indexUUID")
		if prevIndexUUID == "" {
			prevIndexUUID = indexDef.UUID
		}
	}

	log.Printf("rest_create_index: create index request received for %v", indexName)

	indexUUID, err := h.mgr.CreateIndexEx(sourceType, sourceName,
		sourceUUID, sourceParams,
		indexType, indexName, string(indexParams),
		planParams, prevIndexUUID)
	if err != nil {
		ShowErrorBody(w, requestBody, fmt.Sprintf("rest_create_index:"+
			" error creating index: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	MustEncode(w, struct {
		// TODO: Should return created vs 200 HTTP code?
		Status string `json:"status"`
		Name   string `json:"name"`
		UUID   string `json:"uuid"`
	}{
		Status: "ok",
		Name:   indexDef.Name,
		UUID:   indexUUID,
	})
}

func ExtractSourceTypeName(req *http.Request, indexDef *cbgt.IndexDef, indexName string) (string, string) {
	sourceType := req.FormValue("sourceType")
	if sourceType == "" {
		sourceType = indexDef.SourceType
	}
	sourceName := req.FormValue("sourceName")
	if sourceName == "" {
		sourceName = indexDef.SourceName
	}
	if sourceName == "" {
		// NOTE: Some sourceTypes (like "nil") don't care if sourceName is "".
		if sourceType == cbgt.SOURCE_GOCOUCHBASE ||
			sourceType == cbgt.SOURCE_GOCBCORE {
			// TODO: Revisit default sourceName as indexName.
			sourceName = indexName
		}
	}
	return sourceType, sourceName
}

// -----------------------------------------------------------------------------

type statusType string

const (
	StatusOK   statusType = "ok"
	InProgress            = "InProgress"
	Ready                 = "Ready"
)

// IndexStatusHandler is a REST handler for retrieving
// the index creation status or the readiness of the index
// at a metadata level.
type IndexStatusHandler struct {
	mgr *cbgt.Manager
}

func NewIndexStatusHandler(mgr *cbgt.Manager) *IndexStatusHandler {
	return &IndexStatusHandler{mgr: mgr}
}

func (h *IndexStatusHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := IndexNameLookup(req)
	if indexName == "" {
		ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	nodeDefs, err := h.mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, false)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: GetNodeDefs, err: %v",
			err), http.StatusInternalServerError)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("could not retrieve index defs, err: %v", err),
			http.StatusInternalServerError)
		return
	}

	indexDef, exists := indexDefsByName[indexName]
	if !exists || indexDef == nil {
		ShowError(w, req, "index not found", http.StatusBadRequest)
		return
	}

	_, allPlanPIndexes, err := h.mgr.GetPlanPIndexes(false)
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: GetPlanPIndexes, err: %v",
			err), http.StatusInternalServerError)
		return
	}

	var indexStatus statusType
	planPIndexes, exists := allPlanPIndexes[indexName]
	if !exists || len(planPIndexes) <= 0 {
		indexStatus = InProgress
	}

	_, _, missingPIndexes, err := h.mgr.ClassifyPIndexes(indexName, "",
		planPIndexes, nodeDefs, cbgt.PlanPIndexFilters["canRead"])
	if err != nil {
		ShowError(w, req, fmt.Sprintf("rest_index: ClassifyPIndexes, err: %v",
			err), http.StatusInternalServerError)
		return
	}

	if len(missingPIndexes) > 0 {
		indexStatus = InProgress
	}

	if indexStatus == "" {
		indexStatus = Ready
	}

	rv := struct {
		Status      statusType `json:"status"`
		IndexStatus statusType `json:"indexStatus"`
	}{
		Status:      StatusOK,
		IndexStatus: indexStatus,
	}

	MustEncode(w, rv)
}

// -----------------------------------------------------------------------------

// For unit-test overrideablity
var GetNumSourcePartitionsForBucket = getNumSourcePartitionsForBucket

func getNumSourcePartitionsForBucket(server, bucketName string) (int, error) {
	url := server + "/pools/default/buckets/" + url.QueryEscape(bucketName)

	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, err
	}

	var respErr error
	var respBuf []byte
	max_attempts := 5
	cbgt.ExponentialBackoffLoop(
		"getPoolsDefaultForBucket",
		func() int {
			resp, err := cbgt.HttpClient().Do(req)
			if err == nil {
				defer resp.Body.Close()

				buf, err := ioutil.ReadAll(resp.Body)
				if err == nil && len(buf) > 0 {
					respBuf = buf
					respErr = nil
					return -1
				}
			}

			max_attempts--
			if max_attempts == 0 {
				respErr = fmt.Errorf("couldn't obtain a response from cluster manager")
				return -1
			}

			return 0 // exponential backoff
		},
		reqBackoffStartSleepMS,
		reqBackoffFactor,
		reqBackoffMaxSleepMS,
	)

	if respErr != nil {
		return 0, respErr
	}

	rv := struct {
		NumVBuckets int `json:"numVBuckets"`
	}{}

	if err := json.Unmarshal(respBuf, &rv); err != nil {
		return 0, err
	}

	return rv.NumVBuckets, nil
}
