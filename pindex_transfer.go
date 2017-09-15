//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	log "github.com/couchbase/clog"
)

// CopyPIndexRequest represents the PIndex content files copy request
type CopyPIndexRequest struct {
	SourceNode       []*NodeDef // ordered by preference
	SourcePIndexName string
	SourceUUID       string
	CancelCh         chan struct{}
}

// getCopyPIndexRequest returns the optimum pindex copy request.
// Currently this is naive implementation which just checks the existence
// pindexes among the known node lists.
// Most recent/latest sequence calculation is omitted now, since this would
// self heal during the DCP catcup following the file transfer.
// Preference is given to the replica node to leverage those nodes than the
// traffic serving primary node as of now.
// [this need to be revisited once we make the replic's serving traffic]
// The chances of a replica disappearing during the file transfer is minimised
// since the replica's is already a part of the cluster as the node list is
// formed from the KNOWN NODES in Cfg
func getCopyPIndexRequest(pindex string,
	mgr *Manager) (*CopyPIndexRequest, error) {
	planPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
	if err != nil {
		return nil, fmt.Errorf("manager: CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexes == nil {
		return nil, fmt.Errorf("manager: skipped on nil planPIndexes")
	}

	var sourceNodes []*NodeDef
	formerPrimary := ""
	var formerPrimaryNodeDefs *NodeDef

	if planPIndex, exists := planPIndexes.PlanPIndexes[pindex]; exists &&
		len(planPIndex.Nodes) > 1 {
		// get all the potential source nodes except self
		uuids := make([]string, len(planPIndex.Nodes))
		for uuid, node := range planPIndex.Nodes {

			if node.Priority == 0 && node.CanRead && node.CanWrite {
				formerPrimary = uuid
			}

			if mgr.uuid != uuid {
				uuids = append(uuids, uuid)
			}
		}

		sourceUUIDs := getNodesHostingPIndex(uuids, pindex, mgr)
		// during first time index creation,this list would be empty
		if len(sourceUUIDs) > 0 {
			nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg, NODE_DEFS_KNOWN)
			if err != nil {
				return nil, err
			}

			for _, uuid := range sourceUUIDs {
				if node, ok := nodeDefs.NodeDefs[uuid]; ok {
					if formerPrimary != uuid {
						sourceNodes = append(sourceNodes, node)
					} else {
						formerPrimaryNodeDefs = node
					}
				}
			}
			// append the former primary node last in the source Nodes list
			if formerPrimaryNodeDefs != nil && mgr.uuid != formerPrimary {
				sourceNodes = append(sourceNodes, formerPrimaryNodeDefs)
			}

			if len(sourceNodes) > 0 {
				return &CopyPIndexRequest{
					SourceNode:       sourceNodes,
					SourcePIndexName: planPIndex.Name,
					SourceUUID:       planPIndex.UUID,
					CancelCh:         nil,
				}, nil
			}
		}
	}
	return nil, nil
}

func getStatsUrls(uuids []string, mgr *Manager) map[string]string {
	nodeURL := make(map[string]string)
	nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg, NODE_DEFS_KNOWN)
	if err != nil {
		return nodeURL
	}

	urlWithAuth := func(authType, urlStr string) (string, error) {
		if authType == "cbauth" {
			return CBAuthURL(urlStr)
		}
		return urlStr, nil
	}

	authType := mgr.Options()["authType"]
	for _, uuid := range uuids {
		if node, ok := nodeDefs.NodeDefs[uuid]; ok {
			baseURL := "http://" + node.HostPort + "/api/stats"
			url, err := urlWithAuth(authType, baseURL)
			if err != nil {
				log.Printf("manager: auth for query,"+
					" moveURL: %s, authType: %s, err: %v", url, authType, err)
			}
			nodeURL[uuid] = url
		}
	}

	return nodeURL
}

// Check explcitly whether the reuqested pindex resides in those
// nodes, and if so gives back a set of potential source node uuids
func getNodesHostingPIndex(uuids []string,
	pindex string, mgr *Manager) []string {
	urlMap := getStatsUrls(uuids, mgr)
	var wg sync.WaitGroup
	size := len(urlMap)

	type statsReq struct {
		uuid string
		url  string
	}

	type statsResp struct {
		uuid   string
		status bool
		err    error
	}
	client := http.Client{}
	requestCh := make(chan *statsReq, size)
	responseCh := make(chan *statsResp, size)
	nWorkers := getWorkerCount(size)
	// spawn the stats get workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for reqs := range requestCh {
				// To Do
				// hard context time out of 2 secs, need to update the stats endpoint as well
				// may be a hardcodes retry logic as well
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				req, _ := http.NewRequest("GET", reqs.url, nil)
				req = req.WithContext(ctx)

				res, err := client.Do(req)
				if err != nil {
					responseCh <- &statsResp{
						uuid:   reqs.uuid,
						status: false,
						err: fmt.Errorf("manager: getNodesHostingPIndex, get stats for pindex: %s, err: %v",
							reqs.url, err),
					}
					continue
				}
				// parse the response to get the pindex status
				pindexesData := struct {
					PIndexes map[string]struct {
						Partitions map[string]struct {
							UUID string `json:"uuid"`
							Seq  uint64 `json:"seq"`
						} `json:"partitions"`
						Basic struct {
							DocCount uint64 `json:"DocCount"`
						} `json:"basic"`
					} `json:"pindexes"`
				}{}
				data, derr := ioutil.ReadAll(res.Body)
				if err == nil && derr != nil {
					responseCh <- &statsResp{
						uuid:   reqs.uuid,
						status: false,
						err:    derr,
					}
					log.Printf("manager: getNodesHostingPIndex, grab stats for pindex: %s, err: %v",
						reqs.url, err)
					continue
				}
				err = json.Unmarshal(data, &pindexesData)
				if err != nil {
					log.Printf("manager: getNodesHostingPIndex, get stats for pindex: %s, err: %v",
						reqs.url, err)
					continue
				}

				if _, exists := pindexesData.PIndexes[pindex]; exists {
					log.Printf("manager: pindex found on node - %s %s", reqs.url, pindex)
					responseCh <- &statsResp{
						uuid:   reqs.uuid,
						status: true,
						err:    nil,
					}
					continue
				}

				// no pindex found on host
				responseCh <- &statsResp{
					uuid:   reqs.uuid,
					status: false,
					err:    nil,
				}
				continue
			}
			wg.Done()
		}()
	}

	for node, url := range urlMap {
		requestCh <- &statsReq{uuid: node, url: url}
	}
	close(requestCh)
	wg.Wait()
	close(responseCh)

	var UUIDs []string
	for resp := range responseCh {
		if resp.status {
			UUIDs = append(UUIDs, resp.uuid)
		}
	}

	return UUIDs
}
