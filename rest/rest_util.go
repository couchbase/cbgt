//  Copyright (c) 2021 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

var reqBackoffStartSleepMS = 500
var reqBackoffFactor = float32(2)
var reqBackoffMaxSleepMS = 10000
var errRequestProxyingNotNeeded = fmt.Errorf("Current node is the rebalance orchestrator")
var errNoRetryNeeded = fmt.Errorf("Current error doesn't need a request retry")

var httpClient = &http.Client{
	Timeout: time.Second * 20,
}

// RequestProxyStubFunc is mainly for testability.
var RequestProxyStubFunc func() bool

func proxyOrchestratorNodeOnRebalanceDone(w http.ResponseWriter,
	req *http.Request, mgr *cbgt.Manager) bool {
	if RequestProxyStubFunc != nil {
		return RequestProxyStubFunc()
	}
	var err error
	var respBuf []byte
	cbgt.ExponentialBackoffLoop("proxyOrchestratorNodeOnRebalance",
		func() int {
			respBuf, err = proxyOrchestratorNodeOnRebalance(req, mgr)
			if err == nil {
				if len(respBuf) > 0 {
					w.Write(respBuf)
				}
				// stop the loop in case of no ongoing rebalance or
				// upon successfully proxying the request.
				return -1
			}

			// skip request proxying when not necessary.
			if err == errRequestProxyingNotNeeded {
				log.Printf("rest_create_index: no request forwarding needed as the " +
					"current node is the rebalance orchestrator")
				return -1
			}

			// exit on no retry errors.
			if errors.Is(err, errNoRetryNeeded) {
				// unwrap the error before sending to the end user.
				err = errors.Unwrap(errNoRetryNeeded)
				return -1
			}

			// retry upon finding other errors.
			return 0 // exponential backoff.
		},
		reqBackoffStartSleepMS, reqBackoffFactor, reqBackoffMaxSleepMS)

	if err != nil && err != errRequestProxyingNotNeeded {
		// fail the index CUD operation here as all the repeated attempts
		// to check the rebalance status or proxying the request to
		// the rebalance orchestrator node failed, as this could
		// hint a heavily loaded/unhealthy cluster.
		ShowError(w, req,
			fmt.Sprintf("rest_create_index: failed, err: %v", err),
			http.StatusInternalServerError)
		return true
	}

	//  non-empty response was already handled.
	if len(respBuf) > 0 {
		return true
	}

	// no request proxying was done/applicable.
	return false
}

func proxyOrchestratorNodeOnRebalance(req *http.Request,
	mgr *cbgt.Manager) ([]byte, error) {
	// check whether a rebalance operation is in progress
	// with the local ns-server.
	running, err := checkRebalanceStatus(mgr)
	if err != nil {
		return nil, err
	}

	// no ongoing rebalance operations found.
	if !running {
		return nil, nil
	}

	log.Printf("rest_util: ongoing rebalance operation found")
	hostPort, err := findRebalanceOrchestratorNode(mgr)
	if err != nil {
		return nil, err
	}

	log.Printf("rest_util: identified the rebalance orchestrator node: %s", hostPort)
	// current node is the rebalance orchestrator.
	if mgr.BindHttp() == hostPort {
		return nil, errRequestProxyingNotNeeded
	}

	// forward the incoming index create/update/delete request if the
	// rebalance orchestrator is a different node in the cluster.
	// create a new url from the raw RequestURI sent by the client
	url, err := cbgt.CBAuthURL(fmt.Sprintf("%s://%s%s", "http",
		hostPort, req.RequestURI))
	if err != nil {
		return nil, err
	}

	proxyReq, err := http.NewRequest(req.Method, url, req.Body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	// custom header to differentiate between fresh vs forwarded requests.
	proxyReq.Header.Add(CLUSTER_ACTION, "orchestrator-forwarded")

	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// propagate any validation failures.
	rv := struct {
		Status string `json:"status"`
		ErrStr string `json:"error"`
	}{}
	err = json.Unmarshal(respBuf, &rv)
	if err != nil {
		return nil, err
	}
	if rv.Status != "ok" {
		return nil, fmt.Errorf("err: %s, %w", rv.ErrStr, errNoRetryNeeded)
	}

	log.Printf("rest_util: index method: %s request successfully forwarded"+
		" to node: %s, resp body: %s", req.Method, hostPort, respBuf)

	return respBuf, nil
}

func checkRebalanceStatus(mgr *cbgt.Manager) (bool, error) {
	if mgr == nil {
		return false, fmt.Errorf("rest_util: invalid manager instance")
	}
	url, err := cbgt.CBAuthURL(mgr.Server() + "/pools/default/rebalanceProgress")
	if err != nil {
		return false, fmt.Errorf("rest_util: "+
			" cbauth url: %s, err: %v",
			mgr.Server()+"/pools/default/rebalanceProgress", err)
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		return false, fmt.Errorf("rest_util: checkRebalanceStatus, err: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		respBuf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, fmt.Errorf("rest_util: error reading resp.Body,"+
				" resp: %#v, err: %v", resp, err)
		}
		res := struct {
			Status string `json:"status"`
		}{}
		err = json.Unmarshal(respBuf, &res)
		if err != nil {
			return false, err
		}

		if res.Status == "running" {
			return true, nil
		}
	}

	return false, nil
}

func findRebalanceOrchestratorNode(mgr *cbgt.Manager) (
	hostport string, err error) {
	if mgr == nil {
		return "", fmt.Errorf("rest_util: invalid manager instance")
	}
	nodeDefs, err := mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, true)
	if err != nil {
		return "", err
	}

	// get all the node URLs.
	nodeURLs := make([]string, len(nodeDefs.NodeDefs))
	i := 0
	for _, nodeDef := range nodeDefs.NodeDefs {
		nodeURLs[i] = nodeDef.HostPort
		i++
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var result string
	errCh := make(chan error, len(nodeURLs))

	for _, hostPort := range nodeURLs {
		go func(hostPort string) {
			var err error
			defer func() {
				if err != nil {
					err = fmt.Errorf("Rebalance orchestrator fetch failed for "+
						"node: %s, err: %v", hostPort, err)
				}
				errCh <- err
			}()

			var url string
			url, err = cbgt.CBAuthURL("http://" + hostPort + "/api/ctlmanager")
			if err != nil {
				return
			}

			var req *http.Request
			req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return
			}

			var resp *http.Response
			resp, err = httpClient.Do(req)
			if err != nil || resp.StatusCode != 200 {
				// if the statusCode is HTTP:404 (endpoint not found)
				// then ignore the error as it could happen in a mixed
				// version cluster during upgrade.
				if resp != nil && resp.StatusCode == 404 {
					log.Printf("rest_util: findRebalanceOrchestratorNode "+
						"failed for node: %s, err: %v", hostPort, err)
					err = nil
				}
				return
			}
			defer resp.Body.Close()

			var respBuf []byte
			respBuf, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}

			rv := struct {
				Orchestrator bool `json:"orchestrator"`
			}{}
			err = json.Unmarshal(respBuf, &rv)
			if err != nil {
				return
			}

			if rv.Orchestrator {
				result = hostPort
			}
		}(hostPort)

	}

	var s []string
	var count int
	for err := range errCh {
		if err != nil {
			s = append(s, err.Error()+", ")
		}
		count++
		if count >= len(nodeURLs) {
			break
		}
	}

	// found a rebalance orchestrator successfully.
	if result != "" {
		return result, nil
	}

	// propagate the errors.
	if len(s) > 0 {
		return "", fmt.Errorf("rest_util: findRebalanceOrchestratorNode, "+
			"errs: %d, %#v", len(s), s)
	}

	return "", nil
}
