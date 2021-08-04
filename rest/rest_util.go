//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"context"
	"encoding/json"
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

type proxyRequestErr struct {
	msg string
}

func (e *proxyRequestErr) Error() string {
	return e.msg
}

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
			if _, ok := err.(*proxyRequestErr); ok {
				err = fmt.Errorf("proxy request err: %v", err)
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
	hostPortUrl, err := findRebalanceOrchestratorNode(mgr)
	if err != nil {
		return nil, err
	}

	log.Printf("rest_util: identified the rebalance orchestrator node: %s", hostPortUrl)

	// forward the incoming index create/update/delete request if the
	// rebalance orchestrator is a different node in the cluster.
	// create a new url from the raw RequestURI sent by the client
	url, err := cbgt.CBAuthURL(hostPortUrl + req.RequestURI)
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
		return nil, &proxyRequestErr{msg: rv.ErrStr}
	}

	log.Printf("rest_util: index method: %s request successfully forwarded"+
		" to node: %s, resp body: %s", req.Method, hostPortUrl, respBuf)

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
	hostPortUrl string, err error) {
	if mgr == nil {
		return "", fmt.Errorf("rest_util: invalid manager instance")
	}
	nodeDefs, err := mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, true)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resultUrl, bindHttp string
	errCh := make(chan error, len(nodeDefs.NodeDefs))

	for _, nd := range nodeDefs.NodeDefs {
		go func(nodeDef *cbgt.NodeDef) {
			var err error
			defer func() {
				if err != nil {
					err = fmt.Errorf("Rebalance orchestrator fetch failed for "+
						"node: %s, err: %v", nodeDef.HostPort, err)
				}
				errCh <- err
			}()

			hostPortUrl := "http://" + nodeDef.HostPort
			if u, err := nodeDef.HttpsURL(); err == nil {
				hostPortUrl = u
			}

			var url string
			url, err = cbgt.CBAuthURL(hostPortUrl + "/api/ctlmanager")
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
						"failed for node: %s, err: %v", hostPortUrl, err)
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
				resultUrl = hostPortUrl
				bindHttp = nodeDef.HostPort
			}
		}(nd)
	}

	var s []string
	var count int
	for err := range errCh {
		if err != nil {
			s = append(s, err.Error()+", ")
		}
		count++
		if count >= len(nodeDefs.NodeDefs) {
			break
		}
	}

	// found a rebalance orchestrator successfully.
	if resultUrl != "" {
		if mgr.BindHttp() == bindHttp {
			// current node is the rebalance orchestrator.
			return "", errRequestProxyingNotNeeded
		}

		return resultUrl, nil
	}

	// propagate the errors.
	if len(s) > 0 {
		return "", fmt.Errorf("rest_util: findRebalanceOrchestratorNode, "+
			"errs: %d, %#v", len(s), s)
	}

	return "", nil
}
