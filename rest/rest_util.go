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
	"io"
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

// RequestProxyStubFunc is mainly for testability.
var RequestProxyStubFunc func() bool

func proxyOrchestratorNodeDone(w http.ResponseWriter,
	req *http.Request, mgr *cbgt.Manager) bool {
	if RequestProxyStubFunc != nil {
		return RequestProxyStubFunc()
	}
	var err error
	var respBuf []byte
	var task string
	cbgt.ExponentialBackoffLoop("proxyOrchestratorNode",
		func() int {
			respBuf, task, err = proxyOrchestratorNode(req, mgr)
			if err == nil {
				if len(respBuf) > 0 {
					w.Write(respBuf)
				}
				// stop the loop in case of no ongoing task or
				// upon successfully proxying the request.
				return -1
			}

			// skip request proxying when not necessary.
			if err == errRequestProxyingNotNeeded {
				log.Printf("rest_create_index: no request forwarding needed as the "+
					"current node is the %s orchestrator", task)
				return -1
			}

			// exit on no retry errors.
			if _, ok := err.(*proxyRequestErr); ok {
				err = fmt.Errorf("proxy request err: %v,task: %s", err, task)
				return -1
			}

			// retry upon finding other errors.
			return 0 // exponential backoff.
		},
		reqBackoffStartSleepMS, reqBackoffFactor, reqBackoffMaxSleepMS)

	if err != nil && err != errRequestProxyingNotNeeded {
		// fail the index CUD operation here as all the repeated attempts
		// to check the task status or proxying the request to
		// the orchestrator node failed, as this could
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

func proxyOrchestratorNode(req *http.Request,
	mgr *cbgt.Manager) ([]byte, string, error) {
	// check whether a rebalance operation is in progress
	// with the local ns-server.
	rebRunning, err := CheckRebalanceStatus(mgr)
	if err != nil {
		return nil, "", err
	}

	// check whether a hibernation operation is in progress and if so, return the
	// hibernation orchestrator and task type
	hibPlanRunning, hibOrchestratorURL, hibernationOp, hibErr :=
		CheckHibernationStatus(mgr)

	// no ongoing operations found.
	if !rebRunning && !hibPlanRunning {
		return nil, "", nil
	}

	operation := "rebalance"
	if hibPlanRunning {
		operation = hibernationOp
	}

	var hostPortUrl string
	if rebRunning {
		log.Printf("rest_util: ongoing rebalance operation found")
		hostPortUrl, err = findRebalanceOrchestratorNode(mgr)
		if err != nil {
			return nil, operation, err
		}
	} else {
		log.Printf("rest_util: ongoing %s plan phase found", hibernationOp)
		if hibErr != nil {
			return nil, operation, hibErr
		}
		hostPortUrl = hibOrchestratorURL
	}

	log.Printf("rest_util: identified the %s orchestrator node: %s", operation, hostPortUrl)

	// forward the incoming index create/update/delete request if the
	// rebalance orchestrator is a different node in the cluster.
	// create a new url from the raw RequestURI sent by the client
	url, err := cbgt.CBAuthURL(hostPortUrl + req.RequestURI)
	if err != nil {
		return nil, operation, err
	}

	proxyReq, err := http.NewRequest(req.Method, url, req.Body)
	if err != nil {
		return nil, operation, err
	}
	req.Header.Add("Content-Type", "application/json")
	// custom header to differentiate between fresh vs forwarded requests.
	proxyReq.Header.Add(CLUSTER_ACTION, "orchestrator-forwarded")

	httpClient := cbgt.HttpClient()
	if httpClient == nil {
		return nil, operation, fmt.Errorf("rest_util: HttpClient unavailable")
	}

	resp, err := httpClient.Do(proxyReq)
	if err != nil {
		return nil, operation, err
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, operation, err
	}
	// propagate any validation failures.
	rv := struct {
		Status string `json:"status"`
		ErrStr string `json:"error"`
	}{}
	err = json.Unmarshal(respBuf, &rv)
	if err != nil {
		return nil, operation, err
	}
	if rv.Status != "ok" {
		return nil, operation, &proxyRequestErr{msg: rv.ErrStr}
	}

	log.Printf("rest_util: index method: %s request successfully forwarded"+
		" to node: %s, resp body: %s", req.Method, hostPortUrl, respBuf)

	return respBuf, operation, nil
}

// Check if the plan phase is ongoing in any of the nodes and
// return the orchestrator and hibernation task type in case it is.
func CheckHibernationStatus(mgr *cbgt.Manager) (bool, string, string, error) {
	if mgr.Options()[cbgt.HIBERNATE_TASK] != "true" &&
		mgr.Options()[cbgt.UNHIBERNATE_TASK] != "true" {
		return false, "", "", nil
	}

	nodeDefs, err := mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, true)
	if err != nil {
		return false, "", "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var planStatus bool
	var resultUrl, bindHttp, taskType string
	errCh := make(chan error, len(nodeDefs.NodeDefs))

	for _, nd := range nodeDefs.NodeDefs {
		go func(nodeDef *cbgt.NodeDef) {
			var err error
			defer func() {
				if err != nil {
					err = fmt.Errorf("Orchestrator fetch failed for "+
						"node: %s, err: %v", nodeDef.HostPort, err)
				}
				errCh <- err
			}()

			hostPortUrl := "http://" + nodeDef.HostPort
			if u, err := nodeDef.HttpsURL(); err == nil {
				hostPortUrl = u
			}

			var url string
			url, err = cbgt.CBAuthURL(hostPortUrl + "/api/hibernationStatus")
			if err != nil {
				return
			}

			var req *http.Request
			req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return
			}

			httpClient := cbgt.HttpClient()
			if httpClient == nil {
				return
			}

			var resp *http.Response
			resp, err = httpClient.Do(req)
			if err != nil || resp.StatusCode != 200 {
				// if the statusCode is HTTP:404 (endpoint not found)
				// then ignore the error as it could happen in a mixed
				// version cluster during upgrade.
				if resp != nil && resp.StatusCode == 404 {
					log.Printf("rest_util: findHibernationStatus "+
						"failed for node: %s, err: %v", hostPortUrl, err)
					err = nil
				}
				return
			}
			defer resp.Body.Close()

			var respBuf []byte
			respBuf, err = io.ReadAll(resp.Body)
			if err != nil {
				return
			}

			rv := struct {
				HibernationPlanStatus bool   `json:"hibernationPlanPhase"`
				HibernationTaskType   string `json:"hibernationTaskType"`
			}{}
			err = json.Unmarshal(respBuf, &rv)
			if err != nil {
				return
			}

			if rv.HibernationPlanStatus {
				resultUrl = hostPortUrl
				bindHttp = nodeDef.HostPort
				planStatus = rv.HibernationPlanStatus
				taskType = rv.HibernationTaskType
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

	// found a hibernation orchestrator successfully.
	if resultUrl != "" {
		if mgr.BindHttp() == bindHttp {
			// current node is the hibernation orchestrator.
			return planStatus, "", taskType, errRequestProxyingNotNeeded
		}

		return planStatus, resultUrl, taskType, nil
	}

	// propagate the errors.
	if len(s) > 0 {
		return false, "", "", fmt.Errorf("rest_util: CheckHibernationStatus, "+
			"errs: %d, %#v", len(s), s)
	}

	return false, "", "", nil
}

type taskProgress struct {
	Type      string `json:"type"`
	StageInfo struct {
		Search struct {
			StartTime     interface{} `json:"startTime,omitempty"`
			CompletedTime interface{} `json:"completedTime,omitempty"`
		} `json:"search"`
	} `json:"stageInfo,omitempty"`
}

func CheckRebalanceStatus(mgr *cbgt.Manager) (bool, error) {
	if mgr == nil {
		return false, fmt.Errorf("rest_util: invalid manager instance")
	}
	tasksURL := mgr.Server() + "/pools/default/tasks"

	url, err := cbgt.CBAuthURL(tasksURL)
	if err != nil {
		return false, fmt.Errorf("rest_util: "+
			" cbauth url: %s, err: %v", tasksURL, err)
	}

	httpClient := cbgt.HttpClient()
	if httpClient == nil {
		return false, fmt.Errorf("rest_util: HttpClient unavailable")
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		return false, fmt.Errorf("rest_util: checkRebalanceStatus, err: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		respBuf, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, fmt.Errorf("rest_util: error reading resp.Body,"+
				" resp: %#v, err: %v", resp, err)
		}

		var res []taskProgress
		err = json.Unmarshal(respBuf, &res)
		if err != nil {
			return false, err
		}

		rebFound := false
		var rebProgress taskProgress
		for _, task := range res {
			if task.Type == "rebalance" {
				rebFound = true
				rebProgress = task
				break
			}
		}

		if !rebFound {
			return false, nil
		}

		taskProgress := rebProgress

		if taskProgress.StageInfo.Search.StartTime == nil ||
			taskProgress.StageInfo.Search.CompletedTime == nil {
			return false, nil
		}

		started, ok := taskProgress.StageInfo.Search.StartTime.(bool)
		// If FTS rebalance has not yet started.
		if ok && started == false {
			return false, nil
		}

		if completedStatus, ok := taskProgress.StageInfo.Search.CompletedTime.(bool); ok {
			if completedStatus == false {
				// Not completed, still in progress.
				return true, nil
			}
		}
		completedTime, err := time.Parse("2006-01-02T15:04:05-07:00",
			taskProgress.StageInfo.Search.CompletedTime.(string))
		if err != nil {
			return false, err
		}
		// If FTS rebalance has already completed/just completed.
		if completedTime.Before(time.Now()) || completedTime.Equal(time.Now()) {
			return false, nil
		}

		return false, nil
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

			httpClient := cbgt.HttpClient()
			if httpClient == nil {
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
			respBuf, err = io.ReadAll(resp.Body)
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
