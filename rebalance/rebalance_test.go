//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package rebalance

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/couchbase/blance"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

func TestRebalance(t *testing.T) {
	testDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testDir)

	nodeDir := func(node string) string {
		d := testDir + string(os.PathSeparator) + node
		os.MkdirAll(d, 0700)
		return d
	}

	var mut sync.Mutex

	httpGets := 0
	httpGet := func(url string) (resp *http.Response, err error) {
		mut.Lock()
		httpGets++
		mut.Unlock()

		return &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewBuffer([]byte("{}"))),
		}, nil
	}

	tests := []struct {
		label       string
		ops         string // Space separated "+a", "-x".
		params      map[string]string
		expNodes    string // Space separated list of nodes ("a"..."v").
		expIndexes  string // Space separated list of indxes ("x"..."z").
		expStartErr bool
	}{
		{"1st node",
			"+a", nil,
			"a",
			"",
			false,
		},
		{"add 1st index x",
			"+x", nil,
			"a",
			"x",
			false,
		},
		{"add 2nd node b",
			"+b", nil,
			"a b",
			"x",
			false,
		},
		{"add 2nd index y",
			"+y", nil,
			"a b",
			"x y",
			false,
		},
		{"remove node b",
			"-b", nil,
			"a",
			"x y",
			false,
		},
	}

	cfg := cbgt.NewCfgMem()

	mgrs := map[string]*cbgt.Manager{}

	var mgr0 *cbgt.Manager

	server := "."

	waitUntilEmptyCfgEvents := func(ch chan cbgt.CfgEvent) {
		for {
			select {
			case <-ch:
			default:
				return
			}
		}
	}

	cfgEventsNodeDefsWanted := make(chan cbgt.CfgEvent, 100)
	cfg.Subscribe(cbgt.NODE_DEFS_WANTED, cfgEventsNodeDefsWanted)

	waitUntilEmptyCfgEventsNodeDefsWanted := func() {
		waitUntilEmptyCfgEvents(cfgEventsNodeDefsWanted)
	}

	cfgEventsIndexDefs := make(chan cbgt.CfgEvent, 100)
	cfg.Subscribe(cbgt.INDEX_DEFS_KEY, cfgEventsIndexDefs)

	waitUntilEmptyCfgEventsIndexDefs := func() {
		waitUntilEmptyCfgEvents(cfgEventsIndexDefs)
	}

	for testi, test := range tests {
		log.Printf("testi: %d, label: %q", testi, test.label)

		checkCurrStatesIndexes := false

		nodesToRemove := []string(nil)

		for opi, op := range strings.Split(test.ops, " ") {
			log.Printf(" opi: %d, op: %s", opi, op)

			name := op[1:2]

			isIndexOp := name >= "x"
			if isIndexOp {
				indexName := name
				log.Printf(" indexOp: %s, indexName: %s", op[0:1], indexName)

				testCreateIndex(t, mgr0, indexName, test.params,
					waitUntilEmptyCfgEventsIndexDefs)

				checkCurrStatesIndexes = false
			} else { // It's a node op.
				nodeName := name
				log.Printf(" nodeOp: %s, nodeName: %s", op[0:1], nodeName)

				register := "wanted"
				if op[0:1] == "-" {
					register = "unknown"
				}
				if test.params["register"] != "" {
					register = test.params["register"]
				}
				if test.params[nodeName+".register"] != "" {
					register = test.params[nodeName+".register"]
				}

				if register == "unknown" {
					nodesToRemove = append(nodesToRemove, nodeName)

					// Delay actual unknown registration / removal
					// until after rebalance finishes.
					continue
				}

				waitUntilEmptyCfgEventsNodeDefsWanted()

				mgr, err := startNodeManager(nodeDir(nodeName),
					cfg, nodeName, register, test.params, server)
				if err != nil || mgr == nil {
					t.Errorf("expected no err, got: %#v", err)
				}
				if mgr0 == nil {
					mgr0 = mgr
				}

				mgrs[nodeName] = mgr

				mgr.Kick("kick")

				waitUntilEmptyCfgEventsNodeDefsWanted()

				checkCurrStatesIndexes = true
			}
		}

		r, err := StartRebalance(cbgt.VERSION, cfg, ".", nil,
			nodesToRemove,
			RebalanceOptions{
				HttpGet:       httpGet,
				SkipSeqChecks: true,
			},
		)
		if (test.expStartErr && err == nil) ||
			(!test.expStartErr && err != nil) {
			t.Errorf("testi: %d, label: %q,"+
				" expStartErr: %v, but got: %v",
				testi, test.label,
				test.expStartErr, err)
		}

		if err != nil || r == nil {
			continue
		}

		progressCh := r.ProgressCh()
		if progressCh == nil {
			t.Errorf("expected progressCh")
		}

		err = nil
		for progress := range progressCh {
			if progress.Error != nil {
				err = progress.Error

				log.Printf("saw progress error: %#v\n", progress)
			}
		}

		r.Stop()

		if err != nil {
			t.Errorf("expected no end err, got: %v", err)
		}

		for _, nodeToRemove := range nodesToRemove {
			if mgrs[nodeToRemove] != nil {
				mgrs[nodeToRemove].Stop()
				delete(mgrs, nodeToRemove)
			}

			waitUntilEmptyCfgEventsNodeDefsWanted()

			mgr, err := startNodeManager(nodeDir(nodeToRemove),
				cfg, nodeToRemove, "unknown", test.params, server)
			if err != nil || mgr == nil {
				t.Errorf("expected no err, got: %#v", err)
			}

			mgr.Kick("kick")

			waitUntilEmptyCfgEventsNodeDefsWanted()

			mgr.Stop()
		}

		endIndexDefs, endNodeDefs, endPlanPIndexes, _, err :=
			cbgt.PlannerGetPlan(cfg, cbgt.VERSION, "")
		if err != nil {
			t.Errorf("expected no err, got: %v", err)
		}

		if endIndexDefs == nil ||
			endNodeDefs == nil ||
			endPlanPIndexes == nil {
			t.Errorf("expected some end defs and plan, got: %v, %v, %v",
				endIndexDefs, endNodeDefs, endPlanPIndexes)
		}

		expNodes := strings.Split(test.expNodes, " ")
		if len(expNodes) != len(endNodeDefs.NodeDefs) {
			t.Errorf("len(expNodes) != len(endNodeDefs.NodeDefs), "+
				" expNodes: %#v, endNodeDefs.NodeDefs: %#v",
				expNodes, endNodeDefs.NodeDefs)
		}

		for _, expNode := range expNodes {
			if endNodeDefs.NodeDefs[expNode] == nil {
				t.Errorf("didn't find expNode: %s,"+
					" expNodes: %#v, endNodeDefs.NodeDefs: %#v",
					expNode, expNodes, endNodeDefs.NodeDefs)
			}
		}

		expIndexes := []string{}
		if len(test.expIndexes) > 0 {
			expIndexes = strings.Split(test.expIndexes, " ")
		}

		r.Visit(func(
			currStates CurrStates,
			currSeqs CurrSeqs,
			wantSeqs WantSeqs,
			nextMoves map[string]*blance.NextMoves) {
			if !checkCurrStatesIndexes {
				return
			}

			if len(currStates) != len(expIndexes) {
				t.Errorf("test.label: %s,"+
					" len(expIndexes) != len(currStates), "+
					" expIndexes: %#v, currStates: %#v, endIndexDefs: %#v",
					test.label, expIndexes, currStates, endIndexDefs)
			}
		})
	}
}

func testCreateIndex(t *testing.T,
	mgr *cbgt.Manager,
	indexName string,
	params map[string]string,
	waitUntilEmptyCfgEventsIndexDefs func()) {
	sourceType := "primary"
	if params["sourceType"] != "" {
		sourceType = params["sourceType"]
	}
	if params[indexName+".sourceType"] != "" {
		sourceType = params[indexName+".sourceType"]
	}

	sourceName := "default"
	if params["sourceName"] != "" {
		sourceName = params["sourceName"]
	}
	if params[indexName+".sourceName"] != "" {
		sourceName = params[indexName+".sourceName"]
	}

	sourceUUID := ""
	if params["sourceUUID"] != "" {
		sourceUUID = params["sourceUUID"]
	}
	if params[indexName+".sourceUUID"] != "" {
		sourceUUID = params[indexName+".sourceUUID"]
	}

	sourceParams := `{"numPartitions":4}`
	if params["sourceParams"] != "" {
		sourceParams = params["sourceParams"]
	}
	if params[indexName+".sourceParams"] != "" {
		sourceParams = params[indexName+".sourceParams"]
	}

	indexType := "blackhole"
	if params["indexType"] != "" {
		indexType = params["indexType"]
	}
	if params[indexName+".indexType"] != "" {
		indexType = params[indexName+".indexType"]
	}

	indexParams := ""
	if params["indexParams"] != "" {
		indexParams = params["indexParams"]
	}
	if params[indexName+".indexParams"] != "" {
		indexParams = params[indexName+".indexParams"]
	}

	prevIndexUUID := ""
	if params["prevIndexUUID"] != "" {
		prevIndexUUID = params["prevIndexUUID"]
	}
	if params[indexName+".prevIndexUUID"] != "" {
		prevIndexUUID = params[indexName+".prevIndexUUID"]
	}

	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 1,
	}

	waitUntilEmptyCfgEventsIndexDefs()

	err := mgr.CreateIndex(
		sourceType, sourceName, sourceUUID, sourceParams,
		indexType, indexName, indexParams,
		planParams,
		prevIndexUUID)
	if err != nil {
		t.Errorf("expected no err, got: %#v", err)
	}

	waitUntilEmptyCfgEventsIndexDefs()
}

func startNodeManager(testDir string, cfg cbgt.Cfg, node, register string,
	params map[string]string, server string) (
	mgr *cbgt.Manager, err error) {
	uuid := node
	if params["uuid"] != "" {
		uuid = params["uuid"]
	}
	if params[node+".uuid"] != "" {
		uuid = params[node+".uuid"]
	}

	// No planner in tags as that will be controlled outside.
	tags := []string{"feed", "pindex", "janitor", "queryer"}
	if params["tags"] != "" {
		tags = strings.Split(params["tags"], ",")
	}
	if params[node+".tags"] != "" {
		tags = strings.Split(params[node+".tags"], ",")
	}

	container := ""
	if params["container"] != "" {
		container = params["container"]
	}
	if params[node+".container"] != "" {
		container = params[node+".container"]
	}

	weight := 1
	if params["weight"] != "" {
		weight, err = strconv.Atoi(params["weight"])
		if err != nil {
			return nil, err
		}
	}
	if params[node+".weight"] != "" {
		weight, err = strconv.Atoi(params[node+".weight"])
		if err != nil {
			return nil, err
		}
	}
	if weight < 1 {
		weight = 1
	}

	extras := ""

	bindHttp := node

	dataDir := testDir + string(os.PathSeparator) + node

	os.MkdirAll(dataDir, 0700)

	meh := cbgt.ManagerEventHandlers(nil)

	mgr = cbgt.NewManager(cbgt.VERSION, cfg, uuid,
		tags, container, weight, extras,
		bindHttp, dataDir, server, meh)

	err = mgr.Start(register)
	if err != nil {
		mgr.Stop()

		return nil, err
	}

	return mgr, nil
}
