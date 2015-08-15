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

package main

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
)

func TestRunRebalancer(t *testing.T) {
	testDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testDir)

	log.Printf("testDir: %s", testDir)

	tests := []struct {
		label      string
		ops        string // Space separated "+a", "-x".
		params     map[string]string
		expNodes   string // Space separated list of nodes ("a"..."v").
		expIndexes string // Space separated list of indxes ("x"..."z").
		expChanged bool
		expErr     bool
	}{
		{"1st node",
			"+a", nil,
			"a",
			"",
			false, true,
		},
		{"add 1st index x",
			"+x", nil,
			"a",
			"x",
			true, false,
		},
		{"add 2nd node b",
			"+b", nil,
			"a b",
			"x",
			true, false,
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

		for opi, op := range strings.Split(test.ops, " ") {
			log.Printf(" opi: %d, op: %s", opi, op)

			name := op[1:2]

			isIndexOp := name >= "x"
			if isIndexOp {
				indexName := name
				log.Printf(" indexOp: %s, indexName: %s", op[0:1], indexName)

				sourceType := "primary"
				if test.params["sourceType"] != "" {
					sourceType = test.params["sourceType"]
				}
				if test.params[indexName+".sourceType"] != "" {
					sourceType = test.params[indexName+".sourceType"]
				}

				sourceName := "default"
				if test.params["sourceName"] != "" {
					sourceName = test.params["sourceName"]
				}
				if test.params[indexName+".sourceName"] != "" {
					sourceName = test.params[indexName+".sourceName"]
				}

				sourceUUID := ""
				if test.params["sourceUUID"] != "" {
					sourceUUID = test.params["sourceUUID"]
				}
				if test.params[indexName+".sourceUUID"] != "" {
					sourceUUID = test.params[indexName+".sourceUUID"]
				}

				sourceParams := `{"numPartitions":4}`
				if test.params["sourceParams"] != "" {
					sourceParams = test.params["sourceParams"]
				}
				if test.params[indexName+".sourceParams"] != "" {
					sourceParams = test.params[indexName+".sourceParams"]
				}

				indexType := "blackhole"
				if test.params["indexType"] != "" {
					indexType = test.params["indexType"]
				}
				if test.params[indexName+".indexType"] != "" {
					indexType = test.params[indexName+".indexType"]
				}

				indexParams := ""
				if test.params["indexParams"] != "" {
					indexParams = test.params["indexParams"]
				}
				if test.params[indexName+".indexParams"] != "" {
					indexParams = test.params[indexName+".indexParams"]
				}

				prevIndexUUID := ""
				if test.params["prevIndexUUID"] != "" {
					prevIndexUUID = test.params["prevIndexUUID"]
				}
				if test.params[indexName+".prevIndexUUID"] != "" {
					prevIndexUUID = test.params[indexName+".prevIndexUUID"]
				}

				planParams := cbgt.PlanParams{
					MaxPartitionsPerPIndex: 1,
				}

				waitUntilEmptyCfgEventsIndexDefs()

				err := mgr0.CreateIndex(
					sourceType, sourceName, sourceUUID, sourceParams,
					indexType, indexName, indexParams,
					planParams,
					prevIndexUUID)
				if err != nil {
					t.Errorf("expected no err, got: %#v", err)
				}

				waitUntilEmptyCfgEventsIndexDefs()
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

				if mgrs[nodeName] != nil {
					mgrs[nodeName].Stop()
					delete(mgrs, nodeName)
				}

				waitUntilEmptyCfgEventsNodeDefsWanted()

				mgr, err := startNodeManager(testDir, cfg,
					name, register, test.params, server)
				if err != nil || mgr == nil {
					t.Errorf("expected no err, got: %#v", err)
				}
				if mgr0 == nil {
					mgr0 = mgr
				}

				if register != "unknown" {
					mgrs[nodeName] = mgr
				}

				mgr.Kick("kick")

				waitUntilEmptyCfgEventsNodeDefsWanted()
			}
		}

		changed, err := runRebalancer(cbgt.VERSION, cfg, ".")
		if changed != test.expChanged {
			t.Errorf("testi: %d, label: %q,"+
				" expChanged: %v, but got: %v",
				testi, test.label,
				test.expChanged, changed)
		}
		if (test.expErr && err == nil) ||
			(!test.expErr && err != nil) {
			t.Errorf("testi: %d, label: %q,"+
				" expErr: %v, but got: %v",
				testi, test.label,
				test.expErr, err)
		}
	}
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

	// No planner in tags because mcp provides the planner.
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
	}
	if params[node+".weight"] != "" {
		weight, err = strconv.Atoi(params[node+".weight"])
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
