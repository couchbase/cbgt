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
	}{
		{"1st node",
			"+a", nil,
			"a",
			"",
		},
		{"add 1st index x",
			"+x", nil,
			"a",
			"x",
		},
		{"add 2nd node b",
			"+b", nil,
			"a b",
			"x",
		},
	}

	cfg := cbgt.NewCfgMem()
	mgrs := map[string]*cbgt.Manager{}

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

	// cfgEventsIndexDefs := make(chan cbgt.CfgEvent, 100)
	// cfg.Subscribe(cbgt.INDEX_DEFS_KEY, cfgEventsIndexDefs)
	//
	// waitUntilEmptyCfgEventsIndexDefs := func() {
	//	waitUntilEmptyCfgEvents(cfgEventsIndexDefs)
	// }

	for testi, test := range tests {
		log.Printf("testi: %d, label: %s, ops: %q",
			testi, test.label, test.ops)

		for opi, op := range strings.Split(test.ops, " ") {
			log.Printf(" opi: %d, op: %s", opi, op)

			name := op[1:2]

			isIndexOp := name >= "x"
			if isIndexOp {
				index := name
				log.Printf(" indexOp: %s, index: %s", op[0:1], index)
			} else { // It's a node op.
				node := name
				log.Printf(" nodeOp: %s, node: %s", op[0:1], node)

				register := "wanted"
				if op[0:1] == "-" {
					register = "unknown"
				}
				if test.params["register"] != "" {
					register = test.params["register"]
				}
				if test.params[node+".register"] != "" {
					register = test.params[node+".register"]
				}

				if mgrs[node] != nil {
					mgrs[node].Stop()
					delete(mgrs, node)
				}

				waitUntilEmptyCfgEventsNodeDefsWanted()

				mgr, err := startNodeManager(testDir, cfg,
					name, register, test.params)
				if err != nil || mgr == nil {
					t.Errorf("expected no err, got: %#v", err)
				}

				if register != "unknown" {
					mgrs[node] = mgr
				}

				mgr.Kick("kick")

				waitUntilEmptyCfgEventsNodeDefsWanted()
			}
		}
	}
}

func startNodeManager(testDir string, cfg cbgt.Cfg, node, register string,
	params map[string]string) (mgr *cbgt.Manager, err error) {
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

	server := "."

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
