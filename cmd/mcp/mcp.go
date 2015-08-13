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
	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
)

func runMCP(mgr *cbgt.Manager) (bool, error) {
	cfg, version, uuid := mgr.Cfg(), mgr.Version(), mgr.UUID()
	if cfg == nil { // Can occur during testing.
		return false, nil
	}

	err := cbgt.PlannerCheckVersion(cfg, version)
	if err != nil {
		return false, err
	}
	indexDefs, err := cbgt.PlannerGetIndexDefs(cfg, version)
	if err != nil {
		return false, err
	}
	nodeDefs, err := cbgt.PlannerGetNodeDefs(cfg, version, uuid)
	if err != nil {
		return false, err
	}
	planPIndexesPrev, cas, err := cbgt.PlannerGetPlanPIndexes(cfg, version)
	if err != nil {
		return false, err
	}

	log.Printf("runMCP, indexDefs: %#v", indexDefs)
	log.Printf("runMCP, nodeDefs: %#v", nodeDefs)
	log.Printf("runMCP, planPIndexesPrev: %#v, cas: %v", planPIndexesPrev, cas)

	// TODO.

	return true, nil
}
