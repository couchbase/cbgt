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
	"fmt"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
)

func runMCP(mgr *cbgt.Manager, server string) (bool, error) {
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

	if indexDefs == nil || nodeDefs == nil {
		return false, nil
	}

	nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove,
		nodeWeights, nodeHierarchy :=
		cbgt.CalcNodesLayout(indexDefs, nodeDefs, planPIndexesPrev)

	// TODO.

	planPIndexes, err := cbgt.CalcPlan(indexDefs, nodeDefs,
		planPIndexesPrev, version, server)
	if err != nil {
		return false, fmt.Errorf("mcp: CalcPlan, err: %v", err)
	}

	log.Printf("mcp, indexDefs: %#v", indexDefs)
	log.Printf("mcp, nodeDefs: %#v", nodeDefs)
	log.Printf("mcp, planPIndexesPrev: %#v, cas: %v",
		planPIndexesPrev, cas)
	log.Printf("mcp, planPIndexes: %#v",
		planPIndexes, cas)
	log.Printf("mcp, nodeUUIDsAll: %#v", nodeUUIDsAll)
	log.Printf("mcp, nodeUUIDsToAdd: %#v", nodeUUIDsToAdd)
	log.Printf("mcp, nodeUUIDsToRemove: %#v", nodeUUIDsToRemove)
	log.Printf("mcp, nodeWeights: %#v", nodeWeights)
	log.Printf("mcp, nodeHierarchy: %#v", nodeHierarchy)

	if cbgt.SamePlanPIndexes(planPIndexes, planPIndexesPrev) {
		return false, nil
	}

	_, err = cbgt.CfgSetPlanPIndexes(cfg, planPIndexes, cas)
	if err != nil {
		return false, fmt.Errorf("mcp: could not save new plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
	}

	return true, nil
}
