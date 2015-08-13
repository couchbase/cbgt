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
	"sync"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/blance"
	"github.com/couchbaselabs/cbgt"
)

// runMCP implements the central MCP rebalance workflow.
func runMCP(mgr *cbgt.Manager, server string) (bool, error) {
	cfg, version := mgr.Cfg(), mgr.Version()
	if cfg == nil { // Can occur during testing.
		return false, nil
	}

	indexDefs, nodeDefs, planPIndexes, cas, err := getPlan(cfg, version)
	if err != nil {
		return false, err
	}

	nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove,
		nodeWeights, nodeHierarchy :=
		cbgt.CalcNodesLayout(indexDefs, nodeDefs, planPIndexes)

	log.Printf("begin: nodeUUIDsAll: %#v", nodeUUIDsAll)
	log.Printf("begin: nodeUUIDsToAdd: %#v", nodeUUIDsToAdd)
	log.Printf("begin: nodeUUIDsToRemove: %#v", nodeUUIDsToRemove)
	log.Printf("begin: nodeWeights: %#v", nodeWeights)
	log.Printf("begin: nodeHierarchy: %#v", nodeHierarchy)
	log.Printf("begin: nodeDefs: %#v", nodeDefs)
	log.Printf("begin: indexDefs: %#v", indexDefs)
	log.Printf("begin: planPIndexes: %#v, cas: %v",
		planPIndexes, cas)

	// planPIndexesFFwd will be populated as we process each indexDef.
	planPIndexesFFwd := cbgt.NewPlanPIndexes(version)

	for _, indexDef := range indexDefs.IndexDefs {
		log.Printf("indexDef.Name: %s", indexDef.Name)

		if casePlanFrozen(indexDef, planPIndexes, planPIndexesFFwd) {
			continue
		}

		planPIndexesForIndex, err :=
			cbgt.SplitIndexDefIntoPlanPIndexes(indexDef,
				server, planPIndexesFFwd)
		if err != nil {
			log.Printf("indexDef.Name: %s,"+
				" could not SplitIndexDefIntoPlanPIndexes,"+
				" indexDef: %#v, server: %s, err: %v",
				indexDef.Name, indexDef, server, err)

			continue // Continue with the other IndexDefs.
		}

		// Invoke blance to assign the PlanPIndexes to nodes.
		warnings := cbgt.BlancePlanPIndexes(indexDef,
			planPIndexesForIndex, planPIndexes,
			nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove,
			nodeWeights, nodeHierarchy)

		planPIndexesFFwd.Warnings[indexDef.Name] = warnings

		// TODO: handle blance ffwd plan warnings.

		for _, warning := range warnings {
			log.Printf("indexDef.Name: %s,"+
				" BlancePlanPIndexes warning: %q, indexDef: %#v",
				indexDef.Name, warning, indexDef)
		}

		log.Printf("indexDef.Name: %s, planPIndexesFFwd: %#v",
			indexDef.Name, planPIndexesFFwd)

		partitionModel, _ := cbgt.BlancePartitionModel(indexDef)

		begMap := cbgt.BlanceMap(planPIndexesForIndex, planPIndexes)
		endMap := cbgt.BlanceMap(planPIndexesForIndex, planPIndexesFFwd)

		var m sync.Mutex

		// Map of partition -> node -> state.
		currStates := map[string]map[string]string{}

		assignPartitionFunc := func(stopCh chan struct{},
			partition, node, state, op string) error {
			log.Printf("indexDef.Name: %s,"+
				" partition: %s, node: %s, state: %s, op: %s",
				indexDef.Name, partition, node, state, op)

			m.Lock()

			nodes := currStates[partition]
			if nodes == nil {
				nodes = map[string]string{}
				currStates[partition] = nodes
			}

			nodes[node] = state

			m.Unlock()

			return nil
		}

		partitionStateFunc := func(stopCh chan struct{},
			partition string, node string) (
			state string, pct float32, err error) {
			m.Lock()
			currState := currStates[partition][node]
			m.Unlock()
			return currState, 1.0, nil
		}

		o, err := blance.OrchestrateMoves(
			partitionModel,
			blance.OrchestratorOptions{},
			nodeUUIDsAll,
			begMap,
			endMap,
			assignPartitionFunc,
			partitionStateFunc,
			blance.LowestWeightPartitionMoveForNode,
		)
		if err != nil {
			log.Printf("mcp: OrchestratorMoves, err: %#v", err)
			continue
		}

		gotProgress := 0

		for progress := range o.ProgressCh() {
			gotProgress++

			log.Printf("indexDef.Name: %s, progress: %#v",
				indexDef.Name, progress)
		}

		o.Stop()
	}

	if false {
		_, err = cbgt.CfgSetPlanPIndexes(cfg, planPIndexesFFwd, cas)
		if err != nil {
			return false, fmt.Errorf("mcp: could not save new plan,"+
				" perhaps a concurrent planner won, cas: %d, err: %v",
				cas, err)
		}
	}

	return true, nil
}

// Returns the current plan related info from the Cfg.
func getPlan(cfg cbgt.Cfg, version string) (
	indexDefs *cbgt.IndexDefs,
	nodeDefs *cbgt.NodeDefs,
	planPIndexes *cbgt.PlanPIndexes,
	cas uint64,
	err error) {
	err = cbgt.PlannerCheckVersion(cfg, version)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	indexDefs, err = cbgt.PlannerGetIndexDefs(cfg, version)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	nodeDefs, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		return nil, nil, nil, 0,
			fmt.Errorf("mcp: CfgGetNodeDefs err: %v", err)
	}
	if nodeDefs == nil {
		return nil, nil, nil, 0,
			fmt.Errorf("mcp: ended since no NodeDefs")
	}
	if cbgt.VersionGTE(version, nodeDefs.ImplVersion) == false {
		return nil, nil, nil, 0,
			fmt.Errorf("mcp: nodeDefs.ImplVersion: %s"+
				" > version: %s", nodeDefs.ImplVersion, version)
	}

	planPIndexes, cas, err = cbgt.PlannerGetPlanPIndexes(cfg, version)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	return indexDefs, nodeDefs, planPIndexes, cas, nil
}

// casePlanFrozen returns true if the plan for the indexDef is frozen,
// in which case is also populates planPIndexesFFwd with a clone of
// the indexDef's plans from planPIndexes.
func casePlanFrozen(indexDef *cbgt.IndexDef,
	planPIndexes, planPIndexesFFwd *cbgt.PlanPIndexes) bool {
	if !indexDef.PlanParams.PlanFrozen {
		return false
	}

	log.Printf("indexDef.Name: %s, plan frozen", indexDef.Name)

	// If the plan is frozen, just copy over the previous plan
	// for this index.
	if planPIndexes != nil {
		log.Printf("indexDef.Name: %s, plan frozen,"+
			" cloning previous plan", indexDef.Name)

		for n, p := range planPIndexes.PlanPIndexes {
			if p.IndexName == indexDef.Name &&
				p.IndexUUID == indexDef.UUID {
				planPIndexesFFwd.PlanPIndexes[n] = p
			}
		}
	}

	return true
}
