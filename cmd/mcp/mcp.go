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

type rebalancer struct {
	version       string
	cfg           cbgt.Cfg
	server        string
	nodesAll      []string          // Array of node UUID's.
	nodesToAdd    []string          // Array of node UUID's.
	nodesToRemove []string          // Array of node UUID's.
	nodeWeights   map[string]int    // Keyed by node UUID.
	nodeHierarchy map[string]string // Keyed by node UUID.

	begIndexDefs    *cbgt.IndexDefs
	begNodeDefs     *cbgt.NodeDefs
	begPlanPIndexes *cbgt.PlanPIndexes

	m sync.Mutex // Protects the mutatable fields that follow.

	cas uint64

	endPlanPIndexes *cbgt.PlanPIndexes

	o *blance.Orchestrator

	// Map of index -> partition -> node -> state.
	currStates map[string]map[string]map[string]string
}

// runRebalancer implements the "master, central planner (MCP)"
// rebalance workflow.
func runRebalancer(version string, cfg cbgt.Cfg, server string) (
	changed bool, err error) {
	if cfg == nil { // Can occur during testing.
		return false, nil
	}

	begIndexDefs, begNodeDefs, begPlanPIndexes, cas, err :=
		getPlan(cfg, version)
	if err != nil {
		return false, err
	}

	nodesAll, nodesToAdd, nodesToRemove,
		nodeWeights, nodeHierarchy :=
		cbgt.CalcNodesLayout(begIndexDefs, begNodeDefs, begPlanPIndexes)

	log.Printf("runRebalancer: nodesAll: %#v", nodesAll)
	log.Printf("runRebalancer: nodesToAdd: %#v", nodesToAdd)
	log.Printf("runRebalancer: nodesToRemove: %#v", nodesToRemove)
	log.Printf("runRebalancer: nodeWeights: %#v", nodeWeights)
	log.Printf("runRebalancer: nodeHierarchy: %#v", nodeHierarchy)
	log.Printf("runRebalancer: begIndexDefs: %#v", begIndexDefs)
	log.Printf("runRebalancer: begNodeDefs: %#v", begNodeDefs)
	log.Printf("runRebalancer: begPlanPIndexes: %#v, cas: %v",
		begPlanPIndexes, cas)

	r := &rebalancer{
		version:         version,
		cfg:             cfg,
		server:          server,
		nodesAll:        nodesAll,
		nodesToAdd:      nodesToAdd,
		nodesToRemove:   nodesToRemove,
		nodeWeights:     nodeWeights,
		nodeHierarchy:   nodeHierarchy,
		begIndexDefs:    begIndexDefs,
		begNodeDefs:     begNodeDefs,
		begPlanPIndexes: begPlanPIndexes,
		endPlanPIndexes: cbgt.NewPlanPIndexes(version),
		cas:             cas,
		currStates:      map[string]map[string]map[string]string{},
	}

	// TODO: Prepopulate currStates so that we can double-check that
	// our state transitions(assignPartition) are valid.

	return r.run()
}

// The run method rebalances each index, one at a time.
func (r *rebalancer) run() (bool, error) {
	changedAny := false

	for _, indexDef := range r.begIndexDefs.IndexDefs {
		changed, err := r.runIndex(indexDef)
		if err != nil {
			log.Printf("run: indexDef.Name: %s, err: %#v",
				indexDef.Name, err)
		}

		changedAny = changedAny || changed
	}

	return changedAny, nil
}

// The runIndex method rebalances a single index.
func (r *rebalancer) runIndex(indexDef *cbgt.IndexDef) (
	changed bool, err error) {
	log.Printf("runIndex: indexDef.Name: %s", indexDef.Name)

	r.m.Lock()
	if casePlanFrozen(indexDef, r.begPlanPIndexes, r.endPlanPIndexes) {
		r.m.Unlock()
		return false, nil
	}
	r.m.Unlock()

	partitionModel, begMap, endMap, err := r.calcBegEndMaps(indexDef)
	if err != nil {
		return false, err
	}

	assignPartitionFunc := func(stopCh chan struct{},
		partition, node, state, op string) error {
		return r.assignPartition(stopCh,
			indexDef.Name, partition, node, state, op)
	}

	partitionStateFunc := func(stopCh chan struct{},
		partition string, node string) (
		state string, pct float32, err error) {
		return r.partitionState(stopCh,
			indexDef.Name, partition, node)
	}

	o, err := blance.OrchestrateMoves(
		partitionModel,
		blance.OrchestratorOptions{}, // TODO.
		r.nodesAll,
		begMap,
		endMap,
		assignPartitionFunc,
		partitionStateFunc,
		blance.LowestWeightPartitionMoveForNode) // TODO: concurrency.
	if err != nil {
		return false, err
	}

	r.m.Lock()
	r.o = o
	r.m.Unlock()

	numProgress := 0

	for progress := range o.ProgressCh() {
		numProgress++

		log.Printf("run: indexDef.Name: %s,"+
			" gotProgress: %d, progress: %#v",
			indexDef.Name, numProgress, progress)
	}

	o.Stop()

	// TDOO: Check that the plan in the cfg should match our endMap...
	//
	// _, err = cbgt.CfgSetPlanPIndexes(cfg, planPIndexesFFwd, cas)
	// if err != nil {
	//     return false, fmt.Errorf("mcp: could not save new plan,"+
	//     " perhaps a concurrent planner won, cas: %d, err: %v",
	//     cas, err)
	// }

	return true, err // TODO: compute proper change response.
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
// in which case we also populate endPlanPIndexes with a clone of
// the indexDef's plans from begPlanPIndexes.
func casePlanFrozen(indexDef *cbgt.IndexDef,
	begPlanPIndexes, endPlanPIndexes *cbgt.PlanPIndexes) bool {
	if !indexDef.PlanParams.PlanFrozen {
		return false
	}

	log.Printf("casePlanFrozen: indexDef.Name: %s, plan frozen",
		indexDef.Name)

	// Copy over the previous plan, if any, for the index.
	if begPlanPIndexes != nil {
		log.Printf("casePlanFrozen: indexDef.Name: %s, plan frozen,"+
			" cloning previous plan", indexDef.Name)

		for n, p := range begPlanPIndexes.PlanPIndexes {
			if p.IndexName == indexDef.Name &&
				p.IndexUUID == indexDef.UUID {
				endPlanPIndexes.PlanPIndexes[n] = p
			}
		}
	}

	return true
}

func (r *rebalancer) calcBegEndMaps(indexDef *cbgt.IndexDef) (
	partitionModel blance.PartitionModel,
	begMap blance.PartitionMap,
	endMap blance.PartitionMap,
	err error) {
	r.m.Lock()
	defer r.m.Unlock()

	// The endPlanPIndexesForIndex is a working data structure that's
	// mutated as calcBegEndMaps progresses.
	endPlanPIndexesForIndex, err := cbgt.SplitIndexDefIntoPlanPIndexes(
		indexDef, r.server, r.endPlanPIndexes)
	if err != nil {
		log.Printf("calcBegEndMaps: indexDef.Name: %s,"+
			" could not SplitIndexDefIntoPlanPIndexes,"+
			" indexDef: %#v, server: %s, err: %v",
			indexDef.Name, indexDef, r.server, err)

		return partitionModel, begMap, endMap, err
	}

	// Invoke blance to assign the endPlanPIndexesForIndex to nodes.
	warnings := cbgt.BlancePlanPIndexes(indexDef,
		endPlanPIndexesForIndex, r.begPlanPIndexes,
		r.nodesAll, r.nodesToAdd, r.nodesToRemove,
		r.nodeWeights, r.nodeHierarchy)

	r.endPlanPIndexes.Warnings[indexDef.Name] = warnings

	// TODO: handle blance ffwd plan warnings.

	for _, warning := range warnings {
		log.Printf("calcBegEndMaps: indexDef.Name: %s,"+
			" BlancePlanPIndexes warning: %q, indexDef: %#v",
			indexDef.Name, warning, indexDef)
	}

	log.Printf("calcBegEndMaps: indexDef.Name: %s,"+
		" endPlanPIndexes: %#v", indexDef.Name, r.endPlanPIndexes)

	partitionModel, _ = cbgt.BlancePartitionModel(indexDef)

	begMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.begPlanPIndexes)
	endMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.endPlanPIndexes)

	return partitionModel, begMap, endMap, nil
}

// --------------------------------------------------------

func (r *rebalancer) assignPartition(stopCh chan struct{},
	index, partition, node, state, op string) error {
	log.Printf("assignPartitionFunc: index: %s,"+
		" partition: %s, node: %s, state: %s, op: %s",
		index, partition, node, state, op)

	r.m.Lock()

	// TODO: validate that we're making a valid state transition.
	//
	// Update currStates to the assigned index/partition/node/state.
	partitions, exists := r.currStates[index]
	if !exists || partitions == nil {
		partitions = map[string]map[string]string{}
		r.currStates[index] = partitions
	}

	nodes, exists := partitions[partition]
	if !exists || nodes == nil {
		nodes = map[string]string{}
		partitions[partition] = nodes
	}

	nodes[node] = state

	r.m.Unlock()

	// TODO: stopCh handling.

	return nil
}

func (r *rebalancer) partitionState(stopCh chan struct{},
	index, partition, node string) (
	state string, pct float32, err error) {
	log.Printf("partitionStateFunc: index: %s,"+
		" partition: %s, node: %s", index, partition, node)

	currState := ""

	r.m.Lock()
	if r.currStates[index] != nil &&
		r.currStates[index][partition] != nil {
		currState = r.currStates[index][partition][node]
	}
	r.m.Unlock()

	// TODO: real state & pct, with stopCh handling.

	return currState, 1.0, nil
}
