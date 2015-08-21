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
	"errors"
	"fmt"
	"sync"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/blance"
	"github.com/couchbaselabs/cbgt"
)

var ErrorNotPausable = errors.New("not pausable")
var ErrorNotResumable = errors.New("not resumable")

// WaitAssignPartitionDone is the signature of the synchronous
// callback that Rebalance() invokes to wait for an
// index/partition/node/state/op transition to complete.
type WaitAssignPartitionDone func(stopCh chan struct{},
	index, partition, node, state, op string) error

// A rebalancer struct holds all the tracking information for the
// Rebalance operation.
type rebalancer struct {
	version       string            // See cbgt.Manager's version.
	cfg           cbgt.Cfg          // See cbgt.Manager's cfg.
	server        string            // See cbgt.Manager's server.
	nodesAll      []string          // Array of node UUID's.
	nodesToAdd    []string          // Array of node UUID's.
	nodesToRemove []string          // Array of node UUID's.
	nodeWeights   map[string]int    // Keyed by node UUID.
	nodeHierarchy map[string]string // Keyed by node UUID.

	begIndexDefs       *cbgt.IndexDefs
	begNodeDefs        *cbgt.NodeDefs
	begPlanPIndexes    *cbgt.PlanPIndexes
	begPlanPIndexesCAS uint64

	waitAssignPartitionDone WaitAssignPartitionDone

	m sync.Mutex // Protects the mutable fields that follow.

	endPlanPIndexes *cbgt.PlanPIndexes

	o *blance.Orchestrator

	// Map of index -> partition -> node -> stateOp.
	currStates map[string]map[string]map[string]stateOp
}

// A stateOp is used to track state transitions and associates a state
// (i.e., "master") with an op (e.g., "add", "del").
type stateOp struct {
	state string
	op    string // May be "" for unknown or no in-flight op.
}

// --------------------------------------------------------

// Rebalance provides a cluster-wide, multi-index rebalancing of
// PIndexes for cbgt.  It integrates in the blance library.
func Rebalance(version string, cfg cbgt.Cfg, server string,
	waitAssignPartitionDone WaitAssignPartitionDone) (
	// TODO: Need to ensure that all nodes are up, especially those
	// that haven't been removed yet.
	//
	// TODO: Need timeouts on moves.
	changed bool, err error) {
	if cfg == nil { // Can occur during testing.
		return false, nil
	}

	uuid := "" // We don't have a uuid, as we're not a node.

	begIndexDefs, begNodeDefs, begPlanPIndexes, begPlanPIndexesCAS, err :=
		cbgt.PlannerGetPlan(cfg, version, uuid)
	if err != nil {
		return false, err
	}

	nodesAll, nodesToAdd, nodesToRemove,
		nodeWeights, nodeHierarchy :=
		cbgt.CalcNodesLayout(begIndexDefs, begNodeDefs, begPlanPIndexes)

	log.Printf("rebalance: nodesAll: %#v", nodesAll)
	log.Printf("rebalance: nodesToAdd: %#v", nodesToAdd)
	log.Printf("rebalance: nodesToRemove: %#v", nodesToRemove)
	log.Printf("rebalance: nodeWeights: %#v", nodeWeights)
	log.Printf("rebalance: nodeHierarchy: %#v", nodeHierarchy)
	log.Printf("rebalance: begIndexDefs: %#v", begIndexDefs)
	log.Printf("rebalance: begNodeDefs: %#v", begNodeDefs)
	log.Printf("rebalance: begPlanPIndexes: %#v, cas: %v",
		begPlanPIndexes, begPlanPIndexesCAS)

	r := &rebalancer{
		version:            version,
		cfg:                cfg,
		server:             server,
		nodesAll:           nodesAll,
		nodesToAdd:         nodesToAdd,
		nodesToRemove:      nodesToRemove,
		nodeWeights:        nodeWeights,
		nodeHierarchy:      nodeHierarchy,
		begIndexDefs:       begIndexDefs,
		begNodeDefs:        begNodeDefs,
		begPlanPIndexes:    begPlanPIndexes,
		begPlanPIndexesCAS: begPlanPIndexesCAS,
		endPlanPIndexes:    cbgt.NewPlanPIndexes(version),
		currStates:         map[string]map[string]map[string]stateOp{},

		waitAssignPartitionDone: waitAssignPartitionDone,
	}

	// TODO: Prepopulate currStates so that we can double-check that
	// our state transitions in assignPartition are valid.

	return r.rebalanceIndexes()
}

// PauseNewAssignments pauses any new assignments.  Any inflight
// assignments, however, will continue to completion or error.
func (r *rebalancer) PauseNewAssignments() (err error) {
	err = ErrorNotPausable

	r.m.Lock()
	if r.o != nil {
		err = r.o.PauseNewAssignments()
	}
	r.m.Unlock()

	return err
}

// ResumeNewAssignments resumes new assignments.
func (r *rebalancer) ResumeNewAssignments() (err error) {
	err = ErrorNotResumable

	r.m.Lock()
	if r.o != nil {
		err = r.o.ResumeNewAssignments()
	}
	r.m.Unlock()

	return err
}

// --------------------------------------------------------

// rebalanceIndexes rebalances each index, one at a time.
func (r *rebalancer) rebalanceIndexes() (bool, error) {
	changedAny := false

	for _, indexDef := range r.begIndexDefs.IndexDefs {
		changed, err := r.rebalanceIndex(indexDef)
		if err != nil {
			log.Printf("run: indexDef.Name: %s, err: %#v",
				indexDef.Name, err)
		}

		changedAny = changedAny || changed
	}

	return changedAny, nil
}

// --------------------------------------------------------

// rebalanceIndex rebalances a single index.
func (r *rebalancer) rebalanceIndex(indexDef *cbgt.IndexDef) (
	changed bool, err error) {
	log.Printf(" rebalanceIndex: indexDef.Name: %s", indexDef.Name)

	r.m.Lock()
	if cbgt.CasePlanFrozen(indexDef, r.begPlanPIndexes, r.endPlanPIndexes) {
		r.m.Unlock()

		log.Printf("  plan frozen: indexDef.Name: %s,"+
			" cloned previous plan", indexDef.Name)

		return false, nil
	}
	r.m.Unlock()

	partitionModel, begMap, endMap, err := r.calcBegEndMaps(indexDef)
	if err != nil {
		return false, err
	}

	assignPartitionFunc := func(stopCh chan struct{},
		partition, node, state, op string) error {
		err := r.assignPartition(stopCh,
			indexDef.Name, partition, node, state, op)
		if err != nil {
			log.Printf("assignPartitionFunc, err: %v", err)
		}

		return err
	}

	o, err := blance.OrchestrateMoves(
		partitionModel,
		blance.OrchestratorOptions{}, // TODO.
		r.nodesAll,
		begMap,
		endMap,
		assignPartitionFunc,
		blance.LowestWeightPartitionMoveForNode) // TODO: concurrency.
	if err != nil {
		return false, err
	}

	r.m.Lock()
	r.o = o
	r.m.Unlock()

	numProgress := 0
	var lastProgress blance.OrchestratorProgress

	for progress := range o.ProgressCh() {
		numProgress++
		lastProgress = progress

		log.Printf("   numProgress: %d,"+
			" indexDef.Name: %s, progress: %#v",
			numProgress, indexDef.Name, progress)
	}

	r.m.Lock()
	r.o = nil
	r.m.Unlock()

	o.Stop()

	// TDOO: Check that the plan in the cfg should match our endMap...
	//
	// _, err = cbgt.CfgSetPlanPIndexes(cfg, planPIndexesFFwd, cas)
	// if err != nil {
	//     return false, fmt.Errorf("mcp: could not save new plan,"+
	//     " perhaps a concurrent planner won, cas: %d, err: %v",
	//     cas, err)
	// }

	if len(lastProgress.Errors) > 0 {
		// TODO: Propagate errors better.
		return true, lastProgress.Errors[0]
	}

	return true, nil // TODO: compute proper change response.
}

// --------------------------------------------------------

// calcBegEndMaps calculates the before and after maps for an index.
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
		log.Printf("  calcBegEndMaps: indexDef.Name: %s,"+
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
		log.Printf("  calcBegEndMaps: indexDef.Name: %s,"+
			" BlancePlanPIndexes warning: %q, indexDef: %#v",
			indexDef.Name, warning, indexDef)
	}

	log.Printf("  calcBegEndMaps: indexDef.Name: %s,"+
		" endPlanPIndexes: %#v", indexDef.Name, r.endPlanPIndexes)

	partitionModel, _ = cbgt.BlancePartitionModel(indexDef)

	begMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.begPlanPIndexes)
	endMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.endPlanPIndexes)

	return partitionModel, begMap, endMap, nil
}

// --------------------------------------------------------

// assignPartition is invoked when blance.OrchestrateMoves() wants to
// synchronously change the partition/node/state/op for an index.
func (r *rebalancer) assignPartition(stopCh chan struct{},
	index, partition, node, state, op string) error {
	log.Printf("  assignPartition: index: %s,"+
		" partition: %s, node: %s, state: %s, op: %s",
		index, partition, node, state, op)

	err := r.assignPartitionCurrStates(index, partition, node, state, op)
	if err != nil {
		return err
	}

	indexDefs, err := cbgt.PlannerGetIndexDefs(r.cfg, r.version)
	if err != nil {
		return err
	}

	indexDef := indexDefs.IndexDefs[index]
	if indexDef == nil {
		return fmt.Errorf("assignPartition: no indexDef,"+
			" index: %s, partition: %s, node: %s, state: %s, op: %s",
			index, partition, node, state, op)
	}

	planPIndexes, cas, err := cbgt.PlannerGetPlanPIndexes(r.cfg, r.version)
	if err != nil {
		return err
	}

	err = r.updatePlanPIndexes(planPIndexes, indexDef,
		partition, node, state, op)
	if err != nil {
		return err
	}

	_, err = cbgt.CfgSetPlanPIndexes(r.cfg, planPIndexes, cas)
	if err != nil {
		return fmt.Errorf("assignPartition: update plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
	}

	if r.waitAssignPartitionDone == nil {
		return nil
	}

	return r.waitAssignPartitionDone(stopCh, index, partition,
		node, state, op)
}

// assignPartitionCurrStates validates the state transition is proper
// and then updates currStates to the assigned
// index/partition/node/state/op.
func (r *rebalancer) assignPartitionCurrStates(
	index, partition, node, state, op string) error {
	r.m.Lock()

	partitions, exists := r.currStates[index]
	if !exists || partitions == nil {
		partitions = map[string]map[string]stateOp{}
		r.currStates[index] = partitions
	}

	nodes, exists := partitions[partition]
	if !exists || nodes == nil {
		nodes = map[string]stateOp{}
		partitions[partition] = nodes
	}

	if op == "add" {
		if stateOp, exists := nodes[node]; exists && stateOp.state != "" {
			r.m.Unlock()

			return fmt.Errorf("assignPartitionCurrStates:"+
				" op was add when exists, index: %s,"+
				" partition: %s, node: %s, state: %s, op: %s, stateOp: %#v",
				index, partition, node, state, op, stateOp)
		}
	} else {
		// TODO: This validity check will only work after we
		// pre-populate the currStates with the starting state.
		// if stateOp, exists := nodes[node]; !exists || stateOp.state == "" {
		// 	r.m.Unlock()
		//
		// 	return fmt.Errorf("assignPartitionCurrStates:"+
		// 		" op was non-add when not exists, index: %s,"+
		// 		" partition: %s, node: %s, state: %s, op: %s, stateOp: %#v",
		// 		index, partition, node, state, op, stateOp)
		// }
	}

	nodes[node] = stateOp{state, op}

	r.m.Unlock()

	return nil
}

// updatePlanPIndexes modifies the planPIndexes in/out param based on
// the indexDef/node/state/op params, and may return an error if the
// state transition is invalid.
func (r *rebalancer) updatePlanPIndexes(
	planPIndexes *cbgt.PlanPIndexes, indexDef *cbgt.IndexDef,
	partition, node, state, op string) error {
	planPIndex, err := r.getPIndex(planPIndexes, partition)
	if err != nil {
		return err
	}

	canRead, canWrite :=
		r.getNodePlanParamsReadWrite(indexDef, partition, node)

	if planPIndex.Nodes == nil {
		planPIndex.Nodes = make(map[string]*cbgt.PlanPIndexNode)
	}

	priority := 0
	if state == "replica" {
		priority = len(planPIndex.Nodes)
	}

	if op == "add" {
		if planPIndex.Nodes[node] != nil {
			return fmt.Errorf("updatePlanPIndexes:"+
				" planPIndex already exists,"+
				" indexDef.Name: %s, partition: %s,"+
				" node: %s, state: %s, op: %s, planPIndex: %#v",
				indexDef.Name, partition, node, state, op, planPIndex)
		}

		// TODO: Need to shift the other node priorities around?
		planPIndex.Nodes[node] = &cbgt.PlanPIndexNode{
			CanRead:  canRead,
			CanWrite: canWrite,
			Priority: priority,
		}
	} else {
		if planPIndex.Nodes[node] == nil {
			return fmt.Errorf("updatePlanPIndexes:"+
				" planPIndex missing,"+
				" indexDef.Name: %s, partition: %s,"+
				" node: %s, state: %s, op: %s, planPIndex: %#v",
				indexDef.Name, partition, node, state, op, planPIndex)
		}

		if op == "del" {
			// TODO: Need to shift the other node priorities around?
			delete(planPIndex.Nodes, node)
		} else {
			// TODO: Need to shift the other node priorities around?
			planPIndex.Nodes[node] = &cbgt.PlanPIndexNode{
				CanRead:  canRead,
				CanWrite: canWrite,
				Priority: priority,
			}
		}
	}

	planPIndex.UUID = cbgt.NewUUID()
	planPIndexes.UUID = cbgt.NewUUID()

	return nil
}

// getPIndex returns the planPIndex, defaulting to the endPlanPIndex's
// definition if necessary.
func (r *rebalancer) getPIndex(
	planPIndexes *cbgt.PlanPIndexes, partition string) (
	*cbgt.PlanPIndex, error) {
	planPIndex := planPIndexes.PlanPIndexes[partition]
	if planPIndex == nil {
		r.m.Lock()
		endPlanPIndex := r.endPlanPIndexes.PlanPIndexes[partition]
		if endPlanPIndex != nil {
			p := *endPlanPIndex // Copy.
			planPIndex = &p
			planPIndex.Nodes = nil
			planPIndexes.PlanPIndexes[partition] = planPIndex
		}
		r.m.Unlock()
	}

	if planPIndex == nil {
		return nil, fmt.Errorf("getPIndex: no planPIndex,"+
			" partition: %s", partition)
	}

	return planPIndex, nil
}

// getNodePlanParamsReadWrite returns the read/write config for a
// partition for a node based on the plan params.
func (r *rebalancer) getNodePlanParamsReadWrite(
	indexDef *cbgt.IndexDef, partition string, node string) (
	canRead, canWrite bool) {
	canRead, canWrite = true, true

	nodePlanParam := cbgt.GetNodePlanParam(
		indexDef.PlanParams.NodePlanParams, node,
		indexDef.Name, partition)
	if nodePlanParam != nil {
		canRead = nodePlanParam.CanRead
		canWrite = nodePlanParam.CanWrite
	}

	return canRead, canWrite
}
