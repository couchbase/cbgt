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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/blance"
	"github.com/couchbaselabs/cbgt"
	"github.com/couchbaselabs/cbgt/rest/monitor"
)

var ErrorNotPausable = errors.New("not pausable")
var ErrorNotResumable = errors.New("not resumable")

// RebalanceProgress represents progress status information as the
// Rebalance() operation proceeds.
type RebalanceProgress struct {
	Error error
	Index string

	OrchestratorProgress blance.OrchestratorProgress
}

type RebalanceOptions struct {
	DryRun bool // When true, no changes, for analysis/planning.

	Log     RebalanceLogFunc
	Verbose int

	// Optional, defaults to http.Get(); this is used, for example,
	// for unit testing.
	HttpGet func(url string) (resp *http.Response, err error)
}

type RebalanceLogFunc func(format string, v ...interface{})

// A rebalancer struct holds all the tracking information for the
// Rebalance operation.
type rebalancer struct {
	version    string   // See cbgt.Manager's version.
	cfg        cbgt.Cfg // See cbgt.Manager's cfg.
	server     string   // See cbgt.Manager's server.
	options    RebalanceOptions
	progressCh chan RebalanceProgress

	monitor             *monitor.MonitorNodes
	monitorDoneCh       chan struct{}
	monitorSampleCh     chan monitor.MonitorSample
	monitorSampleWantCh chan chan monitor.MonitorSample

	nodesAll      []string          // Array of node UUID's.
	nodesToAdd    []string          // Array of node UUID's.
	nodesToRemove []string          // Array of node UUID's.
	nodeWeights   map[string]int    // Keyed by node UUID.
	nodeHierarchy map[string]string // Keyed by node UUID.

	begIndexDefs       *cbgt.IndexDefs
	begNodeDefs        *cbgt.NodeDefs
	begPlanPIndexes    *cbgt.PlanPIndexes
	begPlanPIndexesCAS uint64

	m sync.Mutex // Protects the mutable fields that follow.

	endPlanPIndexes *cbgt.PlanPIndexes

	// We start a new blance.Orchestrator for each index.
	o *blance.Orchestrator

	// Map of index -> pindex -> node -> StateOp.
	currStates CurrStates

	// Map of pindex -> (source) partition -> node -> cbgt.UUIDSeq.
	currSeqs CurrSeqs

	stopCh chan struct{} // Closed by app or when there's an error.
}

// Map of index -> pindex -> node -> StateOp.
type CurrStates map[string]map[string]map[string]StateOp

// A StateOp is used to track state transitions and associates a state
// (i.e., "master") with an op (e.g., "add", "del").
type StateOp struct {
	state string
	op    string // May be "" for unknown or no in-flight op.
}

// Map of pindex -> (source) partition -> node -> cbgt.UUIDSeq.
type CurrSeqs map[string]map[string]map[string]cbgt.UUIDSeq

// --------------------------------------------------------

// StartRebalance begins a concurrent, cluster-wide rebalancing of all
// the indexes (and their index partitions) on a cluster of cbgt
// nodes.  StartRebalance utilizes the blance library for calculating
// and orchestrating partition reassignments and the cbgt/rest/monitor
// library to watch for progress and errors.
func StartRebalance(version string, cfg cbgt.Cfg, server string,
	nodesToRemoveParam []string,
	options RebalanceOptions) (
	*rebalancer, error) {
	// TODO: Need timeouts on moves.
	//
	uuid := "" // We don't have a uuid, as we're not a node.

	begIndexDefs, begNodeDefs, begPlanPIndexes, begPlanPIndexesCAS, err :=
		cbgt.PlannerGetPlan(cfg, version, uuid)
	if err != nil {
		return nil, err
	}

	nodesAll, nodesToAdd, nodesToRemove,
		nodeWeights, nodeHierarchy :=
		cbgt.CalcNodesLayout(begIndexDefs, begNodeDefs, begPlanPIndexes)

	nodesUnknown := cbgt.StringsRemoveStrings(nodesToRemoveParam, nodesAll)
	if len(nodesUnknown) > 0 {
		return nil, fmt.Errorf("rebalance:"+
			" unknown nodes in nodesToRemoveParam: %#v",
			nodesUnknown)
	}

	nodesBoth := cbgt.StringsIntersectStrings(nodesToRemoveParam, nodesToAdd)
	if len(nodesBoth) > 0 {
		return nil, fmt.Errorf("rebalance:"+
			" nodes listed in both nodesToRemoveParam and nodesToAdd: %#v",
			nodesBoth)
	}

	nodesToRemove = append(nodesToRemove, nodesToRemoveParam...)
	nodesToRemove = cbgt.StringsIntersectStrings(nodesToRemove, nodesToRemove)

	// --------------------------------------------------------

	urlUUIDs := monitor.NodeDefsUrlUUIDs(begNodeDefs)

	monitorSampleCh := make(chan monitor.MonitorSample)

	monitorOptions := monitor.MonitorNodesOptions{
		// TODO: more options.
		HttpGet: options.HttpGet,
	}

	monitorInst, err := monitor.StartMonitorNodes(urlUUIDs,
		monitorSampleCh, monitorOptions)
	if err != nil {
		return nil, err
	}

	// --------------------------------------------------------

	stopCh := make(chan struct{})

	r := &rebalancer{
		version:             version,
		cfg:                 cfg,
		server:              server,
		options:             options,
		progressCh:          make(chan RebalanceProgress),
		monitor:             monitorInst,
		monitorDoneCh:       make(chan struct{}),
		monitorSampleCh:     monitorSampleCh,
		monitorSampleWantCh: make(chan chan monitor.MonitorSample),
		nodesAll:            nodesAll,
		nodesToAdd:          nodesToAdd,
		nodesToRemove:       nodesToRemove,
		nodeWeights:         nodeWeights,
		nodeHierarchy:       nodeHierarchy,
		begIndexDefs:        begIndexDefs,
		begNodeDefs:         begNodeDefs,
		begPlanPIndexes:     begPlanPIndexes,
		begPlanPIndexesCAS:  begPlanPIndexesCAS,
		endPlanPIndexes:     cbgt.NewPlanPIndexes(version),
		currStates:          map[string]map[string]map[string]StateOp{},
		currSeqs:            map[string]map[string]map[string]cbgt.UUIDSeq{},
		stopCh:              stopCh,
	}

	r.log("rebalance: nodesAll: %#v", nodesAll)
	r.log("rebalance: nodesToAdd: %#v", nodesToAdd)
	r.log("rebalance: nodesToRemove: %#v", nodesToRemove)
	r.log("rebalance: nodeWeights: %#v", nodeWeights)
	r.log("rebalance: nodeHierarchy: %#v", nodeHierarchy)
	r.log("rebalance: begIndexDefs: %#v", begIndexDefs)
	r.log("rebalance: begNodeDefs: %#v", begNodeDefs)
	r.log("rebalance: monitor urlUUIDs: %#v", urlUUIDs)

	begPlanPIndexesJSON, _ := json.Marshal(begPlanPIndexes)

	r.log("rebalance: begPlanPIndexes: %s, cas: %v",
		begPlanPIndexesJSON, begPlanPIndexesCAS)

	// TODO: Prepopulate currStates so that we can double-check that
	// our state transitions in assignPartition are valid.

	go r.runMonitor(stopCh)

	go r.runRebalanceIndexes(stopCh)

	return r, nil
}

// Stop asynchronously requests a stop to the rebalance operation.
// Callers can look for the closing of the ProgressCh() to see when
// the rebalance operation has actually stopped.
func (r *rebalancer) Stop() {
	r.m.Lock()
	if r.stopCh != nil {
		close(r.stopCh)
		r.stopCh = nil

		if r.o != nil {
			r.o.Stop()
			r.o = nil
		}
	}
	r.m.Unlock()
}

// ProgressCh() returns a channel that is updated occassionally when
// the rebalance has made some progress on one or more partition
// reassignments, or has reached an error.  The channel is closed when
// the rebalance operation is finished, either naturally, or due to an
// error, or via a Stop(), and all the rebalance-related resources
// have been released.
func (r *rebalancer) ProgressCh() chan RebalanceProgress {
	return r.progressCh
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

// VisitCurrStates invokes the visitor callback with the current,
// read-only CurrStates.
func (r *rebalancer) VisitCurrStates(visitor func(CurrStates)) {
	r.m.Lock()
	visitor(r.currStates)
	r.m.Unlock()
}

// --------------------------------------------------------

func (r *rebalancer) log(fmt string, v ...interface{}) {
	if r.options.Verbose < 0 {
		return
	}

	if r.options.Verbose < len(fmt) &&
		fmt[r.options.Verbose] == ' ' {
		return
	}

	f := r.options.Log
	if f == nil {
		f = log.Printf
	}

	f(fmt, v...)
}

// --------------------------------------------------------

// rebalanceIndexes rebalances each index, one at a time.
func (r *rebalancer) runRebalanceIndexes(stopCh chan struct{}) {
	i := 1
	n := len(r.begIndexDefs.IndexDefs)

	for _, indexDef := range r.begIndexDefs.IndexDefs {
		select {
		case <-stopCh:
			break
		default:
		}

		r.log("=====================================")
		r.log("runRebalanceIndexes: %d of %d", i, n)

		_, err := r.rebalanceIndex(indexDef)
		if err != nil {
			r.log("run: indexDef.Name: %s, err: %#v",
				indexDef.Name, err)

			break
		}

		i++
	}

	// Completion of rebalance operation, whether naturally or due to
	// error/Stop(), reaches the cleanup codepath here.  We wait for
	// runMonitor() to finish as it may have more sends to progressCh.
	//
	r.Stop()

	r.monitor.Stop()

	<-r.monitorDoneCh

	close(r.progressCh)

	// TODO: Need to close monitorSampleWantCh?
}

// --------------------------------------------------------

// rebalanceIndex rebalances a single index.
func (r *rebalancer) rebalanceIndex(indexDef *cbgt.IndexDef) (
	changed bool, err error) {
	r.log(" rebalanceIndex: indexDef.Name: %s", indexDef.Name)

	r.m.Lock()
	if cbgt.CasePlanFrozen(indexDef, r.begPlanPIndexes, r.endPlanPIndexes) {
		r.m.Unlock()

		r.log("  plan frozen: indexDef.Name: %s,"+
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
		err := r.assignPIndex(stopCh,
			indexDef.Name, partition, node, state, op)
		if err != nil {
			r.log("rebalance: assignPartitionFunc, err: %v", err)

			r.progressCh <- RebalanceProgress{Error: err}
			r.Stop() // Stop the rebalance.
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
	var firstErr error

	for progress := range o.ProgressCh() {
		if len(progress.Errors) > 0 &&
			firstErr == nil {
			firstErr = progress.Errors[0]
		}

		progressChanges := cbgt.StructChanges(lastProgress, progress)

		r.log("   index: %s, #%d %+v",
			indexDef.Name, numProgress, progressChanges)

		r.log("     progress: %+v", progress)

		r.progressCh <- RebalanceProgress{
			Error:                nil,
			Index:                indexDef.Name,
			OrchestratorProgress: progress,
		}

		numProgress++
		lastProgress = progress
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

	// TODO: Propagate all errors better.
	// TODO: compute proper change response.
	return true, firstErr
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
		r.log("  calcBegEndMaps: indexDef.Name: %s,"+
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

	// TODO: handle blance ffwd plan warnings better?

	r.endPlanPIndexes.Warnings[indexDef.Name] = warnings

	for _, warning := range warnings {
		r.log("  calcBegEndMaps: indexDef.Name: %s,"+
			" BlancePlanPIndexes warning: %q, indexDef: %#v",
			indexDef.Name, warning, indexDef)
	}

	j, _ := json.Marshal(r.endPlanPIndexes)
	r.log("  calcBegEndMaps: indexDef.Name: %s,"+
		" endPlanPIndexes: %s", indexDef.Name, j)

	partitionModel, _ = cbgt.BlancePartitionModel(indexDef)

	begMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.begPlanPIndexes)
	endMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.endPlanPIndexes)

	return partitionModel, begMap, endMap, nil
}

// --------------------------------------------------------

// assignPIndex is invoked when blance.OrchestrateMoves() wants to
// synchronously change the pindex/node/state/op for an index.
func (r *rebalancer) assignPIndex(stopCh chan struct{},
	index, pindex, node, state, op string) error {
	r.log("  assignPIndex: index: %s,"+
		" pindex: %s, node: %s, state: %q, op: %s",
		index, pindex, node, state, op)

	r.m.Lock() // Reduce but not eliminate CAS conflicts.

	err := r.assignPIndexCurrStates_unlocked(index,
		pindex, node, state, op)
	if err != nil {
		r.m.Unlock()
		return err
	}

	indexDefs, err := cbgt.PlannerGetIndexDefs(r.cfg, r.version)
	if err != nil {
		r.m.Unlock()
		return err
	}

	indexDef := indexDefs.IndexDefs[index]
	if indexDef == nil {
		r.m.Unlock()

		return fmt.Errorf("assignPIndex: no indexDef,"+
			" index: %s, pindex: %s, node: %s, state: %q, op: %s",
			index, pindex, node, state, op)
	}

	planPIndexes, cas, err := cbgt.PlannerGetPlanPIndexes(r.cfg, r.version)
	if err != nil {
		r.m.Unlock()
		return err
	}

	err = r.updatePlanPIndexes_unlocked(planPIndexes, indexDef,
		pindex, node, state, op)
	if err != nil {
		r.m.Unlock()
		return err
	}

	if r.options.DryRun {
		r.m.Unlock()
		return nil
	}

	_, err = cbgt.CfgSetPlanPIndexes(r.cfg, planPIndexes, cas)

	r.m.Unlock()

	if err != nil {
		return fmt.Errorf("assignPIndex: update plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
	}

	return r.waitAssignPIndexDone(stopCh, indexDef, planPIndexes,
		pindex, node, state, op)
}

// assignPIndexCurrStates_unlocked validates the state transition is
// proper and then updates currStates to the assigned
// index/pindex/node/state/op.
func (r *rebalancer) assignPIndexCurrStates_unlocked(
	index, pindex, node, state, op string) error {
	pindexes, exists := r.currStates[index]
	if !exists || pindexes == nil {
		pindexes = map[string]map[string]StateOp{}
		r.currStates[index] = pindexes
	}

	nodes, exists := pindexes[pindex]
	if !exists || nodes == nil {
		nodes = map[string]StateOp{}
		pindexes[pindex] = nodes
	}

	if op == "add" {
		if stateOp, exists := nodes[node]; exists && stateOp.state != "" {
			r.m.Unlock()

			return fmt.Errorf("assignPIndexCurrStates:"+
				" op was add when exists, index: %s, pindex: %s,"+
				" node: %s, state: %q, op: %s, stateOp: %#v",
				index, pindex, node, state, op, stateOp)
		}
	} else {
		// TODO: This validity check will only work after we
		// pre-populate the currStates with the starting state.
		// if stateOp, exists := nodes[node]; !exists || stateOp.state == "" {
		// 	r.m.Unlock()
		//
		// 	return fmt.Errorf("assignPIndexCurrStates:"+
		// 		" op was non-add when not exists, index: %s,"+
		// 		" pindex: %s, node: %s, state: %q, op: %s, stateOp: %#v",
		// 		index, pindex, node, state, op, stateOp)
		// }
	}

	nodes[node] = StateOp{state, op}

	return nil
}

// updatePlanPIndexes_unlocked modifies the planPIndexes in/out param
// based on the indexDef/node/state/op params, and may return an error
// if the state transition is invalid.
func (r *rebalancer) updatePlanPIndexes_unlocked(
	planPIndexes *cbgt.PlanPIndexes, indexDef *cbgt.IndexDef,
	pindex, node, state, op string) error {
	planPIndex, err := r.getPlanPIndex_unlocked(planPIndexes, pindex)
	if err != nil {
		return err
	}

	canRead, canWrite :=
		r.getNodePlanParamsReadWrite(indexDef, pindex, node)

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
				" indexDef: %#v, pindex: %s,"+
				" node: %s, state: %q, op: %s, planPIndex: %#v",
				indexDef, pindex, node, state, op, planPIndex)
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
				" indexDef.Name: %s, pindex: %s,"+
				" node: %s, state: %q, op: %s, planPIndex: %#v",
				indexDef.Name, pindex, node, state, op, planPIndex)
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
	planPIndexes.ImplVersion = r.version

	return nil
}

// --------------------------------------------------------

// getPlanPIndex_unlocked returns the planPIndex, defaulting to the
// endPlanPIndex's definition if necessary.
func (r *rebalancer) getPlanPIndex_unlocked(
	planPIndexes *cbgt.PlanPIndexes, pindex string) (
	*cbgt.PlanPIndex, error) {
	planPIndex := planPIndexes.PlanPIndexes[pindex]
	if planPIndex == nil {
		endPlanPIndex := r.endPlanPIndexes.PlanPIndexes[pindex]
		if endPlanPIndex != nil {
			p := *endPlanPIndex // Copy.
			planPIndex = &p
			planPIndex.Nodes = nil
			planPIndexes.PlanPIndexes[pindex] = planPIndex
		}
	}

	if planPIndex == nil {
		return nil, fmt.Errorf("getPlanPIndex: no planPIndex,"+
			" pindex: %s", pindex)
	}

	return planPIndex, nil
}

// getNodePlanParamsReadWrite returns the read/write config for a
// pindex for a node based on the plan params.
func (r *rebalancer) getNodePlanParamsReadWrite(
	indexDef *cbgt.IndexDef, pindex string, node string) (
	canRead, canWrite bool) {
	canRead, canWrite = true, true

	nodePlanParam := cbgt.GetNodePlanParam(
		indexDef.PlanParams.NodePlanParams, node,
		indexDef.Name, pindex)
	if nodePlanParam != nil {
		canRead = nodePlanParam.CanRead
		canWrite = nodePlanParam.CanWrite
	}

	return canRead, canWrite
}

// --------------------------------------------------------

// waitAssignPIndexDone will block until stopped or until an
// index/pindex/node/state/op transition is complete.
func (r *rebalancer) waitAssignPIndexDone(stopCh chan struct{},
	indexDef *cbgt.IndexDef,
	planPIndexes *cbgt.PlanPIndexes,
	pindex, node, state, op string) error {
	if op == "del" {
		return nil // TODO: Handle op del better.
	}

	feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
	if !exists || feedType == nil || feedType.PartitionSeqs == nil {
		return nil
	}

	r.m.Lock()
	planPIndex, err := r.getPlanPIndex_unlocked(planPIndexes, pindex)
	r.m.Unlock()

	if err != nil {
		return err
	}

	sourcePartitions := strings.Split(planPIndex.SourcePartitions, ",")

	// TODO: Give up after waiting too long.
	// TODO: Claim success and proceed if we see it's converging.
	for _, sourcePartition := range sourcePartitions {
		sourcePartitionSeqs, err :=
			feedType.PartitionSeqs(indexDef.SourceType,
				indexDef.SourceName,
				indexDef.SourceUUID,
				indexDef.SourceParams, r.server)
		if err != nil {
			return err
		}

		uuidSeqWant, exists := sourcePartitionSeqs[sourcePartition]
		if !exists {
			return fmt.Errorf("rebalance:"+
				" waitAssignPIndexDone,"+
				" missing sourcePartition from PartitionSeqs,"+
				" indexDef: %#v, sourcePartition: %s, node: %s,"+
				" state: %q, op: %s, sourcePartitionSeqs: %#v",
				indexDef, sourcePartition, node, state, op,
				sourcePartitionSeqs)
		}

		caughtUp := false

		for !caughtUp {
			sampleWantCh := make(chan monitor.MonitorSample)

			select {
			case <-stopCh:
				return blance.ErrorStopped

			case r.monitorSampleWantCh <- sampleWantCh:
				for sample := range sampleWantCh {
					if sample.Error != nil {
						r.log("rebalance:"+
							" waitAssignPIndexDone sample error,"+
							" uuid mismatch, indexDef: %#v,"+
							" indexDef: %#v, sourcePartition: %s,"+
							" node: %s, state: %q, op: %s,"+
							" uuidSeqWant: %+v, sample: %#v",
							indexDef, sourcePartition, node,
							state, op, uuidSeqWant, sample)

						continue
					}

					if sample.Kind == "/api/stats" {
						uuidSeqCurr, exists :=
							r.getCurrSeq(pindex, sourcePartition, node)

						r.log("      waitAssignPIndexDone,"+
							" index: %s, sourcePartition: %s,"+
							" node: %s, state: %q, op: %s,"+
							" uuidSeqWant: %+v, sample: %s, exists: %v",
							indexDef.Name, sourcePartition, node,
							state, op, uuidSeqWant, sample.Kind, exists)

						if exists {
							r.log("      waitAssignPIndexDone,"+
								" indexDef: %s, sourcePartition: %s,"+
								" node: %s, state: %q, op: %s,"+
								" uuidSeqWant: %+v, uuidSeqCurr: %+v",
								indexDef.Name, sourcePartition, node,
								state, op, uuidSeqWant, uuidSeqCurr)

							// TODO: Sometimes UUID's just don't
							// match, so need to determine underlying
							// cause.
							// if uuidSeqCurr.UUID != uuidSeqWant.UUID {
							// 	return fmt.Errorf("rebalance:"+
							// 		" waitAssignPIndexDone uuid mismatch,"+
							// 		" indexDef: %#v, sourcePartition: %s,"+
							// 		" node: %s, state: %q, op: %s,"+
							// 		" uuidSeqWant: %+v, uuidSeqCurr: %+v",
							// 		indexDef, sourcePartition, node,
							// 		state, op, uuidSeqWant, uuidSeqCurr)
							// }

							if uuidSeqCurr.Seq >= uuidSeqWant.Seq {
								caughtUp = true
								break // From !caughtUp loop.
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// getCurrSeq returns the last seen cbgt.UUIDSeq for a
// pindex/sourcePartition/node.
func (r *rebalancer) getCurrSeq(pindex, sourcePartition, node string) (
	uuidSeq cbgt.UUIDSeq, uuidSeqExists bool) {
	r.m.Lock()

	sourcePartitions, exists := r.currSeqs[pindex]
	if exists && sourcePartitions != nil {
		nodes, exists := sourcePartitions[sourcePartition]
		if exists && nodes != nil {
			us, exists := nodes[node]
			if exists {
				uuidSeq = us
				uuidSeqExists = true
			}
		}
	}

	r.m.Unlock()

	return uuidSeq, uuidSeqExists
}

// setCurrSeq updates the last seen cbgt.UUIDSeq for a
// pindex/sourcePartition/node, and returns the previous cbgt.UUIDSeq.
func (r *rebalancer) setCurrSeq(pindex, sourcePartition, node string,
	uuid string, seq uint64) (
	uuidSeqPrev cbgt.UUIDSeq, uuidSeqPrevExists bool) {
	r.m.Lock()

	sourcePartitions, exists := r.currSeqs[pindex]
	if !exists || sourcePartitions == nil {
		sourcePartitions =
			map[string]map[string]cbgt.UUIDSeq{}
		r.currSeqs[pindex] = sourcePartitions
	}

	nodes, exists := sourcePartitions[sourcePartition]
	if !exists || nodes == nil {
		nodes = map[string]cbgt.UUIDSeq{}
		sourcePartitions[sourcePartition] = nodes
	}

	uuidSeqPrev, uuidSeqPrevExists = nodes[node]

	nodes[node] = cbgt.UUIDSeq{
		UUID: uuid,
		Seq:  seq,
	}

	r.m.Unlock()

	return uuidSeqPrev, uuidSeqPrevExists
}

// --------------------------------------------------------

// runMonitor handles any error from the nodes monitoring subsystem by
// stopping the rebalance.
func (r *rebalancer) runMonitor(stopCh chan struct{}) {
	defer close(r.monitorDoneCh)

	for {
		select {
		case <-stopCh:
			return

		case s, ok := <-r.monitorSampleCh:
			if !ok {
				return
			}

			r.log("      monitor: %s, node: %s", s.Kind, s.UUID)

			if s.Error != nil {
				r.log("rebalance: runMonitor, s.Error: %#v", s.Error)

				r.progressCh <- RebalanceProgress{Error: s.Error}
				r.Stop() // Stop the rebalance.
				continue
			}

			if s.Kind == "/api/stats" {
				m := struct {
					PIndexes map[string]struct {
						Partitions map[string]struct {
							UUID string `json:"uuid"`
							Seq  uint64 `json:"seq"`
						} `json:"partitions"`
					} `json:"pindexes"`
				}{}

				err := json.Unmarshal(s.Data, &m)
				if err != nil {
					r.log("rebalance: runMonitor json, err: %#v", err)

					r.progressCh <- RebalanceProgress{Error: err}
					r.Stop() // Stop the rebalance.
					continue
				}

				for pindex, x := range m.PIndexes {
					for sourcePartition, uuidSeq := range x.Partitions {
						uuidSeqPrev, uuidSeqPrevExists := r.setCurrSeq(
							pindex, sourcePartition, s.UUID,
							uuidSeq.UUID, uuidSeq.Seq)
						if !uuidSeqPrevExists ||
							uuidSeqPrev.UUID != uuidSeq.UUID ||
							uuidSeqPrev.Seq != uuidSeq.Seq {
							r.log("    monitor, node: %s,"+
								" pindex: %s, sourcePartition: %s,"+
								" uuidSeq: %+v, uuidSeqPrev: %+v",
								s.UUID, pindex, sourcePartition,
								uuidSeq, uuidSeqPrev)
						}
					}
				}
			}

			notifyWanters := true

			for notifyWanters {
				select {
				case sampleWantCh := <-r.monitorSampleWantCh:
					sampleWantCh <- s
					close(sampleWantCh)

				default:
					notifyWanters = false
				}
			}
		}
	}
}
