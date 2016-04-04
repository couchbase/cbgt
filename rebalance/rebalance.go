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

	"github.com/couchbase/blance"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest/monitor"
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
	// See blance.CalcPartitionMoves(favorMinNodes).
	FavorMinNodes bool

	DryRun bool // When true, no changes, for analysis/planning.

	Log     RebalanceLogFunc
	Verbose int

	// Optional, defaults to http.Get(); this is used, for example,
	// for unit testing.
	HttpGet func(url string) (resp *http.Response, err error)

	SkipSeqChecks bool // For unit-testing.
}

type RebalanceLogFunc func(format string, v ...interface{})

// A Rebalancer struct holds all the tracking information for the
// Rebalance operation.
type Rebalancer struct {
	version    string            // See cbgt.Manager's version.
	cfg        cbgt.Cfg          // See cbgt.Manager's cfg.
	server     string            // See cbgt.Manager's server.
	optionsMgr map[string]string // See cbgt.Manager's options.
	optionsReb RebalanceOptions
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

	// Map of pindex -> (source) partition -> node -> cbgt.UUIDSeq.
	wantSeqs WantSeqs

	stopCh chan struct{} // Closed by app or when there's an error.
}

// Map of index -> pindex -> node -> StateOp.
type CurrStates map[string]map[string]map[string]StateOp

// A StateOp is used to track state transitions and associates a state
// (i.e., "master") with an op (e.g., "add", "del").
type StateOp struct {
	State string
	Op    string // May be "" for unknown or no in-flight op.
}

// Map of pindex -> (source) partition -> node -> cbgt.UUIDSeq.
type CurrSeqs map[string]map[string]map[string]cbgt.UUIDSeq

// Map of pindex -> (source) partition -> node -> cbgt.UUIDSeq.
type WantSeqs map[string]map[string]map[string]cbgt.UUIDSeq

// --------------------------------------------------------

// StartRebalance begins a concurrent, cluster-wide rebalancing of all
// the indexes (and their index partitions) on a cluster of cbgt
// nodes.  StartRebalance utilizes the blance library for calculating
// and orchestrating partition reassignments and the cbgt/rest/monitor
// library to watch for progress and errors.
func StartRebalance(version string, cfg cbgt.Cfg, server string,
	optionsMgr map[string]string,
	nodesToRemoveParam []string,
	optionsReb RebalanceOptions) (
	*Rebalancer, error) {
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

	nodesToRemove = append(nodesToRemove, nodesToRemoveParam...)
	nodesToRemove = cbgt.StringsIntersectStrings(nodesToRemove, nodesToRemove)

	nodesToAdd = cbgt.StringsRemoveStrings(nodesToAdd, nodesToRemove)

	// --------------------------------------------------------

	urlUUIDs := monitor.NodeDefsUrlUUIDs(begNodeDefs)

	monitorSampleCh := make(chan monitor.MonitorSample)

	monitorOptions := monitor.MonitorNodesOptions{
		DiagSampleDisable: true,
		HttpGet:           optionsReb.HttpGet,
	}

	monitorInst, err := monitor.StartMonitorNodes(urlUUIDs,
		monitorSampleCh, monitorOptions)
	if err != nil {
		return nil, err
	}

	// --------------------------------------------------------

	stopCh := make(chan struct{})

	r := &Rebalancer{
		version:             version,
		cfg:                 cfg,
		server:              server,
		optionsMgr:          optionsMgr,
		optionsReb:          optionsReb,
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
		wantSeqs:            map[string]map[string]map[string]cbgt.UUIDSeq{},
		stopCh:              stopCh,
	}

	r.Logf("rebalance: nodesAll: %#v", nodesAll)
	r.Logf("rebalance: nodesToAdd: %#v", nodesToAdd)
	r.Logf("rebalance: nodesToRemove: %#v", nodesToRemove)
	r.Logf("rebalance: nodeWeights: %#v", nodeWeights)
	r.Logf("rebalance: nodeHierarchy: %#v", nodeHierarchy)

	// r.Logf("rebalance: begIndexDefs: %#v", begIndexDefs)
	// r.Logf("rebalance: begNodeDefs: %#v", begNodeDefs)

	r.Logf("rebalance: monitor urlUUIDs: %#v", urlUUIDs)

	// begPlanPIndexesJSON, _ := json.Marshal(begPlanPIndexes)
	//
	// r.Logf("rebalance: begPlanPIndexes: %s, cas: %v",
	// 	begPlanPIndexesJSON, begPlanPIndexesCAS)

	// TODO: Prepopulate currStates so that we can double-check that
	// our state transitions in assignPartition are valid.

	go r.runMonitor(stopCh)

	go r.runRebalanceIndexes(stopCh)

	return r, nil
}

// Stop asynchronously requests a stop to the rebalance operation.
// Callers can look for the closing of the ProgressCh() to see when
// the rebalance operation has actually stopped.
func (r *Rebalancer) Stop() {
	r.m.Lock()
	if r.stopCh != nil {
		close(r.stopCh)
		r.stopCh = nil
	}
	if r.o != nil {
		r.o.Stop()
		r.o = nil
	}
	r.m.Unlock()
}

// ProgressCh() returns a channel that is updated occassionally when
// the rebalance has made some progress on one or more partition
// reassignments, or has reached an error.  The channel is closed when
// the rebalance operation is finished, either naturally, or due to an
// error, or via a Stop(), and all the rebalance-related resources
// have been released.
func (r *Rebalancer) ProgressCh() chan RebalanceProgress {
	return r.progressCh
}

// PauseNewAssignments pauses any new assignments.  Any inflight
// assignments, however, will continue to completion or error.
func (r *Rebalancer) PauseNewAssignments() (err error) {
	err = ErrorNotPausable

	r.m.Lock()
	if r.o != nil {
		err = r.o.PauseNewAssignments()
	}
	r.m.Unlock()

	return err
}

// ResumeNewAssignments resumes new assignments.
func (r *Rebalancer) ResumeNewAssignments() (err error) {
	err = ErrorNotResumable

	r.m.Lock()
	if r.o != nil {
		err = r.o.ResumeNewAssignments()
	}
	r.m.Unlock()

	return err
}

type VisitFunc func(CurrStates, CurrSeqs, WantSeqs,
	map[string]*blance.NextMoves)

// Visit invokes the visitor callback with the current,
// read-only CurrStates, CurrSeqs and WantSeqs.
func (r *Rebalancer) Visit(visitor VisitFunc) {
	r.m.Lock()
	if r.o != nil {
		r.o.VisitNextMoves(func(m map[string]*blance.NextMoves) {
			visitor(r.currStates, r.currSeqs, r.wantSeqs, m)
		})
	} else {
		visitor(r.currStates, r.currSeqs, r.wantSeqs, nil)
	}
	r.m.Unlock()
}

// --------------------------------------------------------

func (r *Rebalancer) Logf(fmt string, v ...interface{}) {
	if r.optionsReb.Verbose < 0 {
		return
	}

	if r.optionsReb.Verbose < len(fmt) &&
		fmt[r.optionsReb.Verbose] == ' ' {
		return
	}

	f := r.optionsReb.Log
	if f == nil {
		f = log.Printf
	}

	f(fmt, v...)
}

// --------------------------------------------------------

// GetEndPlanPIndexes return value should be treated as immutable.
func (r *Rebalancer) GetEndPlanPIndexes() *cbgt.PlanPIndexes {
	r.m.Lock()
	ppi := *r.endPlanPIndexes
	r.m.Unlock()
	return &ppi
}

// --------------------------------------------------------

// rebalanceIndexes rebalances each index, one at a time.
func (r *Rebalancer) runRebalanceIndexes(stopCh chan struct{}) {
	defer func() {
		// Completion of rebalance operation, whether naturally or due
		// to error/Stop(), needs this cleanup.  Wait for runMonitor()
		// to finish as it may have more sends to progressCh.
		//
		r.Stop()

		r.monitor.Stop()

		<-r.monitorDoneCh

		close(r.progressCh)

		// TODO: Need to close monitorSampleWantCh?
	}()

	i := 1
	n := len(r.begIndexDefs.IndexDefs)

	for _, indexDef := range r.begIndexDefs.IndexDefs {
		select {
		case <-stopCh:
			return

		default:
			// NO-OP.
		}

		r.Logf("=====================================")
		r.Logf("runRebalanceIndexes: %d of %d", i, n)

		_, err := r.rebalanceIndex(stopCh, indexDef)
		if err != nil {
			r.Logf("run: indexDef.Name: %s, err: %#v",
				indexDef.Name, err)

			return
		}

		i++
	}
}

// --------------------------------------------------------

// rebalanceIndex rebalances a single index.
func (r *Rebalancer) rebalanceIndex(stopCh chan struct{},
	indexDef *cbgt.IndexDef) (
	changed bool, err error) {
	r.Logf(" rebalanceIndex: indexDef.Name: %s", indexDef.Name)

	r.m.Lock()
	if cbgt.CasePlanFrozen(indexDef, r.begPlanPIndexes, r.endPlanPIndexes) {
		r.m.Unlock()

		r.Logf("  plan frozen: indexDef.Name: %s,"+
			" cloned previous plan", indexDef.Name)

		return false, nil
	}
	r.m.Unlock()

	// Skip indexDef's with no instantiatable pindexImplType, such
	// as index aliases.
	pindexImplType, exists := cbgt.PIndexImplTypes[indexDef.Type]
	if !exists ||
		pindexImplType == nil ||
		pindexImplType.New == nil ||
		pindexImplType.Open == nil {
		return false, nil
	}

	partitionModel, begMap, endMap, err := r.calcBegEndMaps(indexDef)
	if err != nil {
		return false, err
	}

	assignPartitionFunc := func(stopCh2 chan struct{},
		partition, node, state, op string,
	) error {
		err := r.assignPIndex(stopCh, stopCh2,
			indexDef.Name, partition, node, state, op)
		if err != nil {
			r.Logf("rebalance: assignPartitionFunc, err: %v", err)

			r.progressCh <- RebalanceProgress{Error: err}
			r.Stop() // Stop the rebalance.
		}

		return err
	}

	o, err := blance.OrchestrateMoves(
		partitionModel,
		blance.OrchestratorOptions{
			// TODO: More options.
			FavorMinNodes: r.optionsReb.FavorMinNodes,
		},
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

		r.Logf("   index: %s, #%d %+v",
			indexDef.Name, numProgress, progressChanges)

		r.Logf("     progress: %+v", progress)

		r.progressCh <- RebalanceProgress{
			Error:                firstErr,
			Index:                indexDef.Name,
			OrchestratorProgress: progress,
		}

		numProgress++
		lastProgress = progress
	}

	o.Stop()

	// TDOO: Check that the plan in the cfg should match our endMap...
	//
	// _, err = cbgt.CfgSetPlanPIndexes(cfg, planPIndexesFFwd, cas)
	// if err != nil {
	//     return false, fmt.Errorf("rebalance: could not save new plan,"+
	//     " perhaps a concurrent planner won, cas: %d, err: %v",
	//     cas, err)
	// }

	// TODO: Propagate all errors better.
	// TODO: Compute proper change response.

	return true, firstErr
}

// --------------------------------------------------------

// calcBegEndMaps calculates the before and after maps for an index.
func (r *Rebalancer) calcBegEndMaps(indexDef *cbgt.IndexDef) (
	partitionModel blance.PartitionModel,
	begMap blance.PartitionMap,
	endMap blance.PartitionMap,
	err error) {
	r.m.Lock()
	defer r.m.Unlock()

	// The endPlanPIndexesForIndex is a working data structure that's
	// mutated as calcBegEndMaps progresses.
	endPlanPIndexesForIndex, err := cbgt.SplitIndexDefIntoPlanPIndexes(
		indexDef, r.server, r.optionsMgr, r.endPlanPIndexes)
	if err != nil {
		r.Logf("  calcBegEndMaps: indexDef.Name: %s,"+
			" could not SplitIndexDefIntoPlanPIndexes,"+
			" server: %s, err: %v", indexDef.Name, r.server, err)

		return partitionModel, begMap, endMap, err
	}

	// Invoke blance to assign the endPlanPIndexesForIndex to nodes.
	warnings := cbgt.BlancePlanPIndexes("", indexDef,
		endPlanPIndexesForIndex, r.begPlanPIndexes,
		r.nodesAll, r.nodesToAdd, r.nodesToRemove,
		r.nodeWeights, r.nodeHierarchy)

	r.endPlanPIndexes.Warnings[indexDef.Name] = warnings

	for _, warning := range warnings {
		r.Logf("  calcBegEndMaps: indexDef.Name: %s,"+
			" BlancePlanPIndexes warning: %q",
			indexDef.Name, warning)
	}

	j, _ := json.Marshal(r.endPlanPIndexes)
	r.Logf("  calcBegEndMaps: indexDef.Name: %s,"+
		" endPlanPIndexes: %s", indexDef.Name, j)

	partitionModel, _ = cbgt.BlancePartitionModel(indexDef)

	begMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.begPlanPIndexes)
	endMap = cbgt.BlanceMap(endPlanPIndexesForIndex, r.endPlanPIndexes)

	return partitionModel, begMap, endMap, nil
}

// --------------------------------------------------------

// assignPIndex is invoked when blance.OrchestrateMoves() wants to
// synchronously change the pindex/node/state/op for an index.
func (r *Rebalancer) assignPIndex(stopCh, stopCh2 chan struct{},
	index, pindex, node, state, op string) error {
	r.Logf("  assignPIndex: index: %s,"+
		" pindex: %s, node: %s, state: %q, op: %s",
		index, pindex, node, state, op)

	r.m.Lock() // Reduce but not eliminate CAS conflicts.
	indexDef, planPIndexes, formerPrimaryNode, err :=
		r.assignPIndexLOCKED(index, pindex, node, state, op)
	r.m.Unlock()

	if err != nil {
		return fmt.Errorf("assignPIndex: update plan,"+
			" perhaps a concurrent planner won, err: %v", err)
	}

	return r.waitAssignPIndexDone(stopCh, stopCh2,
		indexDef, planPIndexes,
		pindex, node, state, op, formerPrimaryNode)
}

// assignPIndexLOCKED updates the cfg with the pindex assignment, and
// should be invoked while holding the r.m lock.
func (r *Rebalancer) assignPIndexLOCKED(index, pindex, node, state, op string) (
	*cbgt.IndexDef, *cbgt.PlanPIndexes, string, error) {
	err := r.assignPIndexCurrStates_unlocked(index, pindex, node, state, op)
	if err != nil {
		return nil, nil, "", err
	}

	indexDefs, err := cbgt.PlannerGetIndexDefs(r.cfg, r.version)
	if err != nil {
		return nil, nil, "", err
	}

	indexDef := indexDefs.IndexDefs[index]
	if indexDef == nil {
		return nil, nil, "", fmt.Errorf("assignPIndex: no indexDef,"+
			" index: %s, pindex: %s, node: %s, state: %q, op: %s",
			index, pindex, node, state, op)
	}

	planPIndexes, cas, err := cbgt.PlannerGetPlanPIndexes(r.cfg, r.version)
	if err != nil {
		return nil, nil, "", err
	}

	formerPrimaryNode, err := r.updatePlanPIndexes_unlocked(planPIndexes,
		indexDef, pindex, node, state, op)
	if err != nil {
		return nil, nil, "", err
	}

	if r.optionsReb.DryRun {
		return nil, nil, formerPrimaryNode, nil
	}

	_, err = cbgt.CfgSetPlanPIndexes(r.cfg, planPIndexes, cas)
	if err != nil {
		return nil, nil, "", err
	}

	return indexDef, planPIndexes, formerPrimaryNode, err
}

// --------------------------------------------------------

// assignPIndexCurrStates_unlocked validates the state transition is
// proper and then updates currStates to the assigned
// index/pindex/node/state/op.
func (r *Rebalancer) assignPIndexCurrStates_unlocked(
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
		if stateOp, exists := nodes[node]; exists && stateOp.State != "" {
			return fmt.Errorf("assignPIndexCurrStates:"+
				" op was add when exists, index: %s, pindex: %s,"+
				" node: %s, state: %q, op: %s, stateOp: %#v",
				index, pindex, node, state, op, stateOp)
		}
	} else {
		// TODO: This validity check will only work after we
		// pre-populate the currStates with the starting state.
		// if stateOp, exists := nodes[node]; !exists || stateOp.State == "" {
		// 	return fmt.Errorf("assignPIndexCurrStates:"+
		// 		" op was non-add when not exists, index: %s,"+
		// 		" pindex: %s, node: %s, state: %q, op: %s, stateOp: %#v",
		// 		index, pindex, node, state, op, stateOp)
		// }
	}

	nodes[node] = StateOp{state, op}

	return nil
}

// --------------------------------------------------------

// updatePlanPIndexes_unlocked modifies the planPIndexes in/out param
// based on the indexDef/node/state/op params, and may return an error
// if the state transition is invalid.
func (r *Rebalancer) updatePlanPIndexes_unlocked(
	planPIndexes *cbgt.PlanPIndexes, indexDef *cbgt.IndexDef,
	pindex, node, state, op string) (string, error) {
	planPIndex, err := r.getPlanPIndex_unlocked(planPIndexes, pindex)
	if err != nil {
		return "", err
	}

	formerPrimaryNode := ""
	for node, planPIndexNode := range planPIndex.Nodes {
		if planPIndexNode.Priority <= 0 {
			formerPrimaryNode = node
		}
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
			return "", fmt.Errorf("updatePlanPIndexes:"+
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
			return "", fmt.Errorf("updatePlanPIndexes:"+
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

	return formerPrimaryNode, nil
}

// --------------------------------------------------------

// getPlanPIndex_unlocked returns the planPIndex, defaulting to the
// endPlanPIndex's definition if necessary.
func (r *Rebalancer) getPlanPIndex_unlocked(
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

// --------------------------------------------------------

// getNodePlanParamsReadWrite returns the read/write config for a
// pindex for a node based on the plan params.
func (r *Rebalancer) getNodePlanParamsReadWrite(
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
func (r *Rebalancer) waitAssignPIndexDone(stopCh, stopCh2 chan struct{},
	indexDef *cbgt.IndexDef,
	planPIndexes *cbgt.PlanPIndexes,
	pindex, node, state, op, formerPrimaryNode string) error {
	if op == "del" {
		return nil // TODO: Handle op del better.
	}

	if state == "replica" {
		// No need to wait for a replica pindex to be "caught up".
		return nil
	}

	if formerPrimaryNode == "" {
		// There was no previous primary pindex on some node to be
		// "caught up" against.
		return nil
	}

	r.m.Lock()
	planPIndex, err := r.getPlanPIndex_unlocked(planPIndexes, pindex)
	r.m.Unlock()

	if err != nil {
		return err
	}

	sourcePartitions := strings.Split(planPIndex.SourcePartitions, ",")

	// Loop to retrieve all the seqs that we need to reach for all
	// source partitions.
	if !r.optionsReb.SkipSeqChecks {
		for _, sourcePartition := range sourcePartitions {
		INIT_WANT_SEQ:
			for {
				_, exists := r.getUUIDSeq(r.wantSeqs, pindex,
					sourcePartition, node)
				if exists {
					break INIT_WANT_SEQ
				}

				sampleWantCh := make(chan monitor.MonitorSample)

				select {
				case <-stopCh:
					return blance.ErrorStopped

				case <-stopCh2:
					return blance.ErrorStopped

				case r.monitorSampleWantCh <- sampleWantCh:
					for range sampleWantCh {
						// NO-OP, but a new sample meant r.currSeqs was updated.
					}
				}

				uuidSeqWant, exists := r.getUUIDSeq(r.currSeqs, pindex,
					sourcePartition, formerPrimaryNode)
				if exists {
					r.setUUIDSeq(r.wantSeqs, pindex, sourcePartition, node,
						uuidSeqWant.UUID, uuidSeqWant.Seq)
				}
			}
		}
	}

	// Loop to wait until we're caught up to the wanted seq for all
	// source partitions.
	//
	// TODO: Give up after waiting too long.
	// TODO: Claim success and proceed if we see it's converging.
	for _, sourcePartition := range sourcePartitions {
		uuidSeqWant, exists := r.getUUIDSeq(r.wantSeqs, pindex,
			sourcePartition, node)
		if !exists && !r.optionsReb.SkipSeqChecks {
			return fmt.Errorf("rebalance:"+
				" waitAssignPIndexDone, could not find uuidSeqWant,"+
				" indexDef: %#v, pindex: %s, sourcePartition: %s, node: %s,"+
				" state: %q, op: %s",
				indexDef, pindex, sourcePartition, node, state, op)
		}

		reached, err := r.uuidSeqReached(indexDef.Name,
			pindex, sourcePartition, node, uuidSeqWant)
		if err != nil {
			return err
		}

		if reached {
			return nil
		}

		caughtUp := false

		for !caughtUp {
			sampleWantCh := make(chan monitor.MonitorSample)

			select {
			case <-stopCh:
				return blance.ErrorStopped

			case <-stopCh2:
				return blance.ErrorStopped

			case r.monitorSampleWantCh <- sampleWantCh:
				var sampleErr error

				for sample := range sampleWantCh {
					if sample.Error != nil {
						sampleErr = sample.Error

						r.Logf("rebalance:"+
							" waitAssignPIndexDone sample error,"+
							" uuid mismatch, index: %s,"+
							" sourcePartition: %s, node: %s,"+
							" state: %q, op: %s, uuidSeqWant: %+v,"+
							" sample: %#v",
							indexDef.Name, sourcePartition, node,
							state, op, uuidSeqWant, sample)

						continue
					}

					if sample.Kind == "/api/stats" {
						reached, err := r.uuidSeqReached(indexDef.Name,
							pindex, sourcePartition, node, uuidSeqWant)
						if err != nil {
							sampleErr = err
						} else {
							caughtUp = caughtUp || reached

							r.progressCh <- RebalanceProgress{}
						}
					}
				}

				if sampleErr != nil {
					return sampleErr
				}
			}
		}
	}

	return nil
}

// --------------------------------------------------------

func (r *Rebalancer) uuidSeqReached(index string, pindex string,
	sourcePartition string, node string,
	uuidSeqWant cbgt.UUIDSeq) (bool, error) {
	if r.optionsReb.SkipSeqChecks {
		return true, nil
	}

	uuidSeqCurr, exists :=
		r.getUUIDSeq(r.currSeqs, pindex, sourcePartition, node)

	r.Logf("      uuidSeqReached,"+
		" index: %s, pindex: %s, sourcePartition: %s,"+
		" node: %s, uuidSeqWant: %+v, uuidSeqCurr: %+v, exists: %v",
		index, pindex, sourcePartition, node,
		uuidSeqWant, uuidSeqCurr, exists)

	if exists {
		// TODO: Sometimes UUID's just don't
		// match, so need to determine underlying
		// cause.
		// if uuidSeqCurr.UUID != uuidSeqWant.UUID {
		// 	return false, fmt.Errorf("rebalance:"+
		// 		" uuidSeqReached uuid mismatch,"+
		// 		" index: %s, pindex: %s, sourcePartition: %s,"+
		// 		" node: %s, uuidSeqWant: %+v, uuidSeqCurr: %+v",
		// 		index, pindex, sourcePartition, node,
		// 		uuidSeqWant, uuidSeqCurr)
		// }

		if uuidSeqCurr.Seq >= uuidSeqWant.Seq {
			return true, nil
		}
	}

	return false, nil
}

// --------------------------------------------------------

// getUUIDSeq returns the cbgt.UUIDSeq for a
// pindex/sourcePartition/node.
func (r *Rebalancer) getUUIDSeq(
	m map[string]map[string]map[string]cbgt.UUIDSeq,
	pindex, sourcePartition, node string) (
	uuidSeq cbgt.UUIDSeq, uuidSeqExists bool) {
	r.m.Lock()
	uuidSeq, uuidSeqExists = GetUUIDSeq(m, pindex, sourcePartition, node)
	r.m.Unlock()

	return uuidSeq, uuidSeqExists
}

// setUUIDSeq updates the cbgt.UUIDSeq for a
// pindex/sourcePartition/node, and returns the previous cbgt.UUIDSeq.
func (r *Rebalancer) setUUIDSeq(
	m map[string]map[string]map[string]cbgt.UUIDSeq,
	pindex, sourcePartition, node string,
	uuid string, seq uint64) (
	uuidSeqPrev cbgt.UUIDSeq, uuidSeqPrevExists bool) {
	r.m.Lock()
	uuidSeqPrev, uuidSeqPrevExists =
		SetUUIDSeq(m, pindex, sourcePartition, node, uuid, seq)
	r.m.Unlock()

	return uuidSeqPrev, uuidSeqPrevExists
}

// --------------------------------------------------------

// GetUUIDSeq returns the cbgt.UUIDSeq for a
// pindex/sourcePartition/node.
func GetUUIDSeq(
	m map[string]map[string]map[string]cbgt.UUIDSeq,
	pindex, sourcePartition, node string) (
	uuidSeq cbgt.UUIDSeq, uuidSeqExists bool) {
	sourcePartitions, exists := m[pindex]
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

	return uuidSeq, uuidSeqExists
}

// SetUUIDSeq updates the cbgt.UUIDSeq for a
// pindex/sourcePartition/node, and returns the previous cbgt.UUIDSeq.
func SetUUIDSeq(
	m map[string]map[string]map[string]cbgt.UUIDSeq,
	pindex, sourcePartition, node string,
	uuid string, seq uint64) (
	uuidSeqPrev cbgt.UUIDSeq, uuidSeqPrevExists bool) {
	sourcePartitions, exists := m[pindex]
	if !exists || sourcePartitions == nil {
		sourcePartitions =
			map[string]map[string]cbgt.UUIDSeq{}
		m[pindex] = sourcePartitions
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

	return uuidSeqPrev, uuidSeqPrevExists
}

// --------------------------------------------------------

// runMonitor handles any error from the nodes monitoring subsystem by
// stopping the rebalance.
func (r *Rebalancer) runMonitor(stopCh chan struct{}) {
	defer close(r.monitorDoneCh)

	for {
		select {
		case <-stopCh:
			return

		case s, ok := <-r.monitorSampleCh:
			if !ok {
				return
			}

			r.Logf("      monitor: %s, node: %s", s.Kind, s.UUID)

			if s.Error != nil {
				r.Logf("rebalance: runMonitor, s.Error: %#v", s.Error)

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
					r.Logf("rebalance: runMonitor json, err: %#v", err)

					r.progressCh <- RebalanceProgress{Error: err}
					r.Stop() // Stop the rebalance.
					continue
				}

				for pindex, x := range m.PIndexes {
					for sourcePartition, uuidSeq := range x.Partitions {
						uuidSeqPrev, uuidSeqPrevExists := r.setUUIDSeq(
							r.currSeqs, pindex, sourcePartition,
							s.UUID, uuidSeq.UUID, uuidSeq.Seq)
						if !uuidSeqPrevExists ||
							uuidSeqPrev.UUID != uuidSeq.UUID ||
							uuidSeqPrev.Seq != uuidSeq.Seq {
							r.Logf("    monitor, node: %s,"+
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
