//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rebalance

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/blance"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest/monitor"
)

var ErrorNotPausable = errors.New("not pausable")
var ErrorNotResumable = errors.New("not resumable")
var ErrorNoIndexDefinitionFound = errors.New("no index definition found")
var ErrorConcurrentPlannerInProgress = errors.New("concurrent planner in progress")

// StatsSampleErrorThreshold defines the default upper limit for
// the ephemeral stats monitoring errors tolerated / ignored
// during a heavy rebalance scenario.
var StatsSampleErrorThreshold = uint8(3)

// RebalanceProgress represents progress status information as the
// Rebalance() operation proceeds.
type RebalanceProgress struct {
	Error error
	Index string

	OrchestratorProgress blance.OrchestratorProgress
	// Map of pindex -> transfer progress in range of 0 to 1.
	TransferProgress map[string]float64
}

type RebalanceOptions struct {
	// See blance.CalcPartitionMoves(favorMinNodes).
	FavorMinNodes bool

	MaxConcurrentPartitionMovesPerNode int

	// AddPrimaryDirectly, when true, means the rebalancer should
	// assign a pindex as primary to a node directly, and not use a
	// replica-promotion maneuver (e.g., assign replica first, wait
	// until replica is caught up, then promote replica to primary).
	AddPrimaryDirectly bool

	DryRun bool // When true, no changes, for analysis/planning.

	Log     RebalanceLogFunc
	Verbose int

	// Optional, defaults to http.Get(); this is used, for example,
	// for unit testing.
	HttpGet func(url string) (resp *http.Response, err error)

	SkipSeqChecks bool // For unit-testing.

	// SeqChecksTimeoutInSec is an optional configurable timeout value,
	// which when set would timeout to unblock any of the partition catch
	// related wait loops. It is applicable to both the sequence number
	// initialization and catch up wait loops for the formerPrimary as well
	// as the new partition node.
	SeqChecksTimeoutInSec int

	Manager *cbgt.Manager

	StatsSampleErrorThreshold *int
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

	recoveryPlanPIndexes *cbgt.PlanPIndexes

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

	transferProgress map[string]float64 // pindex -> file transfer progress
}

// Map of index -> pindex -> node -> StateOp.
type CurrStates map[string]map[string]map[string]StateOp

// A StateOp is used to track state transitions and associates a state
// (i.e., "primary") with an op (e.g., "add", "del").
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
		transferProgress:    map[string]float64{},
	}

	r.Logf("rebalance: nodesAll: %#v", nodesAll)
	r.Logf("rebalance: nodesToAdd: %#v", nodesToAdd)
	r.Logf("rebalance: nodesToRemove: %#v", nodesToRemove)
	r.Logf("rebalance: nodeWeights: %#v", nodeWeights)
	r.Logf("rebalance: nodeHierarchy: %#v", nodeHierarchy)

	// r.Logf("rebalance: begIndexDefs: %#v", begIndexDefs)
	// r.Logf("rebalance: begNodeDefs: %#v", begNodeDefs)

	r.Logf("rebalance: monitor urlUUIDs: %#v", urlUUIDs)

	r.initPlansForRecoveryRebalance(nodesToAdd)

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

// ProgressCh() returns a channel that is updated occasionally when
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

type VisitFunc func(CurrStates, CurrSeqs, WantSeqs, map[string]float64,
	map[string]*blance.NextMoves)

// Visit invokes the visitor callback with the current,
// read-only CurrStates, CurrSeqs and WantSeqs.
func (r *Rebalancer) Visit(visitor VisitFunc) {
	r.m.Lock()
	if r.o != nil {
		r.o.VisitNextMoves(func(m map[string]*blance.NextMoves) {
			visitor(r.currStates, r.currSeqs, r.wantSeqs, r.transferProgress, m)
		})
	} else {
		visitor(r.currStates, r.currSeqs, r.wantSeqs, r.transferProgress, nil)
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

// GetMovingPartitionsCount returns the total partitions
// to be moved as a part of the rebalance operation.
func (r *Rebalancer) GetMovingPartitionsCount() int {
	count := 0
	r.m.Lock()
	if r.o != nil {
		r.o.VisitNextMoves(func(m map[string]*blance.NextMoves) {
			if m != nil {
				for _, nextMoves := range m {
					if len(nextMoves.Moves) > 0 {
						count++
					}
				}
			}
		})
	}
	r.m.Unlock()
	if r.begIndexDefs != nil && r.begIndexDefs.IndexDefs != nil {
		// upfront approximation to get the total partitions
		// based on the assumption that index partitions are evenly
		// distributed which may not quite true, due to chronology of
		// index creations and the corresponding topology changes
		return len(r.begIndexDefs.IndexDefs) * count
	}
	return 0
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

	if reflect.DeepEqual(begMap, endMap) {
		r.Logf("rebalanceIndex: skipping indexDef.Name: %s"+
			" as the begin and end plans are same", indexDef.Name)
		return true, nil
	}

	assignPartitionsFunc := func(stopCh2 chan struct{}, node string,
		partitions, states, ops []string) error {
		r.Logf("rebalance: assignPIndexes, index: %s, node: %s, partitions: %v,"+
			" states: %v, ops: %v, starts", indexDef.Name, node, partitions,
			states, ops)

		err2 := r.assignPIndexes(stopCh, stopCh2,
			indexDef.Name, node, partitions, states, ops)

		r.Logf("rebalance: assignPIndexes, index: %s, node: %s, partitions: %v,"+
			" states: %v, ops: %v, finished", indexDef.Name, node, partitions,
			states, ops)

		if err2 != nil {
			r.Logf("rebalance: assignPartitionsFunc, err: %v", err2)
			// Stop rebalance for all other errors.
			if !errors.Is(err2, ErrorNoIndexDefinitionFound) {
				r.progressCh <- RebalanceProgress{Error: err2}
				r.Stop()
				return err2
			}
		}
		return nil
	}

	o, err := blance.OrchestrateMoves(
		partitionModel,
		blance.OrchestratorOptions{
			MaxConcurrentPartitionMovesPerNode: r.optionsReb.MaxConcurrentPartitionMovesPerNode,
			FavorMinNodes:                      r.optionsReb.FavorMinNodes,
		},
		r.nodesAll,
		begMap,
		endMap,
		assignPartitionsFunc,
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

		// propagate the orchestrator progress for further triggering detailed
		// rebalance progress computations only when it is absolute necessary,
		// eg: upon any actual partition movement status related events.
		if partitionSeqCatchUpInProgress(progress, lastProgress) {

			r.progressCh <- RebalanceProgress{
				Error:                firstErr,
				Index:                indexDef.Name,
				OrchestratorProgress: progress,
			}
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

// partitionSeqCatchUpInProgress checks whether there's been any
// progress in the physical partition movement since the last
// iteration based on the orchestrator stats.
func partitionSeqCatchUpInProgress(progress,
	lastProgress blance.OrchestratorProgress) bool {
	started := progress.TotMoverAssignPartition
	finished := lastProgress.TotMoverAssignPartitionOk +
		lastProgress.TotMoverAssignPartitionErr
	return started > finished
}

// initPlansForRecoveryRebalance attempts to figure out whether the
// current rebalance operation is a recovery one or not and sets the
// recoveryPlanPIndexes accordingly.
func (r *Rebalancer) initPlansForRecoveryRebalance(nodesToAdd []string) {
	if len(nodesToAdd) == 0 || r.optionsReb.Manager == nil {
		return
	}
	// check whether the previous cluster contained the nodesToAdd to
	// figure out whether it is a recovery operation.
	begPlanPIndexesCopy := r.optionsReb.Manager.GetStableLocalPlanPIndexes()
	if begPlanPIndexesCopy != nil {
		var prevNodes []string
		for _, pp := range begPlanPIndexesCopy.PlanPIndexes {
			for uuid := range pp.Nodes {
				prevNodes = append(prevNodes, uuid)
			}
		}
		// check whether all the nodes to get added were a part of the cluster.
		cNodes := cbgt.StringsIntersectStrings(prevNodes, nodesToAdd)
		sort.Strings(cNodes)

		if len(cNodes) != len(nodesToAdd) {
			return
		}
		for i := range nodesToAdd {
			if cNodes[i] != nodesToAdd[i] {
				return
			}
		}
		r.recoveryPlanPIndexes = begPlanPIndexesCopy
	}
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

	var warnings []string
	if r.recoveryPlanPIndexes != nil {
		// During the failover, cbgt ignores the new nextMap from blance
		// and just promotes the replica partitions to primary.
		// Hence during the failover-recovery rebalance operation,
		// feed the pre failover plan to the blance so that it would
		// be able to come up with the same exact plan for the
		// same set of nodes and the original planPIndexes.
		r.Logf("  calcBegEndMaps: recovery rebalance for index: %s", indexDef.Name)
		warnings = cbgt.BlancePlanPIndexes("", indexDef,
			endPlanPIndexesForIndex, r.recoveryPlanPIndexes,
			r.nodesAll, []string{}, r.nodesToRemove,
			r.nodeWeights, r.nodeHierarchy)
	} else {
		nodeWeights := r.adjustNodeWeights(endPlanPIndexesForIndex)

		// Invoke blance to assign the endPlanPIndexesForIndex to nodes.
		warnings = cbgt.BlancePlanPIndexes("", indexDef,
			endPlanPIndexesForIndex, r.begPlanPIndexes,
			r.nodesAll, r.nodesToAdd, r.nodesToRemove,
			nodeWeights, r.nodeHierarchy)
	}

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

// adjustNodeWeights overrides the node weights reflective of the
// existing partition count on those nodes. It helps in balanced
// partition assignments across nodes for the single partitioned indexes.
func (r *Rebalancer) adjustNodeWeights(
	planPIndexesForIndex map[string]*cbgt.PlanPIndex) map[string]int {
	if r.optionsReb.Manager != nil {
		options := r.optionsReb.Manager.GetOptions()
		if enabled, found := options["enablePartitionNodeStickiness"]; found &&
			enabled == "true" {
			return r.nodeWeights
		}
	}

	nodeWeights := r.nodeWeights
	// if the index is a single partitioned one,
	// then try to normalize the node weights.
	if len(planPIndexesForIndex) == 1 {
		nodeWeights = cbgt.NormaliseNodeWeights(r.nodeWeights,
			r.endPlanPIndexes, len(r.begPlanPIndexes.PlanPIndexes))
	}

	return nodeWeights
}

// --------------------------------------------------------
// pindexMoves is a wrapper for a pindex movement containing
// the detailed/multi-step stateOps transitions.
type pindexMoves struct {
	name     string
	stateOps []StateOp
}

// --------------------------------------------------------

// assignPIndex is invoked when blance.OrchestrateMoves() wants to
// synchronously change one or more pindex/node/state/op for an index.
func (r *Rebalancer) assignPIndexes(stopCh, stopCh2 chan struct{},
	index string, node string, pindexes, states, ops []string) error {
	pindexesMoves := r.createPindexesMoves(pindexes, states, ops)

	r.Logf("  assignPIndex: index: %s,"+
		" pindexes: %v, node: %s, target states: %v, target ops: %v",
		index, pindexes, node, states, ops)

	// Move multiple partitions one step at a time. There could be a
	// few potential multi-step partition movements.
	var next int
	for len(pindexesMoves) > 0 {
		r.m.Lock() // Reduce but not eliminate CAS conflicts.
		indexDef, planPIndexes, formerPrimaryNodes, err := r.assignPIndexesLOCKED(
			index, node, pindexesMoves, next)
		r.m.Unlock()

		if err != nil {
			if !errors.Is(err, ErrorNoIndexDefinitionFound) {
				return fmt.Errorf("assignPIndex: update plan,"+
					" perhaps a concurrent planner won, err: %w", err)
			}
			r.Logf("assignPIndex: update plan,"+
				" perhaps a concurrent planner won, no indexDef,"+
				" index: %s, pindexes: %v, node: %s, states: %v, ops: %v",
				index, pindexes, node, states, ops)
			return err
		}

		// start workers per pindex for tracking the partition assignment
		// completion.
		var wg sync.WaitGroup
		doneCh := make(chan error, len(pindexesMoves))

		for i := 0; i < len(pindexesMoves); i++ {
			wg.Add(1)
			go func(pm *pindexMoves, formerPrimaryNode string) {
				err := r.waitAssignPIndexDone(stopCh, stopCh2,
					indexDef, planPIndexes, pm.name, node,
					pm.stateOps[next].State,
					pm.stateOps[next].Op,
					formerPrimaryNode,
					len(pm.stateOps) > 1)
				doneCh <- err
				wg.Done()
			}(pindexesMoves[i], formerPrimaryNodes[i])
		}

		wg.Wait()
		close(doneCh)

		var errs []string
		indexMissingErrsOnly := true
		for err := range doneCh {
			if err != nil {
				errs = append(errs, err.Error())
				if indexMissingErrsOnly && !errors.Is(err, ErrorNoIndexDefinitionFound) {
					indexMissingErrsOnly = false
				}
			}
		}
		if len(errs) > 0 {
			// with index definition missing errors rebalance would continue further.
			if indexMissingErrsOnly {
				return fmt.Errorf("rebalance: waitAssignPIndexDone missing index: %s,"+
					" errors: %w", index, ErrorNoIndexDefinitionFound)
			}
			return fmt.Errorf("rebalance: waitAssignPIndexDone errors: %d, %#v",
				len(errs), errs)
		}

		// pindexesMoves might contain partition movements with single/two-step
		// maneuvers for completion. So filter out any of the already completed
		// single step pindex movements.
		next++
		pindexesMoves = removeShortMoves(pindexesMoves, next)
	}

	return nil
}

// --------------------------------------------------------

func (r *Rebalancer) createPindexesMoves(pindexes, states,
	ops []string) []*pindexMoves {
	pindexesMoves := make([]*pindexMoves, len(pindexes))

	for i := 0; i < len(pindexes); i++ {
		pm := &pindexMoves{name: pindexes[i]}

		if !r.optionsReb.AddPrimaryDirectly &&
			states[i] == "primary" && ops[i] == "add" {
			// If we want to add a pindex to a node as a primary, then
			// perform a 2-step maneuver by first adding the pindex as a
			// replica, then promote that replica to master.
			pm.stateOps = []StateOp{
				{State: "replica", Op: "add"},
				{State: "primary", Op: "promote"},
			}
		} else if states[i] == "primary" && ops[i] == "promote" {
			// If we want to promote a pindex from replica to primary, then
			// introduce a 2-step maneuver, the first step is a no-op, to
			// allow the loop below to wait-for-catchup before promoting the
			// replica to master.
			pm.stateOps = []StateOp{
				{State: "replica", Op: "promote"},
				{State: "primary", Op: "promote"},
			}
		} else {
			pm.stateOps = []StateOp{{State: states[i], Op: ops[i]}}
		}

		pindexesMoves[i] = pm
	}

	return pindexesMoves
}

// --------------------------------------------------------

func removeShortMoves(pms []*pindexMoves, length int) []*pindexMoves {
	var rv []*pindexMoves
	for _, pm := range pms {
		if len(pm.stateOps) > length {
			rv = append(rv, pm)
		}
	}
	return rv
}

// --------------------------------------------------------

// assignPIndexesLOCKED updates the cfg with the pindex assignment, and
// should be invoked while holding the r.m lock.
func (r *Rebalancer) assignPIndexesLOCKED(index string, node string,
	pms []*pindexMoves, next int) (*cbgt.IndexDef, *cbgt.PlanPIndexes,
	[]string, error) {
	for _, pm := range pms {
		err := r.assignPIndexCurrStatesLOCKED(index, pm.name, node,
			pm.stateOps[next].State, pm.stateOps[next].Op)

		if err != nil {
			return nil, nil, nil,
				fmt.Errorf("assignPIndexCurrStatesLOCKED err: %v, %w",
					err, ErrorConcurrentPlannerInProgress)
		}
	}

	indexDefs, err := cbgt.PlannerGetIndexDefs(r.cfg, r.version)
	if err != nil {
		return nil, nil, nil, err
	}

	indexDef := indexDefs.IndexDefs[index]
	if indexDef == nil {
		r.Logf("rebalance: assignPIndexesLOCKED,"+
			" empty definitions found for index: %s", index)
		return nil, nil, nil, ErrorNoIndexDefinitionFound
	}

	planPIndexes, cas, err := cbgt.PlannerGetPlanPIndexes(r.cfg, r.version)
	if err != nil {
		return nil, nil, nil, err
	}

	formerPrimaryNodes := make([]string, len(pms))
	for i, pm := range pms {
		formerPrimaryNodes[i], err = r.updatePlanPIndexesLOCKED(planPIndexes,
			indexDef, pm.name, node, pm.stateOps[next].State,
			pm.stateOps[next].Op)
		if err != nil {
			return nil, nil, nil,
				fmt.Errorf("updatePlanPIndexesLOCKED err: %v, %w",
					err, ErrorConcurrentPlannerInProgress)
		}
	}

	if r.optionsReb.DryRun {
		return nil, nil, formerPrimaryNodes, nil
	}

	_, err = cbgt.CfgSetPlanPIndexes(r.cfg, planPIndexes, cas)
	if err != nil {
		return nil, nil, nil, err
	}

	return indexDef, planPIndexes, formerPrimaryNodes, err
}

// --------------------------------------------------------

// assignPIndexCurrStatesLOCKED validates the state transition is
// proper and then updates currStates to the assigned
// index/pindex/node/state/op.
func (r *Rebalancer) assignPIndexCurrStatesLOCKED(
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

// updatePlanPIndexesLOCKED modifies the planPIndexes in/out param
// based on the indexDef/node/state/op params, and may return an error
// if the state transition is invalid.
func (r *Rebalancer) updatePlanPIndexesLOCKED(
	planPIndexes *cbgt.PlanPIndexes, indexDef *cbgt.IndexDef,
	pindex, node, state, op string) (string, error) {
	planPIndex, err := r.getPlanPIndexLOCKED(planPIndexes, pindex)
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
			// check whether the index definition is still alive,
			// if not, then propagate the ErrorNoIndexDefinitionFound.
			indexDefs, err := cbgt.PlannerGetIndexDefs(r.cfg, r.version)
			if err != nil {
				return "", err
			}
			indexDef := indexDefs.IndexDefs[indexDef.Name]
			if indexDef == nil {
				return "", fmt.Errorf("updatePlanPIndexes: planPIndex "+
					" and index missing, pindex: %s, index: %s, err: %w",
					indexDef.Name, pindex, ErrorNoIndexDefinitionFound)
			}

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

// getPlanPIndexLOCKED returns the planPIndex, defaulting to the
// endPlanPIndex's definition if necessary.
func (r *Rebalancer) getPlanPIndexLOCKED(
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

// grabCurrentSample will block until it gets some stats
// information from monitor routine at a 1 sec interval.
func (r *Rebalancer) grabCurrentSample(stopCh, stopCh2 chan struct{},
	pindex, node string) error {
	sampleWantCh := make(chan monitor.MonitorSample)
	select {
	case <-stopCh:
		return blance.ErrorStopped

	case <-stopCh2:
		return blance.ErrorStopped

	case r.monitorSampleWantCh <- sampleWantCh:
		for s := range sampleWantCh {
			if node == s.UUID {
				if s.Data == nil {
					return fmt.Errorf("rebalance: grabCurrentSample, "+
						"empty response for node: %s", s.UUID)
				}

				// err upon not finding the pindex data in
				// the stats response since that could indicate an index deletion
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
					return err
				}

				if _, exists := m.PIndexes[pindex]; !exists {
					return ErrorNoIndexDefinitionFound
				}
			}
		}
	}

	return nil
}

// --------------------------------------------------------

// waitAssignPIndexDone will block until stopped or until an
// index/pindex/node/state/op transition is complete.
func (r *Rebalancer) waitAssignPIndexDone(stopCh, stopCh2 chan struct{},
	indexDef *cbgt.IndexDef,
	planPIndexes *cbgt.PlanPIndexes,
	pindex, node, state, op, formerPrimaryNode string,
	forceWaitForCatchup bool) error {
	if op == "del" {
		return nil // TODO: Handle op del better.
	}

	if state == "replica" && !forceWaitForCatchup {
		// No need to wait for a replica pindex to be "caught up".
		return nil
	}

	if formerPrimaryNode == "" {
		// There was no previous primary pindex on some node to be
		// "caught up" against.
		return nil
	}

	r.m.Lock()
	planPIndex, err := r.getPlanPIndexLOCKED(planPIndexes, pindex)
	r.m.Unlock()

	if err != nil {
		return err
	}

	sourcePartitions := strings.Split(planPIndex.SourcePartitions, ",")

	errThreshold := StatsSampleErrorThreshold
	if r.optionsReb.StatsSampleErrorThreshold != nil {
		errThreshold = uint8(*r.optionsReb.StatsSampleErrorThreshold)
	}

	timeout := time.Second *
		time.Duration(r.optionsReb.SeqChecksTimeoutInSec)

	// Loop to retrieve all the seqs that we need to reach for all
	// source partitions.
	if !r.optionsReb.SkipSeqChecks {
		for _, sourcePartition := range sourcePartitions {
			start := time.Now()

		INIT_WANT_SEQ:
			for {
				_, exists := r.getUUIDSeq(r.wantSeqs, pindex,
					sourcePartition, node)
				if exists {
					break INIT_WANT_SEQ
				}
				uuidSeqWant, exists := r.getUUIDSeq(r.currSeqs, pindex,
					sourcePartition, formerPrimaryNode)
				if exists {
					r.setUUIDSeq(r.wantSeqs, pindex, sourcePartition, node,
						uuidSeqWant.UUID, uuidSeqWant.Seq, uuidSeqWant.SourceSeq)
				} else {
					r.Logf("rebalance: waitAssignPIndexDone,"+
						" awaiting a stats sample grab for pindex %s, partition %s,"+
						" formerPrimaryNode %s", pindex, sourcePartition, formerPrimaryNode)
					err := r.grabCurrentSample(stopCh, stopCh2, pindex, formerPrimaryNode)
					if err != nil {
						// adding more resiliency with pindex not found errors to safe guard against
						// any plan propagation or implementation lag at the remote nodes.
						if err == ErrorNoIndexDefinitionFound && errThreshold > 0 {
							errThreshold--
							continue INIT_WANT_SEQ
						}

						r.Logf("rebalance: waitAssignPIndexDone,"+
							" failed for pindex: %s, err: %+v", pindex, err)
						return err
					}

					// skip sequence checks for this partition upon timeout.
					if timeout > 0 && time.Now().Sub(start) > timeout {
						r.Logf("rebalance: waitAssignPIndexDone,"+
							" skipping a stats sample grab for pindex %s, partition %s,"+
							" formerPrimaryNode %s, on timeout %d secs",
							pindex, sourcePartition, formerPrimaryNode, timeout)
						break INIT_WANT_SEQ
					}
				}
			}
		}
	}

	// Loop to wait until we're caught up to the wanted seq for all
	// source partitions.
	//
	// TODO: Give up after waiting too long.
	// TODO: Claim success and proceed if we see it's converging.
CATCHUP_CUR_SEQ:
	for _, sourcePartition := range sourcePartitions {
		start := time.Now()
		uuidSeqWant, exists := r.getUUIDSeq(r.wantSeqs, pindex,
			sourcePartition, node)
		if !exists && !r.optionsReb.SkipSeqChecks &&
			r.optionsReb.SeqChecksTimeoutInSec == 0 {
			return fmt.Errorf("rebalance:"+
				" waitAssignPIndexDone, could not find uuidSeqWant,"+
				" indexDef: %#v, pindex: %s, sourcePartition: %s, node: %s,"+
				" state: %q, op: %s",
				indexDef, pindex, sourcePartition, node, state, op)
		}

		uuidSeqPrev, reached, err := r.uuidSeqReached(indexDef.Name,
			pindex, sourcePartition, node, uuidSeqWant)
		if err != nil {
			return err
		}

		if reached {
			//moving onto the next sp
			continue
		}

		caughtUp := false

		catchUpTimeout := func(uuidSeqCurr, uuidSeqPrev *cbgt.UUIDSeq) bool {
			if uuidSeqCurr.Seq == uuidSeqPrev.Seq {
				if time.Now().Sub(start) > timeout {
					r.Logf("rebalance: waitAssignPIndexDone,"+
						" skipping seq catch up for pindex %s,"+
						" partition %s, node %s, upon timeout %d secs",
						pindex, sourcePartition, node, timeout)
					return true
				}
				return false
			}

			// reset the timer upon any seq number progress.
			start = time.Now()
			uuidSeqPrev = uuidSeqCurr
			return false
		}

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
							" index: %s, sourcePartition: %s, node: %s,"+
							" state: %q, op: %s, uuidSeqWant: %+v,"+
							" sample: %#v",
							indexDef.Name, sourcePartition, node,
							state, op, uuidSeqWant, sample)

						continue
					}

					if sample.Kind == "/api/stats?partitions=true" {
						uuidSeqCurr, reached, err := r.uuidSeqReached(indexDef.Name,
							pindex, sourcePartition, node, uuidSeqWant)
						if err != nil {
							sampleErr = err
						} else {
							caughtUp = caughtUp || reached

							r.progressCh <- RebalanceProgress{}
						}

						// Skip the seq number catch up wait for the
						// partition on the new node if there is no
						// progress made before the specified timeout.
						if timeout > 0 && err != nil &&
							catchUpTimeout(uuidSeqCurr, uuidSeqPrev) {
							continue CATCHUP_CUR_SEQ
						}

						// At the same polling frequency as stats, query cbgt
						// Manager to verify that the index we are waiting
						// on has not been deleted.
						if r.optionsReb.Manager != nil {
							idxDef, err := r.optionsReb.Manager.
								CheckAndGetIndexDef(indexDef.Name, false)
							if err != nil && err != cbgt.ErrNoIndexDefs {
								r.Logf("rebalance:"+
									" waitAssignPIndexDone GetIndex error,"+
									" unable to get index definitions, err: %s"+
									" index: %s,"+
									" sourcePartition: %s, node: %s,"+
									" state: %q, op: %s, uuidSeqWant: %+v,"+
									" sample: %#v",
									err.Error(), indexDef.Name, sourcePartition, node,
									state, op, uuidSeqWant, sample)
								return err
							}
							if idxDef == nil || indexDef.UUID != idxDef.UUID {
								r.Logf("rebalance:"+
									" waitAssignPIndexDone index missing!,"+
									" index: %s,"+
									" sourcePartition: %s, node: %s,"+
									" state: %q, op: %s, uuidSeqWant: %+v,"+
									" sample: %#v",
									indexDef.Name, sourcePartition, node,
									state, op, uuidSeqWant, sample)
								return ErrorNoIndexDefinitionFound
							}
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
	uuidSeqWant cbgt.UUIDSeq) (*cbgt.UUIDSeq, bool, error) {
	if r.optionsReb.SkipSeqChecks {
		return nil, true, nil
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

		// check whether the new partition caught up with the
		// former partition.
		if uuidSeqCurr.Seq >= uuidSeqWant.Seq {
			return &uuidSeqCurr, true, nil
		}
		// check whether the new partition has caught up with
		// the minimum source partition sequence number from
		// the source stats of former/new fts nodes.
		// Source partition seq numbers should be same across
		// former/new fts nodes, but this is to be on the safer
		// side with any unforeseen kv rebalance/failover
		// complications.
		sourceSeqMin := uuidSeqWant.SourceSeq
		if sourceSeqMin > uuidSeqCurr.SourceSeq {
			sourceSeqMin = uuidSeqCurr.SourceSeq
		}
		if uuidSeqCurr.Seq >= sourceSeqMin {
			return &uuidSeqCurr, true, nil
		}
	}

	return &uuidSeqCurr, false, nil
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
	uuid string, seq uint64, sourceSeq uint64) (
	uuidSeqPrev cbgt.UUIDSeq, uuidSeqPrevExists bool) {
	r.m.Lock()
	uuidSeqPrev, uuidSeqPrevExists =
		SetUUIDSeq(m, pindex, sourcePartition, node, uuid, seq, sourceSeq)
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
	uuid string, seq uint64, sourceSeq uint64) (
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
		UUID:      uuid,
		Seq:       seq,
		SourceSeq: sourceSeq,
	}

	return uuidSeqPrev, uuidSeqPrevExists
}

// --------------------------------------------------------

// runMonitor handles any error from the nodes monitoring subsystem by
// stopping the rebalance.
func (r *Rebalancer) runMonitor(stopCh chan struct{}) {
	defer close(r.monitorDoneCh)

	errMap := make(map[string]uint8, len(r.nodesAll))

	errThreshold := StatsSampleErrorThreshold
	if r.optionsReb.StatsSampleErrorThreshold != nil {
		errThreshold = uint8(*r.optionsReb.StatsSampleErrorThreshold)
	}

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
				errMap[s.UUID]++
				if errMap[s.UUID] < errThreshold {
					r.Logf("rebalance: runMonitor, ignoring the s.Error: %#v, "+
						"for node: %s, for %d time", s.Error, s.UUID,
						errMap[s.UUID])
					continue
				}

				r.Logf("rebalance: runMonitor, s.Error: %#v", s.Error)

				r.progressCh <- RebalanceProgress{Error: s.Error}
				r.Stop() // Stop the rebalance.
				continue
			}

			if s.Kind == "/api/stats?partitions=true" {
				if s.Data == nil {
					errMap[s.UUID]++
					if errMap[s.UUID] < errThreshold {
						r.Logf("rebalance: runMonitor, skipping the empty response"+
							"for node: %s, for %d time", s.UUID, errMap[s.UUID])
						continue
					}
				}

				// reset the error resiliency count to zero upon a successful response.
				errMap[s.UUID] = 0

				m := struct {
					PIndexes map[string]struct {
						Partitions map[string]struct {
							UUID      string `json:"uuid"`
							Seq       uint64 `json:"seq"`
							SourceSeq uint64 `json:"sourceSeq,omitempty"`
						} `json:"partitions"`

						CopyPartitionStats struct {
							TransferProgress float64 `json:"transferProgress,omitempty"`
						} `json:"copyPartitionStats,omitempty"`
					} `json:"pindexes"`
				}{}

				err := json.Unmarshal(s.Data, &m)
				if err != nil {
					r.Logf("rebalance: runMonitor json, s.Data: %s, err: %#v",
						s.Data, err)

					r.progressCh <- RebalanceProgress{Error: err}
					r.Stop() // Stop the rebalance.
					continue
				}

				// reset the error count, as rebalance supposed to fail only
				// if it hits a sequential run of errors for a given node.
				errMap[s.UUID] = 0

				for pindex, x := range m.PIndexes {
					r.m.Lock()
					r.transferProgress[pindex] = x.CopyPartitionStats.TransferProgress
					r.m.Unlock()

					for sourcePartition, uuidSeq := range x.Partitions {
						uuidSeqPrev, uuidSeqPrevExists := r.setUUIDSeq(
							r.currSeqs, pindex, sourcePartition,
							s.UUID, uuidSeq.UUID, uuidSeq.Seq, uuidSeq.SourceSeq)
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
