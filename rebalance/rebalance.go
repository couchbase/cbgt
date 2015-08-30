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
	// Optional, defaults to http.Get(); this is used, for example,
	// for unit testing.
	HttpGet func(url string) (resp *http.Response, err error)
}

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

	// Map of index -> partition -> node -> StateOp.
	currStates CurrStates

	// Map of pindex -> partition -> node -> cbgt.UUIDSeq.
	currSeqs CurrSeqs

	stopCh chan struct{} // Closed by app or when there's an error.
}

// Map of index -> partition -> node -> StateOp.
type CurrStates map[string]map[string]map[string]StateOp

// A StateOp is used to track state transitions and associates a state
// (i.e., "master") with an op (e.g., "add", "del").
type StateOp struct {
	state string
	op    string // May be "" for unknown or no in-flight op.
}

// Map of pindex -> partition -> node -> cbgt.UUIDSeq.
type CurrSeqs map[string]map[string]map[string]cbgt.UUIDSeq

// --------------------------------------------------------

// StartRebalance begins a concurrent, cluster-wide rebalancing of all
// the indexes (and their index partitions) on a cluster of cbgt
// nodes.  StartRebalance utilizes the blance library for calculating
// and orchestrating partition reassignments and the cbgt/rest/monitor
// library to watch for progress and errors.
func StartRebalance(version string, cfg cbgt.Cfg, server string,
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

	log.Printf("rebalance: nodesAll: %#v", nodesAll)
	log.Printf("rebalance: nodesToAdd: %#v", nodesToAdd)
	log.Printf("rebalance: nodesToRemove: %#v", nodesToRemove)
	log.Printf("rebalance: nodeWeights: %#v", nodeWeights)
	log.Printf("rebalance: nodeHierarchy: %#v", nodeHierarchy)
	log.Printf("rebalance: begIndexDefs: %#v", begIndexDefs)
	log.Printf("rebalance: begNodeDefs: %#v", begNodeDefs)

	begPlanPIndexesJSON, _ := json.Marshal(begPlanPIndexes)

	log.Printf("rebalance: begPlanPIndexes: %s, cas: %v",
		begPlanPIndexesJSON, begPlanPIndexesCAS)

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

// rebalanceIndexes rebalances each index, one at a time.
func (r *rebalancer) runRebalanceIndexes(stopCh chan struct{}) {
	for _, indexDef := range r.begIndexDefs.IndexDefs {
		select {
		case <-stopCh:
			break
		default:
		}

		_, err := r.rebalanceIndex(indexDef)
		if err != nil {
			log.Printf("run: indexDef.Name: %s, err: %#v",
				indexDef.Name, err)
		}
	}

	// Completion of rebalance operation, whether naturally or due to
	// error/Stop(), reaches the cleanup codepath here.  We wait for
	// runMonitor() to finish as it may have more sends to progressCh.
	//
	r.Stop()

	r.monitor.Stop()

	<-r.monitorDoneCh

	close(r.progressCh)
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
		progressChanges := cbgt.StructChanges(lastProgress, progress)

		log.Printf("   indexDef.Name: %s, progress: %d, %+v",
			indexDef.Name, numProgress, progressChanges)

		log.Printf("   indexDef.Name: %s, progress: %d, %+v",
			indexDef.Name, numProgress, progress)

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

	if len(lastProgress.Errors) > 0 {
		// TODO: Propagate all errors better.
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

	// TODO: handle blance ffwd plan warnings better?

	r.endPlanPIndexes.Warnings[indexDef.Name] = warnings

	for _, warning := range warnings {
		log.Printf("  calcBegEndMaps: indexDef.Name: %s,"+
			" BlancePlanPIndexes warning: %q, indexDef: %#v",
			indexDef.Name, warning, indexDef)
	}

	j, _ := json.Marshal(r.endPlanPIndexes)
	log.Printf("  calcBegEndMaps: indexDef.Name: %s,"+
		" endPlanPIndexes: %s", indexDef.Name, j)

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

	return r.waitAssignPartitionDone(stopCh, indexDef, planPIndexes,
		partition, node, state, op)
}

// assignPartitionCurrStates validates the state transition is proper
// and then updates currStates to the assigned
// index/partition/node/state/op.
func (r *rebalancer) assignPartitionCurrStates(
	index, partition, node, state, op string) error {
	r.m.Lock()

	partitions, exists := r.currStates[index]
	if !exists || partitions == nil {
		partitions = map[string]map[string]StateOp{}
		r.currStates[index] = partitions
	}

	nodes, exists := partitions[partition]
	if !exists || nodes == nil {
		nodes = map[string]StateOp{}
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

	nodes[node] = StateOp{state, op}

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

// --------------------------------------------------------

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

// --------------------------------------------------------

// waitAssignPartitionDone will block until stopped or until an
// index/partition/node/state/op transition is complete.
func (r *rebalancer) waitAssignPartitionDone(stopCh chan struct{},
	indexDef *cbgt.IndexDef,
	planPIndexes *cbgt.PlanPIndexes,
	partition, node, state, op string) error {
	feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
	if !exists || feedType == nil || feedType.PartitionSeqs == nil {
		return nil
	}

	planPIndex, err := r.getPIndex(planPIndexes, partition)
	if err != nil {
		return err
	}

	// TODO: Give up after waiting too long.
	// TODO: Or, if we see it's converging.
	for {
		partitions, err := feedType.PartitionSeqs(indexDef.SourceType,
			indexDef.SourceName,
			indexDef.SourceUUID,
			indexDef.SourceParams, r.server)
		if err != nil {
			return err
		}

		uuidSeqWant, exists := partitions[partition]
		if !exists {
			return fmt.Errorf("rebalance:"+
				" waitAssignPartitionDone,"+
				" missing partition from PartitionSeqs, indexDef: %#v,"+
				" partition: %s, node: %s, state: %s, op: %s,"+
				" partitions: %#v", indexDef, partition, node, state, op,
				partitions)
		}

		for {
			sampleWantCh := make(chan monitor.MonitorSample)

			select {
			case <-stopCh:
				return blance.ErrorStopped

			case r.monitorSampleWantCh <- sampleWantCh:
				for sample := range sampleWantCh {
					if sample.Error != nil {
						continue
					}

					if sample.Kind == "/api/stats" {
						var uuidSeqCurr cbgt.UUIDSeq

						r.m.Lock()
						partitions, exists := r.currSeqs[planPIndex.Name]
						if exists && partitions != nil {
							nodes, exists := partitions[partition]
							if exists && nodes != nil {
								uuidSeq, exists := nodes[node]
								if exists {
									uuidSeqCurr = uuidSeq
								}
							}
						}
						r.m.Unlock()

						if uuidSeqCurr.UUID == uuidSeqWant.UUID &&
							uuidSeqCurr.Seq >= uuidSeqWant.Seq {
							return nil
						}
					}
				}
			}
		}
	}

	return nil
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

			if s.Error != nil {
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
					r.progressCh <- RebalanceProgress{Error: err}
					r.Stop() // Stop the rebalance.
					continue
				}

				for pindex, x := range m.PIndexes {
					for partition, uuidSeq := range x.Partitions {
						r.m.Lock()

						partitions, exists := r.currSeqs[pindex]
						if !exists || partitions == nil {
							partitions = map[string]map[string]cbgt.UUIDSeq{}
							r.currSeqs[pindex] = partitions
						}

						nodes, exists := partitions[partition]
						if !exists || nodes == nil {
							nodes = map[string]cbgt.UUIDSeq{}
							partitions[partition] = nodes
						}

						nodes[s.UUID] = cbgt.UUIDSeq{
							UUID: uuidSeq.UUID,
							Seq:  uuidSeq.Seq,
						}

						r.m.Unlock()
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
