// @author Couchbase <info@couchbase.com>
// @copyright 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/rebalance"

	"github.com/couchbase/cbauth/service"
)

var ErrCtlWrongRev = service.ErrConflict
var ErrCtlCanceled = service.ErrCanceled

// An Ctl might be in the midst of controlling a replan/rebalance,
// where ctl.ctlDoneCh will be non-nil.
//
// If we're in the midst of a replan/rebalance, and another topology
// change request comes in, we stop any existing work and then start
// off a new replan/rebalance, because the new topology change request
// will have the latest, wanted topology.  This might happen if some
// stopChangeTopology request or signal got lost somewhere.
//
type Ctl struct {
	cfg        cbgt.Cfg
	cfgEventCh chan cbgt.CfgEvent

	server     string
	optionsMgr map[string]string
	optionsCtl CtlOptions

	doneCh chan struct{} // Closed by Ctl when Ctl is done.
	initCh chan error    // Closed by Ctl when Ctl is initialized.
	stopCh chan struct{} // Closed by app when Ctl should stop.

	// -----------------------------------
	// The m protects the fields below.
	m sync.Mutex

	revNum       uint64
	revNumWaitCh chan struct{} // Closed when revNum changes.

	memberNodes         []CtlNode // May be nil before initCh closed.
	prevMemberNodeUUIDs []string

	// The following ctlXxxx fields are all nil or non-nil together
	// depending on whether a change topology is inflight.

	ctlDoneCh         chan struct{}
	ctlStopCh         chan struct{}
	ctlChangeTopology *CtlChangeTopology

	// Warnings from previous operation, keyed by index name.
	prevWarnings map[string][]string

	// Errs from previous operation.
	prevErrs []error

	// Handle to the current rebalancer
	r *rebalance.Rebalancer

	movingPartitionsCount int
}

type CtlOptions struct {
	DryRun                             bool
	Verbose                            int
	FavorMinNodes                      bool
	WaitForMemberNodes                 int // Seconds to wait for wanted member nodes to appear.
	MaxConcurrentPartitionMovesPerNode int
	Manager                            *cbgt.Manager
}

type CtlNode struct {
	UUID string // Cluster manager assigned opaque UUID for a service on a node.

	// URL of the node’s service manager (i.e., cbft's REST endpoint).
	ServiceURL string

	// URL of the node’s cluster manager (i.e., ns-server’s REST endpoint).
	ManagerURL string
}

type CtlTopology struct {
	// Rev is a CAS opaque identifier.  Any change to any CtlTopology
	// field will mean a Rev change.
	Rev string

	// MemberNodes lists what the service thinks are the currently
	// "confirmed in" service nodes in the system.  MemberNodes field
	// can change during the midst of a topology change, but it is
	// service specific on when and how MemberNodes will change and
	// stabilize.
	MemberNodes []CtlNode

	// PrevWarnings holds the warnings from the previous operations
	// and topology changes.  NOTE: If the service manager (i.e., Ctl)
	// restarts, it may "forget" its previous PrevWarnings
	// field value (as perhaps it was only tracked in memory).
	PrevWarnings map[string][]string

	// PrevErrs holds the errors from the previous operations and
	// topology changes.  NOTE: If the service manager (i.e., Ctl)
	// restarts, it may "forget" its previous PrevErrs field value
	// (as perhaps it was only tracked in memory).
	PrevErrs []error

	// ChangeTopology will be non-nil when a service topology change
	// is in progress.
	ChangeTopology *CtlChangeTopology
}

type CtlChangeTopology struct {
	Rev string // Works as CAS, so use the last Topology response’s Rev.

	// Use Mode of "failover-hard" for hard failover.
	// Use Mode of "failover-graceful" for graceful failover.
	// Use Mode of "rebalance" for rebalance-style, clean and safe topology change.
	Mode string

	// The MemberNodeUUIDs are the service nodes that should remain in
	// the service cluster after the topology change is finished.
	// When Mode is a variant of failover, then there should not be
	// any new nodes added to the MemberNodes (only service node
	// removal is allowed on failover).
	MemberNodeUUIDs []string

	// The EjectNodeUUIDs are the service nodes that should be removed from
	// the service cluster after the topology change is finished.
	EjectNodeUUIDs []string
}

// CtlOnProgressFunc defines the callback func signature that's
// invoked during change topology operations.
type CtlOnProgressFunc func(
	maxNodeLen, maxPIndexLen int,
	seenNodes map[string]bool,
	seenNodesSorted []string,
	seenPIndexes map[string]bool,
	seenPIndexesSorted []string,
	progressEntries map[string]map[string]map[string]*rebalance.ProgressEntry,
	errs []error,
) string

// ----------------------------------------------------

func StartCtl(cfg cbgt.Cfg, server string,
	optionsMgr map[string]string, optionsCtl CtlOptions) (
	*Ctl, error) {
	ctl := &Ctl{
		cfg:        cfg,
		cfgEventCh: make(chan cbgt.CfgEvent),
		server:     server,
		optionsMgr: optionsMgr,
		optionsCtl: optionsCtl,
		doneCh:     make(chan struct{}),
		initCh:     make(chan error),
		stopCh:     make(chan struct{}),
		revNum:     1,
	}

	go ctl.run()

	return ctl, <-ctl.initCh
}

// ----------------------------------------------------
func (ctl *Ctl) getManagerOptions() map[string]string {
	return ctl.optionsCtl.Manager.GetOptions()
}

// ----------------------------------------------------
func (ctl *Ctl) getMovingPartitionsCount(keepNodeUUIDs, existingNodes []string) (
	int, error) {
	numNewNodes := len(cbgt.StringsRemoveStrings(keepNodeUUIDs, existingNodes))

	nodesToRemove, err := ctl.waitForWantedNodes(keepNodeUUIDs, true, nil)
	if err != nil {
		log.Warnf("ctl: getMovingPartitionsCount, waitForWantedNodes failed,"+
			" err: %v", err)
		return 0, err
	}
	indexDefs, _, err := cbgt.CfgGetIndexDefs(ctl.cfg)
	if err != nil {
		log.Warnf("ctl: getMovingPartitionsCount, CfgGetIndexDefs failed,"+
			" err: %v", err)
		return 0, err
	}

	totalPartitions := 0
	if indexDefs != nil {
		for _, indexDef := range indexDefs.IndexDefs {
			feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
			if !exists || feedType == nil {
				continue
			}
			partitions, err := feedType.Partitions(indexDef.SourceType,
				indexDef.SourceName, indexDef.SourceUUID,
				indexDef.SourceParams,
				ctl.optionsCtl.Manager.Server(),
				ctl.optionsCtl.Manager.GetOptions())
			if err != nil {
				log.Warnf("ctl: getMovingPartitionsCount, CouchbasePartitions"+
					" failed for index: `%v` over source: `%v`, err: %v",
					indexDef.Name, indexDef.SourceName, err)
				totalPartitions += 0
				continue
			}

			numVbuckets := float64(len(partitions))
			effectiveReplicas := math.Min(float64(indexDef.PlanParams.NumReplicas+1),
				float64(len(keepNodeUUIDs)))
			if indexDef.PlanParams.MaxPartitionsPerPIndex > 0 {
				totalPartitions += int(math.Ceil(numVbuckets/
					float64(indexDef.PlanParams.MaxPartitionsPerPIndex)) *
					effectiveReplicas)
			} else {
				// handle the single partition case
				totalPartitions += int(effectiveReplicas)
			}
		}
	}

	mpCount := cbgt.CalcMovingPartitionsCount(len(keepNodeUUIDs),
		len(nodesToRemove), numNewNodes, len(existingNodes), totalPartitions)

	log.Printf("ctl: getMovingPartitionsCount: %d", mpCount)

	return mpCount, nil
}

// ----------------------------------------------------

func (ctl *Ctl) Stop() error {
	close(ctl.stopCh)

	<-ctl.doneCh

	return nil
}

// ----------------------------------------------------

func (ctl *Ctl) run() {
	defer close(ctl.doneCh)

	memberNodes, err := CurrentMemberNodes(ctl.cfg)
	if err != nil {
		log.Printf("ctl: run, CurrentMemberNodes err %v", err)
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	planPIndexes, _, err := cbgt.PlannerGetPlanPIndexes(ctl.cfg,
		cbgt.CfgGetVersion(ctl.cfg))
	if err != nil {
		log.Errorf("ctl: run, PlannerGetPlanPIndexes err: %v", err)
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	ctl.m.Lock()

	ctl.incRevNumLOCKED()

	ctl.memberNodes = memberNodes
	for _, node := range memberNodes {
		ctl.prevMemberNodeUUIDs = append(ctl.prevMemberNodeUUIDs, node.UUID)
	}

	if planPIndexes != nil {
		ctl.prevWarnings = planPIndexes.Warnings
	}

	ctl.m.Unlock()

	// -----------------------------------------------------------

	err = ctl.cfg.Subscribe(cbgt.INDEX_DEFS_KEY, ctl.cfgEventCh)
	if err != nil {
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	var lastIndexDefs *cbgt.IndexDefs

	kickIndexDefs := func(kind string) error {
		log.Printf("ctl: kickIndexDefs, kind: %s", kind)

		indexDefs, _, err2 := cbgt.CfgGetIndexDefs(ctl.cfg)
		if err2 != nil {
			log.Warnf("ctl: kickIndexDefs, kind: %s, CfgGetIndexDefs,"+
				" err: %v", kind, err2)
			return err2
		}

		if kind == "init" || kind == "force" || kind == "force-indexDefs" ||
			!reflect.DeepEqual(lastIndexDefs, indexDefs) {
			err = ctl.IndexDefsChanged()
			if err != nil {
				log.Warnf("ctl: kickIndexDefs, kind: %s, IndexDefsChanged,"+
					" err: %v", kind, err)
			}
		}

		lastIndexDefs = indexDefs

		return err
	}

	nodeDefns := cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_WANTED)
	err = ctl.cfg.Subscribe(nodeDefns, ctl.cfgEventCh)
	if err != nil {
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	err = kickIndexDefs("init")
	if err != nil {
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	// -----------------------------------------------------------

	close(ctl.initCh)

	for {
		select {
		case <-ctl.stopCh:
			ctl.dispatchCtl("", "stop", nil, nil)
			return

		case ev := <-ctl.cfgEventCh:
			kickIndexDefs("cfgEvent")
			if ev.Key == nodeDefns {
				memberNodes, err = CurrentMemberNodes(ctl.cfg)
				if err != nil {
					log.Printf("ctl: run, kind: %s, CurrentMemberNodes,"+
						" err: %v", ev.Key, err)
					continue
				}
				memberUUIDs := "{"
				for _, node := range memberNodes {
					memberUUIDs += node.UUID + ";"
				}
				memberUUIDs += "}"
				log.Printf("ctl: run, kind: %s, updated memberNodes: %s",
					ev.Key, memberUUIDs)
				ctl.m.Lock()
				ctl.memberNodes = memberNodes
				ctl.incRevNumLOCKED()
				ctl.m.Unlock()
			}
		}
	}
}

func updateNodePlanParams(indexDef *cbgt.IndexDef,
	planPIndexesPrev, planPIndexes *cbgt.PlanPIndexes,
	prevPIndex *cbgt.PlanPIndex, pName string) {
	planPIndexes.PlanPIndexes[pName] = &cbgt.PlanPIndex{}
	*planPIndexes.PlanPIndexes[pName] = *prevPIndex

	npp := indexDef.PlanParams.NodePlanParams[""][""]
	canRead := true
	canWrite := true
	// npp could be nil after we reset it back
	if npp != nil {
		for _, v := range planPIndexesPrev.PlanPIndexes[pName].Nodes {
			if (v.CanRead != npp.CanRead || v.CanWrite != npp.CanWrite) ||
				(canRead != npp.CanRead || canWrite != npp.CanWrite) {
				canRead = npp.CanRead
				canWrite = npp.CanWrite
				break
			}
		}
	}

	planPIndexes.PlanPIndexes[pName].Nodes =
		make(map[string]*cbgt.PlanPIndexNode, len(prevPIndex.Nodes))
	for uuid, node := range prevPIndex.Nodes {
		planPIndexes.PlanPIndexes[pName].Nodes[uuid] =
			&cbgt.PlanPIndexNode{CanRead: canRead,
				CanWrite: canWrite,
				Priority: node.Priority}
	}
}

// ----------------------------------------------------

// When the index definitions have changed, our approach is to run the
// planner, but only for brand new indexes that don't have any
// pindexes yet.
func (ctl *Ctl) IndexDefsChanged() (err error) {
	plannerFilterNewIndexesOnly := func(indexDef *cbgt.IndexDef,
		planPIndexesPrev, planPIndexes *cbgt.PlanPIndexes) bool {
		copyPrevPlan := func() {
			// Copy over the previous plan, if any, for the index.
			if planPIndexesPrev != nil && planPIndexes != nil {
				for n, p := range planPIndexesPrev.PlanPIndexes {
					if p.IndexName == indexDef.Name &&
						p.IndexUUID == indexDef.UUID {

						// non nil NodePlanParams indicates some overrides
						if indexDef.PlanParams.NodePlanParams != nil {
							updateNodePlanParams(indexDef,
								planPIndexesPrev, planPIndexes, p, n)
						}

						if planPIndexes.PlanPIndexes[n] == nil {
							planPIndexes.PlanPIndexes[n] = p
						}
						// Copy over previous warnings, if any.
						if planPIndexes.Warnings == nil {
							planPIndexes.Warnings = map[string][]string{}
						}
						if planPIndexesPrev.Warnings != nil {
							prev := planPIndexesPrev.Warnings[indexDef.Name]
							if prev != nil {
								planPIndexes.Warnings[indexDef.Name] = prev
							}
						}
					}
				}
			}
		}

		// Split each indexDef into 1 or more PlanPIndexes.
		planPIndexesForIndex, err := cbgt.SplitIndexDefIntoPlanPIndexes(
			indexDef, ctl.server, ctl.optionsMgr, nil)
		if err != nil {
			copyPrevPlan()
			return false
		}

		for pindexName := range planPIndexesForIndex {
			if planPIndexesPrev.PlanPIndexes[pindexName] != nil {
				copyPrevPlan()
				return false
			}
		}

		return true
	}

	go func() {
		steps := map[string]bool{"planner": true}

		var nodesToRemove []string
		version := cbgt.CfgGetVersion(ctl.cfg)

		cmd.PlannerSteps(steps, ctl.cfg, version,
			ctl.server, ctl.optionsMgr, nodesToRemove, ctl.optionsCtl.DryRun,
			plannerFilterNewIndexesOnly)

		planPIndexes, _, err :=
			cbgt.PlannerGetPlanPIndexes(ctl.cfg, version)
		if err == nil && planPIndexes != nil {
			ctl.m.Lock()
			ctl.incRevNumLOCKED()
			ctl.prevWarnings = planPIndexes.Warnings
			ctl.m.Unlock()
		}
	}()

	return nil
}

// ----------------------------------------------------

// WaitGetTopology is like GetTopology() but will synchronously block
// until there's a rev change.
func (ctl *Ctl) WaitGetTopology(haveRev service.Revision,
	cancelCh <-chan struct{}) (*CtlTopology, error) {
	ctl.m.Lock()

	if len(haveRev) > 0 {
		haveRevNum, err := DecodeRev(haveRev)
		if err != nil {
			ctl.m.Unlock()

			log.Errorf("ctl/ctl: WaitGetTopology, DecodeRev"+
				", haveRev: %s, err: %v", haveRev, err)

			return nil, err
		}

	OUTER:
		for haveRevNum == ctl.revNum {
			if ctl.revNumWaitCh == nil {
				ctl.revNumWaitCh = make(chan struct{})
			}
			revNumWaitCh := ctl.revNumWaitCh // See also incRevNumLOCKED().

			ctl.m.Unlock()
			select {
			case <-cancelCh:
				return nil, ErrCtlCanceled

			case <-time.After(CtlMgrTimeout):
				// TIMEOUT
				ctl.m.Lock()
				break OUTER

			case <-revNumWaitCh:
				// FALLTHRU
			}
			ctl.m.Lock()
		}
	}

	rv := ctl.getTopologyLOCKED()

	ctl.m.Unlock()

	return rv, nil
}

func (ctl *Ctl) incRevNumLOCKED() uint64 {
	ctl.revNum += 1
	rv := ctl.revNum

	if ctl.revNumWaitCh != nil { // See WaitGetTopology().
		close(ctl.revNumWaitCh)
		ctl.revNumWaitCh = nil
	}

	return rv
}

// ----------------------------------------------------

// GetTopology retrieves the current topology.
func (ctl *Ctl) GetTopology() *CtlTopology {
	ctl.m.Lock()
	rv := ctl.getTopologyLOCKED()
	ctl.m.Unlock()

	return rv
}

func (ctl *Ctl) getTopologyLOCKED() *CtlTopology {
	return &CtlTopology{
		Rev:            fmt.Sprintf("%d", ctl.revNum),
		MemberNodes:    ctl.memberNodes,
		PrevWarnings:   ctl.prevWarnings,
		PrevErrs:       ctl.prevErrs,
		ChangeTopology: ctl.ctlChangeTopology,
	}
}

func (ctl *Ctl) getKnownNodeUUIDs() ([]string, error) {
	nodeDefsKnown, _, err :=
		cbgt.CfgGetNodeDefs(ctl.cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		return nil, err
	}
	var rv []string
	for _, nodeDef := range nodeDefsKnown.NodeDefs {
		rv = append(rv, nodeDef.UUID)
	}
	return rv, nil
}

func removeNodeDefs(cfg cbgt.Cfg, uuids []string) error {
	v := cbgt.CfgGetVersion(cfg)
	var err error
	for i := 0; i < len(uuids); i++ {
		err = cbgt.CfgRemoveNodeDefForce(cfg,
			cbgt.NODE_DEFS_KNOWN, uuids[i], v)
		if err != nil {
			log.Printf("ctl: removeNodeDefs, "+
				"CfgRemoveNodeDefForce err: %v", err)
			return err
		}
		err = cbgt.CfgRemoveNodeDefForce(cfg,
			cbgt.NODE_DEFS_WANTED, uuids[i], v)
		if err != nil {
			log.Printf("ctl: removeNodeDefs"+
				"CfgRemoveNodeDefForce err: %v", err)
			return err
		}
	}
	return nil
}

func (ctl *Ctl) purgeStalePlanAndClusterNodes(changeTopology *CtlChangeTopology) {
	if changeTopology.Mode == "rebalance" {
		// With rebalance operations, if there are any unknown/stale
		// nodes present in the cluster then we can infer that
		// there could be some stale cluster information like node
		// definitions/plans in metadata which ought to be cleaned.
		knownNodeUUIDs, err := ctl.getKnownNodeUUIDs()
		if err != nil {
			return
		}
		var clusterNodeUUIDs []string
		clusterNodeUUIDs = changeTopology.MemberNodeUUIDs
		clusterNodeUUIDs = append(clusterNodeUUIDs, changeTopology.EjectNodeUUIDs...)

		staleNodes := cbgt.StringsRemoveStrings(knownNodeUUIDs, clusterNodeUUIDs)
		if len(staleNodes) > 0 {
			// remove or unregister all the stale node defs.
			err := removeNodeDefs(ctl.cfg, staleNodes)
			if err != nil {
				return
			}
			// reset the plan.
			emptyPlan := cbgt.NewPlanPIndexes(cbgt.CfgGetVersion(ctl.cfg))
			_, err = cbgt.CfgSetPlanPIndexes(ctl.cfg, emptyPlan, cbgt.CFG_CAS_FORCE)
			if err != nil {
				log.Printf("ctl: purgeStalePlanAndClusterNodes, "+
					"set plan err: %v", err)
				return
			}
		}
	}
}

// ----------------------------------------------------

// ChangeTopology starts an asynchonous change topology operation, if
// there's no input error.  ChangeTopology also synchronously stops
// any previous, inflight change topology operation, if any, before
// kicking off the new change topology operation.
func (ctl *Ctl) ChangeTopology(changeTopology *CtlChangeTopology,
	cb CtlOnProgressFunc) (topology *CtlTopology, err error) {

	// Handle any stale cluster informations like plans and
	// nodes explicitly,
	ctl.purgeStalePlanAndClusterNodes(changeTopology)

	return ctl.dispatchCtl(
		changeTopology.Rev,
		changeTopology.Mode,
		changeTopology.MemberNodeUUIDs,
		cb)
}

// StopChangeTopology synchronously stops a current change topology
// operation.
func (ctl *Ctl) StopChangeTopology(rev string) {
	ctl.dispatchCtl(rev, "stopChangeTopology", nil, nil)
}

// ----------------------------------------------------

func (ctl *Ctl) dispatchCtl(rev string,
	mode string, memberNodeUUIDs []string, cb CtlOnProgressFunc) (
	*CtlTopology, error) {
	ctl.m.Lock()
	existingNodes := ctl.prevMemberNodeUUIDs
	ctl.m.Unlock()

	movingPartitionsCount, err := ctl.getMovingPartitionsCount(memberNodeUUIDs,
		existingNodes)
	if err != nil {
		return nil, err
	}

	ctl.m.Lock()
	err = ctl.dispatchCtlLOCKED(rev, mode, memberNodeUUIDs,
		movingPartitionsCount, cb)
	topology := ctl.getTopologyLOCKED()
	ctl.m.Unlock()

	return topology, err
}

func (ctl *Ctl) dispatchCtlLOCKED(
	rev string,
	mode string,
	memberNodeUUIDs []string,
	movingPartitionsCount int,
	ctlOnProgress CtlOnProgressFunc) error {
	if rev != "" && rev != fmt.Sprintf("%d", ctl.revNum) {
		return ErrCtlWrongRev
	}

	if ctl.ctlStopCh != nil {
		close(ctl.ctlStopCh)
		ctl.ctlStopCh = nil
	}

	ctlDoneCh := ctl.ctlDoneCh
	if ctlDoneCh != nil {
		// Release lock while waiting for done, so ctl can mutate ctl fields.
		ctl.m.Unlock()
		<-ctlDoneCh
		ctl.m.Lock()
	}

	if ctl.ctlDoneCh == nil &&
		mode != "stop" &&
		mode != "stopChangeTopology" {
		return ctl.startCtlLOCKED(mode, memberNodeUUIDs,
			movingPartitionsCount, ctlOnProgress)
	}

	return nil
}

// ----------------------------------------------------

func (ctl *Ctl) startCtlLOCKED(
	mode string,
	memberNodeUUIDs []string,
	movingPartitionsCount int,
	ctlOnProgress CtlOnProgressFunc) error {
	ctl.incRevNumLOCKED()

	ctlDoneCh := make(chan struct{})
	ctl.ctlDoneCh = ctlDoneCh

	ctlStopCh := make(chan struct{})
	ctl.ctlStopCh = ctlStopCh

	ctlChangeTopology := &CtlChangeTopology{
		Rev:             fmt.Sprintf("%d", ctl.revNum),
		Mode:            mode,
		MemberNodeUUIDs: memberNodeUUIDs,
	}
	ctl.ctlChangeTopology = ctlChangeTopology

	authType := ""
	if ctl.optionsMgr != nil {
		authType = ctl.optionsMgr["authType"]
	}

	httpGetWithAuth := func(urlStr string) (resp *http.Response, err error) {
		if authType == "cbauth" {
			return cbgt.CBAuthHttpGet(urlStr)
		}

		return http.Get(urlStr)
	}

	ctl.movingPartitionsCount = movingPartitionsCount
	ctl.prevMemberNodeUUIDs = memberNodeUUIDs

	// The ctl goroutine.
	//
	go func() {
		var ctlErrs []error
		var ctlWarnings map[string][]string
		version := cbgt.CfgGetVersion(ctl.cfg)
		// Cleanup ctl goroutine.
		//
		defer func() {
			if ctlWarnings == nil {
				// If there were no warnings, see if there were any
				// warnings left in the plan.
				planPIndexes, _, err :=
					cbgt.PlannerGetPlanPIndexes(ctl.cfg, cbgt.CfgGetVersion(ctl.cfg))
				if err == nil {
					if planPIndexes != nil {
						ctlWarnings = planPIndexes.Warnings
					}
				} else {
					ctlErrs = append(ctlErrs, err)
				}
			}

			memberNodes, err := CurrentMemberNodes(ctl.cfg)
			if err != nil {
				ctlErrs = append(ctlErrs, err)
			}

			ctl.m.Lock()

			ctl.incRevNumLOCKED()

			ctl.memberNodes = memberNodes

			if ctl.ctlDoneCh == ctlDoneCh {
				ctl.ctlDoneCh = nil
			}

			if ctl.ctlStopCh == ctlStopCh {
				ctl.ctlStopCh = nil
			}

			if ctl.ctlChangeTopology == ctlChangeTopology {
				ctl.ctlChangeTopology = nil
			}

			ctl.prevWarnings = ctlWarnings
			ctl.prevErrs = ctlErrs

			if ctlOnProgress != nil {
				ctlOnProgress(0, 0, nil, nil, nil, nil, nil, ctlErrs)
			}

			ctl.movingPartitionsCount = 0
			ctl.m.Unlock()

			close(ctlDoneCh)
		}()

		indexDefsStart, err :=
			cbgt.PlannerGetIndexDefs(ctl.cfg, version)
		if err != nil {
			log.Warnf("ctl: PlannerGetIndexDefs, err: %v", err)

			ctlErrs = append(ctlErrs, err)
			return
		}

		// 1) Monitor cfg to wait for wanted nodes to appear.
		ignoreWaitTimeOut := false
		if indexDefsStart == nil ||
			len(indexDefsStart.IndexDefs) == 0 {
			ignoreWaitTimeOut = true
		}
		nodesToRemove, err := ctl.waitForWantedNodes(memberNodeUUIDs, ignoreWaitTimeOut, ctlStopCh)
		if err != nil {
			log.Warnf("ctl: waitForWantedNodes, err: %v", err)
			ctlErrs = append(ctlErrs, err)
			return
		}
		log.Printf("ctl: waitForWantedNodes, nodesToRemove: %+v", nodesToRemove)

		// 2) Run rebalance in a loop (if not failover).
		//
		failover := strings.HasPrefix(mode, "failover")
		if !failover {
			// The loop handles the case if the index definitions had
			// changed during the midst of the rebalance, in which
			// case we run rebalance again.
		REBALANCE_LOOP:
			for {
				// Retrieve the indexDefs before we do anything.
				indexDefsStart, err2 :=
					cbgt.PlannerGetIndexDefs(ctl.cfg, version)
				if err2 != nil {
					log.Warnf("ctl: PlannerGetIndexDefs, err: %v", err2)
					ctlErrs = append(ctlErrs, err2)
					return
				}

				if indexDefsStart == nil ||
					len(indexDefsStart.IndexDefs) <= 0 {
					break REBALANCE_LOOP
				}

				concurrentPartitionsMovesPerNode, found := cbgt.ParseOptionsInt(ctl.getManagerOptions(),
					"maxConcurrentPartitionMovesPerNode")
				if !found {
					concurrentPartitionsMovesPerNode = ctl.optionsCtl.MaxConcurrentPartitionMovesPerNode
				}
				// Start rebalance and monitor progress.
				ctl.r, err = rebalance.StartRebalance(version,
					ctl.cfg, ctl.server, ctl.optionsMgr,
					nodesToRemove,
					rebalance.RebalanceOptions{
						FavorMinNodes:                      ctl.optionsCtl.FavorMinNodes,
						MaxConcurrentPartitionMovesPerNode: concurrentPartitionsMovesPerNode,
						DryRun:                             ctl.optionsCtl.DryRun,
						Verbose:                            ctl.optionsCtl.Verbose,
						HttpGet:                            httpGetWithAuth,
						Manager:                            ctl.optionsCtl.Manager,
					})
				if err != nil {
					log.Warnf("ctl: StartRebalance, err: %v", err)

					ctlErrs = append(ctlErrs, err)
					return
				}

				progressDoneCh := make(chan error)
				go func() {
					defer close(progressDoneCh)

					progressToString := func(maxNodeLen, maxPIndexLen int,
						seenNodes map[string]bool,
						seenNodesSorted []string,
						seenPIndexes map[string]bool,
						seenPIndexesSorted []string,
						progressEntries map[string]map[string]map[string]*rebalance.ProgressEntry,
					) string {
						if ctlOnProgress != nil {
							return ctlOnProgress(maxNodeLen, maxPIndexLen,
								seenNodes,
								seenNodesSorted,
								seenPIndexes,
								seenPIndexesSorted,
								progressEntries,
								nil)
						}

						return rebalance.ProgressTableString(
							maxNodeLen, maxPIndexLen,
							seenNodes,
							seenNodesSorted,
							seenPIndexes,
							seenPIndexesSorted,
							progressEntries)
					}

					err = rebalance.ReportProgress(ctl.r, progressToString)
					if err != nil {
						log.Warnf("ctl: ReportProgress, err: %v", err)
						progressDoneCh <- err
					}
				}()

				defer ctl.r.Stop()

				select {
				case <-ctlStopCh:
					return // Exit ctl goroutine.

				case err = <-progressDoneCh:
					if err != nil {
						ctlErrs = append(ctlErrs, err)
						return
					}
				}

				ctlWarnings = ctl.r.GetEndPlanPIndexes().Warnings

				// Repeat if the indexDefs had changed mid-rebalance.
				indexDefsEnd, err2 :=
					cbgt.PlannerGetIndexDefs(ctl.cfg, version)
				if err2 != nil {
					ctlErrs = append(ctlErrs, err2)
					return
				}

				if reflect.DeepEqual(indexDefsStart, indexDefsEnd) {
					// NOTE: There's a race or hole here where at this
					// point we think the indexDefs haven't changed;
					// but, an adversary could still change the
					// indexDefs before we can run the PlannerSteps().
					break REBALANCE_LOOP
				}
			}
		}

		// 3) Run planner steps, like unregister and failover.
		//
		steps := map[string]bool{"unregister": true}
		if failover {
			steps["failover_"] = true
		} else {
			steps["planner"] = true
		}

		err = cmd.PlannerSteps(steps, ctl.cfg, version,
			ctl.server, ctl.optionsMgr, nodesToRemove,
			ctl.optionsCtl.DryRun, nil)
		if err != nil {
			log.Warnf("ctl: PlannerSteps, err: %v", err)
			ctlErrs = append(ctlErrs, err)
		}
	}()

	return nil
}

// ----------------------------------------------------

// Waits for actual nodeDefsWanted in the cfg to be equal to or a
// superset of wantedNodes, and returns the nodesToRemove.
func (ctl *Ctl) waitForWantedNodes(wantedNodes []string, ignoreWaitTimeOut bool,
	cancelCh <-chan struct{}) ([]string, error) {
	secs := ctl.optionsCtl.WaitForMemberNodes
	if secs <= 0 {
		if ignoreWaitTimeOut {
			secs = 5
		} else {
			secs = 30
		}
	}
	log.Printf("ctl: waitForWantedNodes, wantedNodes: %+v", wantedNodes)
	return WaitForWantedNodes(ctl.cfg, wantedNodes, cancelCh, secs, ignoreWaitTimeOut)
}

// WaitForWantedNodes blocks until the nodeDefsWanted in the cfg is
// equal to or a superset of the provided wantedNodes, and returns the
// "nodes to remove" (actualWantedNodes SET-DIFFERENCE wantedNodes).
func WaitForWantedNodes(cfg cbgt.Cfg, wantedNodes []string,
	cancelCh <-chan struct{}, secs int, ignoreWaitTimeOut bool) (
	[]string, error) {
	var nodeDefWantedUUIDs []string
	for i := 0; i < secs; i++ {
		select {
		case <-cancelCh:
			return nil, ErrCtlCanceled
		default:
		}

		nodeDefsWanted, _, err :=
			cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
		if err != nil {
			return nil, err
		}

		nodeDefWantedUUIDs = nil
		for _, nodeDef := range nodeDefsWanted.NodeDefs {
			nodeDefWantedUUIDs = append(nodeDefWantedUUIDs, nodeDef.UUID)
		}
		if len(cbgt.StringsRemoveStrings(wantedNodes, nodeDefWantedUUIDs)) <= 0 {
			return cbgt.StringsRemoveStrings(nodeDefWantedUUIDs, wantedNodes), nil
		}

		time.Sleep(1 * time.Second)
	}

	if ignoreWaitTimeOut {
		log.Printf("ctl: WaitForWantedNodes ignoreWaitTimeOut")
		return cbgt.StringsRemoveStrings(nodeDefWantedUUIDs, wantedNodes), nil
	}
	return nil, fmt.Errorf("ctl: WaitForWantedNodes"+
		" could not attain wantedNodes: %#v,"+
		" only reached nodeDefWantedUUIDs: %#v",
		wantedNodes, nodeDefWantedUUIDs)
}

// ----------------------------------------------------

func CurrentMemberNodes(cfg cbgt.Cfg) ([]CtlNode, error) {
	nodeDefsWanted, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		return nil, err
	}

	var memberNodes []CtlNode

	for _, nodeDef := range nodeDefsWanted.NodeDefs {
		memberNode := CtlNode{
			UUID:       nodeDef.UUID,
			ServiceURL: "http://" + nodeDef.HostPort,
		}

		if nodeDef.Extras != "" {
			// Early versions of ns_server integration had a simple,
			// non-JSON "host:port" format for nodeDef.Extras, which
			// we use as default.
			nsHostPort := nodeDef.Extras

			var e struct {
				NsHostPort string `json:"nsHostPort"`
			}

			err := json.Unmarshal([]byte(nodeDef.Extras), &e)
			if err != nil {
				nsHostPort = e.NsHostPort
			}

			memberNode.ManagerURL = "http://" + nsHostPort
		}

		memberNodes = append(memberNodes, memberNode)
	}

	return memberNodes, nil
}

// ----------------------------------------------------

func (ctl *Ctl) checkAndReregisterSelf(selfUUID string) {
	memberNodes, err := CurrentMemberNodes(ctl.cfg)
	if err != nil {
		log.Errorf("ctl: CurrentMemberNodes failed, err: %+v", err)
		return
	}
	for _, node := range memberNodes {
		if node.UUID == selfUUID {
			// node already in Cfg
			return
		}
	}
	log.Printf("ctl: checkAndReregisterSelf, current node: %s "+
		" is missing from Cfg", selfUUID)
	if ctl.optionsCtl.Manager != nil {
		log.Printf("ctl: checkAndReregisterSelf, reregistering node: %s ",
			selfUUID)
		err := ctl.optionsCtl.Manager.Register("wanted")
		if err != nil {
			log.Printf("ctl: checkAndReregisterSelf, re register failed, "+
				" err: %+v", err)
		}
	}
}
