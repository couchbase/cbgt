// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package ctl

import (
	"encoding/json"
	"errors"
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
	"github.com/couchbase/cbgt/rest"

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
	m sync.RWMutex

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

	rebOrchestrator bool

	lastIndexDefs *cbgt.IndexDefs
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

// resetPrevWarningsAndErrors is to be called ONLY during
// the PREPARE phase of the cluster operation.
func (ctl *Ctl) resetPrevWarningsAndErrors() {
	ctl.m.Lock()
	ctl.incRevNumLOCKED()
	ctl.prevWarnings = nil
	ctl.prevErrs = nil
	ctl.m.Unlock()
}

func (ctl *Ctl) getManagerOptions() map[string]string {
	return ctl.optionsCtl.Manager.GetOptions()
}

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
		// partitions cache to save redundant calls
		// per sourceName/sourceUUID.
		pCache := make(map[string][]string)
		var partitions []string

		for _, indexDef := range indexDefs.IndexDefs {
			feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
			if !exists || feedType == nil {
				continue
			}

			key := indexDef.SourceName + ":" + indexDef.SourceUUID
			if partitions, exists = pCache[key]; !exists {
				partitions, err = feedType.Partitions(indexDef.SourceType,
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
				pCache[key] = partitions
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

	log.Printf("ctl: getMovingPartitionsCount, keepNodeUUIDs: %+v, "+
		"nodesToRemove: %+v, numNewNodes: %d, existingNodes: %+v, "+
		"totalPartitions: %d", keepNodeUUIDs, nodesToRemove, numNewNodes,
		existingNodes, totalPartitions)

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

// ---------------------------------------------------

// maxDebounceIntervalMS represents the maximum debounce interval.
var maxDebounceIntervalMS = 5000

// ---------------------------------------------------
// cfgDebounceIntervalInMS computes a debounce interval
// to be applied for the config events in a cluster.
// It primarily uses the number of indexes in the system for
// computations as that roughly reflects the amount of meta
// load in a metakv system.
// It also uses the position of the current node's UUID in the
// member node list to avoid the exact debounce interval
// on every node. This ought to help each planner to trigger
// their work at non-overlapping instant of time.
func (ctl *Ctl) cfgDebounceIntervalInMS() int {
	offset, nm := ctl.optionsCtl.Manager.GetCfgDeBounceOffsetAndMultiplier()
	var dinterval int
	var pos int

	ctl.m.RLock()
	if ctl.lastIndexDefs != nil {
		dinterval = len(ctl.lastIndexDefs.IndexDefs) * offset
		// if the interval is greater than maxDebounceIntervalMS then,
		// reset so that it would always be under limits than growing
		// with the number of indexes.
		if dinterval > maxDebounceIntervalMS {
			dinterval = maxDebounceIntervalMS
		}
	}

	for i, node := range ctl.memberNodes {
		if node.UUID == ctl.optionsCtl.Manager.UUID() {
			pos = i + 1
			break
		}
	}
	ctl.m.RUnlock()

	dinterval = dinterval + (pos * nm * offset)

	return dinterval
}

// ---------------------------------------------------

// debounceCfgEvents tries to remove any immediate, spammy
// successive Cfg events to save the planner from performing
// redundant work. If there are no further events within the
// debounce time, then it returns the original input event.
func (ctl *Ctl) debounceCfgEvents(ev cbgt.CfgEvent) cbgt.CfgEvent {
	debounceIntervalInMs := ctl.cfgDebounceIntervalInMS()
	debounceTimeCh := time.After(time.Millisecond * time.Duration(debounceIntervalInMs))

DEBOUNCE_LOOP:
	for {
		select {
		case ev = <-ctl.cfgEventCh:
			// NOOP upon more cfg events.

		case <-debounceTimeCh:
			break DEBOUNCE_LOOP
		}
	}
	log.Printf("ctl: debounceCfgEvents duration: %d MS", debounceIntervalInMs)
	return ev
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

	log.Printf("ctl: cluster member nodes at startup: %+v", ctl.prevMemberNodeUUIDs)

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

	kickIndexDefs := func(kind string) error {
		log.Printf("ctl: kickIndexDefs, kind: %s", kind)

		indexDefs, _, err2 := cbgt.CfgGetIndexDefs(ctl.cfg)
		if err2 != nil {
			log.Warnf("ctl: kickIndexDefs, kind: %s, CfgGetIndexDefs,"+
				" err: %v", kind, err2)
			return err2
		}

		if kind == "init" || kind == "force" || kind == "force-indexDefs" ||
			!reflect.DeepEqual(ctl.lastIndexDefs, indexDefs) {
			err = ctl.IndexDefsChanged()
			if err != nil {
				log.Warnf("ctl: kickIndexDefs, kind: %s, IndexDefsChanged,"+
					" err: %v", kind, err)
			}
		}

		if ctl.lastIndexDefs != nil && indexDefs != nil {
			if !reflect.DeepEqual(ctl.lastIndexDefs, indexDefs) {
				// If there is a change in the cached index definitions,
				// look for any that have been deleted and invoke the
				// OnDelete callback.
				for name, idef := range ctl.lastIndexDefs.IndexDefs {
					if _, exists := indexDefs.IndexDefs[name]; !exists {
						if pindexImplType, exists := cbgt.PIndexImplTypes[idef.Type]; exists {
							if pindexImplType.OnDelete != nil {
								pindexImplType.OnDelete(idef)
							}
						}

						// index definition has been deleted, shut down any associated stats
						// clients (this is needed to close clients on all nodes in the
						// cluster, where the index partitions existed)
						cbgt.CloseStatsClients(idef.SourceName, idef.SourceUUID)
					}
				}
			}
		}

		ctl.lastIndexDefs = indexDefs

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

	eh := func(ev *cbgt.CfgEvent) {
		if ev.Key == cbgt.INDEX_DEFS_KEY {
			// invoking kickIndexDefs() synchronously as
			// the internal plan computation happens on
			// an explicit routine.
			kickIndexDefs("ctl: mgr event")
		}
	}

	ctl.optionsCtl.Manager.RegisterPlannerEventHandlerCallback(eh)

	// -----------------------------------------------------------

	close(ctl.initCh)

	for {
		select {
		case <-ctl.stopCh:
			ctl.dispatchCtl("", "stop", nil, nil)
			return

		case ev := <-ctl.cfgEventCh:
			log.Printf("ctl: cfgEvent, kind: %s", ev.Key)

			if ev.Key == cbgt.INDEX_DEFS_KEY {
				// debounce the indexDef related cfg events.
				ev = ctl.debounceCfgEvents(ev)
			}

			kickIndexDefs("cfgEvent")
			if ev.Key == nodeDefns {
				memberNodes, err = CurrentMemberNodes(ctl.cfg)
				if err != nil {
					log.Printf("ctl: run, kind: %s, CurrentMemberNodes,"+
						" err: %v", ev.Key, err)
					continue
				}
				// initialize the prevMemberNodeUUIDs only if it is
				// unset during the initial boot up of a cluster.
				var initPrevMemberUUIDs bool
				if len(ctl.prevMemberNodeUUIDs) == 0 {
					initPrevMemberUUIDs = true
				}

				memberUUIDs := "{"
				for _, node := range memberNodes {
					memberUUIDs += node.UUID + ";"
					if initPrevMemberUUIDs {
						ctl.prevMemberNodeUUIDs = append(ctl.prevMemberNodeUUIDs, node.UUID)
					}
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
	// check whether the rebalance operation is already in progress.
	if ctl.rebalanceInProgress() {
		log.Printf("ctl: IndexDefsChanged, skipping the planning as the" +
			" rebalance operation is in progress.")
		return
	}

	go func() {
		steps := map[string]bool{"planner": true}

		var nodesToRemove []string
		version := cbgt.CfgGetVersion(ctl.cfg)

		cmd.PlannerSteps(steps, ctl.cfg, version,
			ctl.server, ctl.optionsMgr, nodesToRemove, ctl.optionsCtl.DryRun,
			ctl.plannerFilterNewIndexesOnly)

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

func (ctl *Ctl) plannerFilterNewIndexesOnly(indexDef *cbgt.IndexDef,
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
		// If there hasn't been an update in the ctl.revNum since the last call to
		// GetCurrentTopology(), block till one of the below conditions are met.
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

			// This case will be block till there is an update in the revNum
			// since we close the revNumWaitCh in incRevNumLOCKED().
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

// Rev num is incremented when there is a change in the topology.
// eEg. in the Prepare phase, on changing index defs, etc.
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
			httpClient := cbgt.HttpClient()
			if httpClient != nil {
				return cbgt.CBAuthHttpGetWithClient(urlStr, httpClient)
			}
			return cbgt.CBAuthHttpGet(urlStr)
		}

		return cbgt.HttpClient().Get(urlStr)
	}

	ctl.movingPartitionsCount = movingPartitionsCount
	existingNodeUUIDs := ctl.prevMemberNodeUUIDs
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

			ctl.rebOrchestrator = false

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

				seqChecksTimeoutInSec, _ := cbgt.ParseOptionsInt(ctl.getManagerOptions(),
					"seqChecksTimeoutInSec")

				// Start rebalance and monitor progress.
				ctl.r, err = rebalance.StartRebalance(version,
					ctl.cfg, ctl.server, ctl.optionsMgr,
					nodesToRemove,
					rebalance.RebalanceOptions{
						FavorMinNodes:                      ctl.optionsCtl.FavorMinNodes,
						MaxConcurrentPartitionMovesPerNode: concurrentPartitionsMovesPerNode,
						SeqChecksTimeoutInSec:              seqChecksTimeoutInSec,
						DryRun:                             ctl.optionsCtl.DryRun,
						Verbose:                            ctl.optionsCtl.Verbose,
						HttpGet:                            httpGetWithAuth,
						Manager:                            ctl.optionsCtl.Manager,
						ExistingNodes:                      existingNodeUUIDs,
					})
				if err != nil {
					log.Warnf("ctl: StartRebalance, err: %v", err)

					ctlErrs = append(ctlErrs, err)
					return
				}

				// If StartRebalance() returned a nil rebalance.Rebalancer,
				// with no error, then it means rebalance should be skipped.
				if ctl.r == nil && err == nil {
					log.Printf("ctl: StartRebalance returned nil rebalancer, " +
						"without error, skipping rebalance")
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
						// retry the rebalance upon concurrent planner operations.
						if !errors.Is(err, rebalance.ErrorConcurrentPlannerInProgress) {
							progressDoneCh <- err
						}
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
				} else {
					log.Printf("ctl: Retrying the rebalance since the index" +
						" definitions have changed during the rebalance.")
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
			ctl.optionsCtl.DryRun, ctl.plannerFilterNewIndexesOnly)
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
		hostPortUrl := "http://" + nodeDef.HostPort
		if u, err := nodeDef.HttpsURL(); err == nil {
			hostPortUrl = u
		}

		memberNode := CtlNode{
			UUID:       nodeDef.UUID,
			ServiceURL: hostPortUrl,
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

			// non-ssl nsHostPort always available for communication
			// from service via localhost
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

// ----------------------------------------------------

func (ctl *Ctl) rebalanceOrchestrator() bool {
	return ctl.rebOrchestrator
}

// ----------------------------------------------------

func (ctl *Ctl) rebalanceInProgress() bool {
	// check whether the rebalance operation is
	// already in progress internally.
	if ctl.rebOrchestrator {
		return true
	}

	// check with the cluster manager about the rebalance status.
	rebInProgress, _ := rest.CheckRebalanceStatus(ctl.optionsCtl.Manager)
	if rebInProgress {
		return true
	}

	return false
}
