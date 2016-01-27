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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/rebalance"
)

var ErrWrongRev = errors.New("wrong rev")
var ErrCanceled = errors.New("canceled")

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

	server  string
	options CtlOptions

	doneCh chan struct{} // Closed by Ctl when Ctl is done.
	initCh chan error    // Closed by Ctl when Ctl is initialized.
	stopCh chan struct{} // Closed by app when Ctl should stop.

	// -----------------------------------
	// The m protects the fields below.
	m sync.Mutex

	revNum       uint64
	revNumWaitCh chan struct{} // Closed when revNum changes.

	memberNodes []CtlNode // May be nil before initCh closed.

	// The following ctlXxxx fields are all nil or non-nil together
	// depending on whether a change topology is inflight.

	ctlDoneCh         chan struct{}
	ctlStopCh         chan struct{}
	ctlChangeTopology *CtlChangeTopology

	// Warnings from previous operation, keyed by index name.
	prevWarnings map[string][]string

	// Errs from previous operation.
	prevErrs []error
}

type CtlOptions struct {
	DryRun             bool
	Verbose            int
	FavorMinNodes      bool
	WaitForMemberNodes int // Seconds to wait for wanted member nodes to appear.
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
}

// ----------------------------------------------------

func StartCtl(cfg cbgt.Cfg, server string, options CtlOptions) (
	*Ctl, error) {
	ctl := &Ctl{
		cfg:        cfg,
		cfgEventCh: make(chan cbgt.CfgEvent),
		server:     server,
		options:    options,
		doneCh:     make(chan struct{}),
		initCh:     make(chan error),
		stopCh:     make(chan struct{}),
		revNum:     1,
	}

	go ctl.run()

	return ctl, <-ctl.initCh
}

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
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	planPIndexes, _, err := cbgt.PlannerGetPlanPIndexes(ctl.cfg, cbgt.VERSION)
	if err != nil {
		ctl.initCh <- err
		close(ctl.initCh)
		return
	}

	ctl.m.Lock()

	ctl.incRevNumLOCKED()

	ctl.memberNodes = memberNodes

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
		indexDefs, _, err := cbgt.CfgGetIndexDefs(ctl.cfg)
		if err == nil && indexDefs != nil {
			if lastIndexDefs == nil {
				lastIndexDefs = indexDefs
			}

			if kind == "force" || kind == "force-indexDefs" ||
				!reflect.DeepEqual(lastIndexDefs, indexDefs) {
				err = ctl.IndexDefsChanged()
				if err == nil {
					lastIndexDefs = indexDefs
				}
			}
		}

		return err
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
			ctl.dispatchCtl("", "stop", nil)
			return

		case <-ctl.cfgEventCh:
			kickIndexDefs("cfgEvent")
		}
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
						planPIndexes.PlanPIndexes[n] = p

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
			indexDef, ctl.server, nil)
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

		cmd.PlannerSteps(steps, ctl.cfg, cbgt.VERSION,
			ctl.server, nodesToRemove, ctl.options.DryRun,
			plannerFilterNewIndexesOnly)

		planPIndexes, _, err :=
			cbgt.PlannerGetPlanPIndexes(ctl.cfg, cbgt.VERSION)
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
func (ctl *Ctl) WaitGetTopology(haveRev string, cancelCh <-chan struct{}) (
	*CtlTopology, error) {
	ctl.m.Lock()

	for haveRev == fmt.Sprintf("%d", ctl.revNum) {
		if ctl.revNumWaitCh == nil {
			ctl.revNumWaitCh = make(chan struct{})
		}
		revNumWaitCh := ctl.revNumWaitCh // See also incRevNumLOCKED().

		ctl.m.Unlock()
		select {
		case <-cancelCh:
			return nil, ErrCanceled
		case <-revNumWaitCh:
			// FALLTHRU
		}
		ctl.m.Lock()
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

// ----------------------------------------------------

// ChangeTopology starts an asynchonous change topology operation, if
// there's no input error.  ChangeTopology also synchronously stops
// any previous, inflight change topology operation, if any, before
// kicking off the new change topology operation.
func (ctl *Ctl) ChangeTopology(changeTopology *CtlChangeTopology) (
	topology *CtlTopology, err error) {
	return ctl.dispatchCtl(
		changeTopology.Rev,
		changeTopology.Mode,
		changeTopology.MemberNodeUUIDs)
}

// StopChangeTopology synchronously stops a current change topology
// operation.
func (ctl *Ctl) StopChangeTopology(rev string) {
	ctl.dispatchCtl(rev, "stopChangeTopology", nil)
}

// ----------------------------------------------------

func (ctl *Ctl) dispatchCtl(rev string,
	mode string, memberNodeUUIDs []string) (
	*CtlTopology, error) {
	ctl.m.Lock()
	err := ctl.dispatchCtlLOCKED(rev, mode, memberNodeUUIDs)
	topology := ctl.getTopologyLOCKED()
	ctl.m.Unlock()

	return topology, err
}

func (ctl *Ctl) dispatchCtlLOCKED(rev string,
	mode string, memberNodeUUIDs []string) error {
	if rev != "" && rev != fmt.Sprintf("%d", ctl.revNum) {
		return ErrWrongRev
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
		return ctl.startCtlLOCKED(mode, memberNodeUUIDs, nil)
	}

	return nil
}

// ----------------------------------------------------

func (ctl *Ctl) startCtlLOCKED(mode string, memberNodeUUIDs []string,
	indexDefs *cbgt.IndexDefs) error {
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

	// The ctl goroutine.
	//
	go func() {
		var ctlErrs []error
		var ctlWarnings map[string][]string

		// Cleanup ctl goroutine.
		//
		defer func() {
			if ctlWarnings == nil {
				// If there were no warnings, see if there were any
				// warnings left in the plan.
				planPIndexes, _, err :=
					cbgt.PlannerGetPlanPIndexes(ctl.cfg, cbgt.VERSION)
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

			ctl.m.Unlock()

			close(ctlDoneCh)
		}()

		// 1) Monitor cfg to wait for wanted nodes to appear.
		//
		nodesToRemove, err :=
			ctl.waitForWantedNodes(memberNodeUUIDs, ctlStopCh)
		if err != nil {
			log.Printf("ctl: waitForWantedNodes, err: %v", err)
			ctlErrs = append(ctlErrs, err)
			return
		}

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
				indexDefsStart, err :=
					cbgt.PlannerGetIndexDefs(ctl.cfg, cbgt.VERSION)
				if err != nil {
					ctlErrs = append(ctlErrs, err)
					return
				}

				// Start rebalance and monitor progress.
				r, err := rebalance.StartRebalance(cbgt.VERSION,
					ctl.cfg,
					ctl.server,
					nodesToRemove,
					rebalance.RebalanceOptions{
						FavorMinNodes: ctl.options.FavorMinNodes,
						DryRun:        ctl.options.DryRun,
						Verbose:       ctl.options.Verbose,
					})
				if err != nil {
					log.Printf("ctl: StartRebalance, err: %v", err)
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
						return rebalance.ProgressTableString(
							maxNodeLen, maxPIndexLen,
							seenNodes,
							seenNodesSorted,
							seenPIndexes,
							seenPIndexesSorted,
							progressEntries)
					}

					err = rebalance.ReportProgress(r, progressToString)
					if err != nil {
						log.Printf("ctl: ReportProgress, err: %v", err)
						progressDoneCh <- err
					}
				}()

				defer r.Stop()

				select {
				case <-ctlStopCh:
					return // Exit ctl goroutine.

				case err = <-progressDoneCh:
					if err != nil {
						ctlErrs = append(ctlErrs, err)
						return
					}
				}

				ctlWarnings = r.GetEndPlanPIndexes().Warnings

				// Repeat if the indexDefs had changed mid-rebalance.
				indexDefsEnd, err :=
					cbgt.PlannerGetIndexDefs(ctl.cfg, cbgt.VERSION)
				if err != nil {
					ctlErrs = append(ctlErrs, err)
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

		err = cmd.PlannerSteps(steps, ctl.cfg, cbgt.VERSION,
			ctl.server, nodesToRemove, ctl.options.DryRun, nil)
		if err != nil {
			log.Printf("ctl: PlannerSteps, err: %v", err)
			ctlErrs = append(ctlErrs, err)
		}
	}()

	return nil
}

// ----------------------------------------------------

// Waits for actual nodeDefsWanted in the cfg to be equal to or a
// superset of wantedNodes, and returns the nodesToRemove.
func (ctl *Ctl) waitForWantedNodes(wantedNodes []string,
	cancelCh <-chan struct{}) ([]string, error) {
	secs := ctl.options.WaitForMemberNodes
	if secs <= 0 {
		secs = 30
	}

	return WaitForWantedNodes(ctl.cfg, wantedNodes, cancelCh, secs)
}

// WaitForWantedNodes blocks until the nodeDefsWanted in the cfg is
// equal to or a superset of the provided wantedNodes, and returns the
// "nodes to remove" (actualWantedNodes SET-DIFFERENCE wantedNodes).
func WaitForWantedNodes(cfg cbgt.Cfg, wantedNodes []string,
	cancelCh <-chan struct{}, secs int) (
	[]string, error) {
	var nodeDefWantedUUIDs []string

	for i := 0; i < secs; i++ {
		select {
		case <-cancelCh:
			return nil, ErrCanceled
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
