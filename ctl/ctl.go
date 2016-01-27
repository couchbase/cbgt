package ctl

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/ctl/interfaces"
	"github.com/couchbase/cbgt/rebalance"
)

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
	kickCh chan string   // Written by app when it wants to kick the Ctl.

	// -----------------------------------
	// The m protects the fields below.
	m sync.Mutex

	revNum uint64

	memberNodes []interfaces.Node // May be nil before initCh closed.

	ctlDoneCh         chan struct{}
	ctlStopCh         chan struct{}
	ctlRev            interfaces.Rev
	ctlChangeTopology *interfaces.ChangeTopology

	prevWarnings map[string][]string // Keyed by index name.
	prevErrors   []error             // Errors from previous ctl.
}

type CtlOptions struct {
	DryRun             bool
	Verbose            int
	FavorMinNodes      bool
	WaitForMemberNodes int // Seconds to wait for member nodes.
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
		kickCh:     make(chan string),
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

	memberNodes, err := currentMemberNodes(ctl.cfg)
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

	ctl.revNum = 1

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

		case kind := <-ctl.kickCh:
			kickIndexDefs(kind)

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
			ctl.prevWarnings = planPIndexes.Warnings
			ctl.m.Unlock()
		}
	}()

	return nil
}

// ----------------------------------------------------

func (ctl *Ctl) GetTopology() *interfaces.Topology {
	ctl.m.Lock()
	rv := ctl.getTopologyUnlocked()
	ctl.m.Unlock()

	return rv
}

func (ctl *Ctl) getTopologyUnlocked() *interfaces.Topology {
	return &interfaces.Topology{
		Rev:                    interfaces.Rev(fmt.Sprintf("%d", ctl.revNum)),
		MemberNodes:            ctl.memberNodes,
		ChangeTopologyWarnings: ctl.prevWarnings,
		ChangeTopologyErrors:   ctl.prevErrors,
		ChangeTopology:         ctl.ctlChangeTopology,
	}
}

// ----------------------------------------------------

func (ctl *Ctl) ChangeTopology(changeTopology *interfaces.ChangeTopology) (
	topology *interfaces.Topology, err error) {
	return ctl.dispatchCtl(
		changeTopology.Rev,
		changeTopology.Mode,
		changeTopology.MemberNodes)
}

func (ctl *Ctl) StopChangeTopology(rev interfaces.Rev) {
	ctl.dispatchCtl(rev, "stopChangeTopology", nil)
}

// ----------------------------------------------------

func (ctl *Ctl) dispatchCtl(
	rev interfaces.Rev, mode string, memberNodes []interfaces.Node,
) (*interfaces.Topology, error) {
	ctl.m.Lock()
	err := ctl.dispatchCtlUnlocked(rev, mode, memberNodes)
	topology := ctl.getTopologyUnlocked()
	ctl.m.Unlock()

	return topology, err
}

func (ctl *Ctl) dispatchCtlUnlocked(
	rev interfaces.Rev, mode string, memberNodes []interfaces.Node) error {
	if rev != "" &&
		rev != ctl.ctlRev &&
		rev != interfaces.Rev(fmt.Sprintf("%d", ctl.revNum)) {
		return interfaces.ErrorWrongRev
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
		return ctl.startCtlUnlocked(mode, memberNodes, nil)
	}

	return nil
}

// ----------------------------------------------------

func (ctl *Ctl) startCtlUnlocked(mode string, memberNodes []interfaces.Node,
	indexDefs *cbgt.IndexDefs) error {
	ctlDoneCh := make(chan struct{})
	ctlStopCh := make(chan struct{})

	// The ctl goroutine.
	//
	go func() {
		var ctlErr error
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
					if ctlErr == nil {
						ctlErr = err
					}
				}
			}

			if ctlErr != nil {
				// If there was an error, grab the latest memberNodes
				// rather than using the input memberNodes.
				memberNodes, _ = currentMemberNodes(ctl.cfg)
			}

			ctl.m.Lock()

			if ctlDoneCh == ctl.ctlDoneCh {
				ctl.ctlDoneCh = nil
				ctl.ctlStopCh = nil
				ctl.ctlRev = ""
				ctl.ctlChangeTopology = nil
			}

			ctl.revNum++
			ctl.memberNodes = memberNodes
			ctl.prevWarnings = ctlWarnings
			ctl.prevErrors = []error{ctlErr}

			ctl.m.Unlock()

			close(ctlDoneCh)
		}()

		// 1) Monitor cfg to wait for wanted nodes to appear.
		//
		var wantedNodes []string
		for _, memberNode := range memberNodes {
			wantedNodes = append(wantedNodes, string(memberNode.UUID))
		}

		nodesToRemove, err := ctl.waitForWantedNodes(wantedNodes)
		if err != nil {
			log.Printf("ctl: waitForWantedNodes, err: %v", err)
			ctlErr = err
			return
		}

		// 2) Run rebalance in a loop (if not failover).
		//
		failover := strings.HasPrefix(mode, "failover")
		if !failover {
			// The rebalance loop handles the case if the index
			// definitions had changed during the midst of the
			// rebalance, in which case we run rebalance again.
		REBALANCE_LOOP:
			for {
				// Retrieve the indexDefs before we do anything.
				indexDefsStart, err :=
					cbgt.PlannerGetIndexDefs(ctl.cfg, cbgt.VERSION)
				if err != nil {
					ctlErr = err
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
					ctlErr = err
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
						ctlErr = err
						return
					}
				}

				ctlWarnings = r.GetEndPlanPIndexes().Warnings

				// Repeat if the indexDefs had changed mid-rebalance.
				indexDefsEnd, err :=
					cbgt.PlannerGetIndexDefs(ctl.cfg, cbgt.VERSION)
				if err != nil {
					ctlErr = err
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
			ctlErr = err
		}
	}()

	ctl.revNum++

	ctl.ctlDoneCh = ctlDoneCh
	ctl.ctlStopCh = ctlStopCh
	ctl.ctlRev = interfaces.Rev(fmt.Sprintf("%d", ctl.revNum))
	ctl.ctlChangeTopology = &interfaces.ChangeTopology{
		Mode:        mode,
		MemberNodes: memberNodes,
	}

	return nil
}

// ----------------------------------------------------

// Waits for actual nodeDefsWanted in the cfg to be equal to or a
// superset of wantedNodes, and returns the nodesToRemove.
func (ctl *Ctl) waitForWantedNodes(wantedNodes []string) ([]string, error) {
	secs := ctl.options.WaitForMemberNodes
	if secs <= 0 {
		secs = 30
	}

	return WaitForWantedNodes(ctl.cfg, wantedNodes, secs)
}

func WaitForWantedNodes(cfg cbgt.Cfg, wantedNodes []string, secs int) (
	[]string, error) {
	var nodeDefWantedUUIDs []string

	for i := 0; i < secs; i++ {
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

func currentMemberNodes(cfg cbgt.Cfg) ([]interfaces.Node, error) {
	nodeDefsWanted, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		return nil, err
	}

	var memberNodes []interfaces.Node

	for _, nodeDef := range nodeDefsWanted.NodeDefs {
		memberNode := interfaces.Node{
			UUID:       interfaces.UUID(nodeDef.UUID),
			ServiceURL: interfaces.URL("http://" + nodeDef.HostPort),
		}

		if nodeDef.Extras != "" {
			memberNode.ManagerURL = interfaces.URL("http://" + nodeDef.Extras)
		}

		memberNodes = append(memberNodes, memberNode)
	}

	return memberNodes, nil
}
