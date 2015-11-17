package mcp

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/rebalance"

	"github.com/couchbase/cbgt-mcp/mcp/interfaces"
)

// An MCP might be in the midst of controlling a replan/rebalance,
// where mcp.ctlDoneCh will be non-nil.
//
// If we're in the midst of a replan/rebalance, and another topology
// change request comes in, we stop any existing work and then start
// off a new replan/rebalance, because the new topology change request
// will have the latest, wanted topology.  This might happen if some
// stopChangeTopology request or signal got lost somewhere.
//
type MCP struct {
	cfg        cbgt.Cfg
	cfgEventCh chan cbgt.CfgEvent

	server  string
	options MCPOptions

	doneCh chan struct{} // Closed by MCP when MCP is done.
	initCh chan error    // Closed by MCP when MCP is initialized.
	stopCh chan struct{} // Closed by app when MCP should stop.
	kickCh chan string   // Written by app when it wants to kick the MCP.

	// -----------------------------------
	// The m protects the fields in this section.
	m sync.Mutex

	revNum uint64

	memberNodes []interfaces.Node // May be nil before initCh closed.

	ctlDoneCh         chan struct{}
	ctlStopCh         chan struct{}
	ctlRev            interfaces.Rev
	ctlChangeTopology *interfaces.ChangeTopology

	prevWarnings map[string][]string // Keyed by index name.
	prevErrors   []error             // Errors from previous ctl.

	// -----------------------------------
	// The mLast protects the lastXxx fields in this section.
	mLast sync.Mutex

	lastActivities *interfaces.Activities
}

type MCPOptions struct {
	DryRun  bool
	Verbose int

	WaitForMemberNodes int // Seconds to wait for member nodes.
}

// ----------------------------------------------------

func StartMCP(cfg cbgt.Cfg, server string, options MCPOptions) (
	*MCP, error) {
	mcp := &MCP{
		cfg:        cfg,
		cfgEventCh: make(chan cbgt.CfgEvent),
		server:     server,
		options:    options,
		doneCh:     make(chan struct{}),
		initCh:     make(chan error),
		stopCh:     make(chan struct{}),
		kickCh:     make(chan string),
	}

	go mcp.run()

	return mcp, <-mcp.initCh
}

func (mcp *MCP) Stop() error {
	close(mcp.stopCh)

	<-mcp.doneCh

	return nil
}

// ----------------------------------------------------

func (mcp *MCP) run() {
	defer close(mcp.doneCh)

	memberNodes, err := currentMemberNodes(mcp.cfg)
	if err != nil {
		mcp.initCh <- err
		close(mcp.initCh)
		return
	}

	planPIndexes, _, err := cbgt.PlannerGetPlanPIndexes(mcp.cfg, cbgt.VERSION)
	if err != nil {
		mcp.initCh <- err
		close(mcp.initCh)
		return
	}

	mcp.m.Lock()

	mcp.revNum = 1

	mcp.memberNodes = memberNodes

	if planPIndexes != nil {
		mcp.prevWarnings = planPIndexes.Warnings
	}

	mcp.m.Unlock()

	// -----------------------------------------------------------

	err = mcp.cfg.Subscribe(cbgt.INDEX_DEFS_KEY, mcp.cfgEventCh)
	if err != nil {
		mcp.initCh <- err
		close(mcp.initCh)
		return
	}

	var lastIndexDefs *cbgt.IndexDefs

	kickIndexDefs := func(kind string) error {
		indexDefs, _, err := cbgt.CfgGetIndexDefs(mcp.cfg)
		if err == nil && indexDefs != nil {
			if lastIndexDefs == nil {
				lastIndexDefs = indexDefs
			}

			if kind == "force" || kind == "force-indexDefs" ||
				!reflect.DeepEqual(lastIndexDefs, indexDefs) {
				err = mcp.indexDefsChanged(indexDefs)
				if err == nil {
					lastIndexDefs = indexDefs
				}
			}
		}

		return err
	}

	err = kickIndexDefs("init")
	if err != nil {
		mcp.initCh <- err
		close(mcp.initCh)
		return
	}

	// -----------------------------------------------------------

	close(mcp.initCh)

	for {
		select {
		case <-mcp.stopCh:
			mcp.dispatchCtl("", "stop", nil)
			return

		case kind := <-mcp.kickCh:
			kickIndexDefs(kind)

		case <-mcp.cfgEventCh:
			kickIndexDefs("cfgEvent")
		}
	}
}

// ----------------------------------------------------

// When the index definitions have changed, our approach is to run the
// planner, but only for brand new indexes that don't have any
// pindexes yet.
func (mcp *MCP) indexDefsChanged(indexDefs *cbgt.IndexDefs) (err error) {
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
			indexDef, mcp.server, nil)
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
		var steps map[string]bool
		var nodesToRemove []string

		cmd.PlannerSteps(steps, mcp.cfg, cbgt.VERSION,
			mcp.server, nodesToRemove, mcp.options.DryRun,
			plannerFilterNewIndexesOnly)

		planPIndexes, _, err :=
			cbgt.PlannerGetPlanPIndexes(mcp.cfg, cbgt.VERSION)
		if err == nil && planPIndexes != nil {
			mcp.m.Lock()
			mcp.prevWarnings = planPIndexes.Warnings
			mcp.m.Unlock()
		}
	}()

	return nil
}

// ----------------------------------------------------

func (mcp *MCP) Activities() *interfaces.Activities {
	mcp.mLast.Lock()
	rv := *mcp.lastActivities
	mcp.mLast.Unlock()

	return &rv
}

func (mcp *MCP) GetTopology() *interfaces.Topology {
	mcp.m.Lock()
	rv := mcp.getTopologyUnlocked()
	mcp.m.Unlock()

	return rv
}

func (mcp *MCP) getTopologyUnlocked() *interfaces.Topology {
	return &interfaces.Topology{
		Rev:                    interfaces.Rev(fmt.Sprintf("%d", mcp.revNum)),
		MemberNodes:            mcp.memberNodes,
		ChangeTopologyWarnings: mcp.prevWarnings,
		ChangeTopologyErrors:   mcp.prevErrors,
		ChangeTopology:         mcp.ctlChangeTopology,
	}
}

// ----------------------------------------------------

func (mcp *MCP) ChangeTopology(changeTopology *interfaces.ChangeTopology) (
	topology *interfaces.Topology, err error) {
	return mcp.dispatchCtl(
		changeTopology.Rev,
		changeTopology.Mode,
		changeTopology.MemberNodes)
}

func (mcp *MCP) StopChangeTopology(rev interfaces.Rev) {
	mcp.dispatchCtl(rev, "stopChangeTopology", nil)
}

// ----------------------------------------------------

func (mcp *MCP) dispatchCtl(
	rev interfaces.Rev, mode string, memberNodes []interfaces.Node,
) (*interfaces.Topology, error) {
	mcp.m.Lock()
	err := mcp.dispatchCtlUnlocked(rev, mode, memberNodes)
	topology := mcp.getTopologyUnlocked()
	mcp.m.Unlock()

	return topology, err
}

func (mcp *MCP) dispatchCtlUnlocked(
	rev interfaces.Rev, mode string, memberNodes []interfaces.Node) error {
	if rev != "" &&
		rev != mcp.ctlRev &&
		rev != interfaces.Rev(fmt.Sprintf("%d", mcp.revNum)) {
		return interfaces.ErrorWrongRev
	}

	if mcp.ctlStopCh != nil {
		close(mcp.ctlStopCh)
		mcp.ctlStopCh = nil
	}

	ctlDoneCh := mcp.ctlDoneCh
	if ctlDoneCh != nil {
		// Release lock while waiting for done, so ctl can mutate mcp fields.
		mcp.m.Unlock()
		<-ctlDoneCh
		mcp.m.Lock()
	}

	if mcp.ctlDoneCh == nil &&
		mode != "stop" &&
		mode != "stopChangeTopology" {
		return mcp.startCtlUnlocked(mode, memberNodes, nil)
	}

	return nil
}

// ----------------------------------------------------

func (mcp *MCP) startCtlUnlocked(mode string, memberNodes []interfaces.Node,
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
					cbgt.PlannerGetPlanPIndexes(mcp.cfg, cbgt.VERSION)
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
				memberNodes, _ = currentMemberNodes(mcp.cfg)
			}

			mcp.m.Lock()

			if ctlDoneCh == mcp.ctlDoneCh {
				mcp.ctlDoneCh = nil
				mcp.ctlStopCh = nil
				mcp.ctlRev = ""
				mcp.ctlChangeTopology = nil
			}

			mcp.revNum++
			mcp.memberNodes = memberNodes
			mcp.prevWarnings = ctlWarnings
			mcp.prevErrors = []error{ctlErr}

			mcp.m.Unlock()

			close(ctlDoneCh)
		}()

		// 1) Monitor cfg to wait for wanted nodes to appear.
		//
		var wantedNodes []string
		for _, memberNode := range memberNodes {
			wantedNodes = append(wantedNodes, string(memberNode.UUID))
		}

		nodesToRemove, err := mcp.waitForWantedNodes(wantedNodes)
		if err != nil {
			log.Printf("mcp: waitForWantedNodes, err: %v", err)
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
					cbgt.PlannerGetIndexDefs(mcp.cfg, cbgt.VERSION)
				if err != nil {
					ctlErr = err
					return
				}

				// Start rebalance and monitor progress.
				favorMinNodes := false

				r, err := rebalance.StartRebalance(cbgt.VERSION,
					mcp.cfg,
					mcp.server,
					nodesToRemove,
					rebalance.RebalanceOptions{
						FavorMinNodes: favorMinNodes,
						DryRun:        mcp.options.DryRun,
						Verbose:       mcp.options.Verbose,
					})
				if err != nil {
					log.Printf("mcp: StartRebalance, err: %v", err)
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
						// TODO: Update mcp activities based on progress.

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
						log.Printf("mcp: ReportProgress, err: %v", err)
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
					cbgt.PlannerGetIndexDefs(mcp.cfg, cbgt.VERSION)
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
		var steps map[string]bool
		if failover {
			steps = map[string]bool{"FAILOVER": true}
		}

		err = cmd.PlannerSteps(steps, mcp.cfg, cbgt.VERSION,
			mcp.server, nodesToRemove, mcp.options.DryRun, nil)
		if err != nil {
			log.Printf("mcp: PlannerSteps, err: %v", err)
			ctlErr = err
		}
	}()

	mcp.revNum++

	mcp.ctlDoneCh = ctlDoneCh
	mcp.ctlStopCh = ctlStopCh
	mcp.ctlRev = interfaces.Rev(fmt.Sprintf("%d", mcp.revNum))
	mcp.ctlChangeTopology = &interfaces.ChangeTopology{
		Mode:        mode,
		MemberNodes: memberNodes,
	}

	return nil
}

// ----------------------------------------------------

// Waits for actual nodeDefsWanted in the cfg to be equal to or a
// superset of wantedNodes, and returns the nodesToRemove.
func (mcp *MCP) waitForWantedNodes(wantedNodes []string) ([]string, error) {
	secs := mcp.options.WaitForMemberNodes
	if secs <= 0 {
		secs = 30
	}

	var nodeDefWantedUUIDs []string

	for i := 0; i < secs; i++ {
		nodeDefsWanted, _, err :=
			cbgt.CfgGetNodeDefs(mcp.cfg, cbgt.NODE_DEFS_WANTED)
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

	return nil, fmt.Errorf("mcp: waitForWantedNodes"+
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
		memberNodes = append(memberNodes, interfaces.Node{
			UUID:       interfaces.UUID(nodeDef.UUID),
			ServiceURL: interfaces.URL("http://" + nodeDef.HostPort),
			ManagerURL: interfaces.URL("http://" + nodeDef.Extras),
		})
	}

	return memberNodes, nil
}
