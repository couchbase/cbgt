//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/couchbase/blance"
	log "github.com/couchbase/clog"
)

func init() {
	// custom score booster override for single partioned indexes
	// to result in better balanced assignments.
	blance.NodeScoreBooster = func(w int, s float64) float64 {
		score := float64(-w)
		if score < s {
			score = s
		}
		return score
	}
}

// PlannerHooks allows advanced applications to register callbacks
// into the planning computation, in order to adjust the planning
// outcome.  For example, an advanced application might adjust node
// weights more dynamically in order to achieve an improved balancing
// of pindexes across a cluster.  It should be modified only during
// the init()'ialization phase of process startup.  See CalcPlan()
// implementation for more information.
var PlannerHooks = map[string]PlannerHook{}

// A PlannerHook is an optional callback func supplied by the
// application via PlannerHooks and is invoked during planning.
type PlannerHook func(in PlannerHookInfo) (out PlannerHookInfo, skip bool, err error)

// A PlannerHookInfo is the in/out information provided to PlannerHook
// callbacks.  If the PlannerHook wishes to modify any of these fields
// to affect the planning outcome, it must copy the field value
// (e.g., copy-on-write).
type PlannerHookInfo struct {
	PlannerHookPhase string

	Mode    string
	Version string
	Server  string

	Options map[string]string

	IndexDefs *IndexDefs
	IndexDef  *IndexDef

	NodeDefs          *NodeDefs
	NodeUUIDsAll      []string
	NodeUUIDsToAdd    []string
	NodeUUIDsToRemove []string
	NodeWeights       map[string]int
	NodeHierarchy     map[string]string

	PlannerFilter PlannerFilter

	PlanPIndexesPrev *PlanPIndexes
	PlanPIndexes     *PlanPIndexes

	PlanPIndexesForIndex map[string]*PlanPIndex

	NumPlanPIndexes    int
	NodeTotalActives   map[string]int
	NodeSourceActives  map[string]int
	NodeSourceReplicas map[string]int
	NodePartitionCount map[string]int
	ExistingPlans      *PlanPIndexes
}

// A NoopPlannerHook is a no-op planner hook that just returns its input.
func NoopPlannerHook(x PlannerHookInfo) (PlannerHookInfo, bool, error) {
	return x, false, nil
}

// -------------------------------------------------------

// NOTE: You *must* update VERSION if the planning algorithm or config
// data schema changes, following semver rules.

// PlannerNOOP sends a synchronous NOOP request to the manager's planner, if any.
func (mgr *Manager) PlannerNOOP(msg string) {
	atomic.AddUint64(&mgr.stats.TotPlannerNOOP, 1)

	if mgr.tagsMap == nil || mgr.tagsMap["planner"] {
		syncWorkReq(mgr.plannerCh, WORK_NOOP, msg, nil)
	}
}

// PlannerKick synchronously kicks the manager's planner, if any.
func (mgr *Manager) PlannerKick(msg string) {
	atomic.AddUint64(&mgr.stats.TotPlannerKick, 1)

	if mgr.tagsMap == nil || mgr.tagsMap["planner"] {
		syncWorkReq(mgr.plannerCh, WORK_KICK, msg, nil)
	}

	if mgr.peh != nil {
		ev := &CfgEvent{}
		if strings.Contains(msg, "CreateIndex") ||
			strings.Contains(msg, "DeleteIndex") {
			ev.Key = INDEX_DEFS_KEY
		}
		mgr.peh(ev)
	}
}

// PlannerLoop is the main loop for the planner.
func (mgr *Manager) PlannerLoop() {
	if mgr.cfg != nil { // Might be nil for testing.
		go func() {
			ec := make(chan CfgEvent)
			mgr.cfg.Subscribe(INDEX_DEFS_KEY, ec)
			mgr.cfg.Subscribe(CfgNodeDefsKey(NODE_DEFS_WANTED), ec)
			for {
				select {
				case <-mgr.stopCh:
					return
				case e := <-ec:
					atomic.AddUint64(&mgr.stats.TotPlannerSubscriptionEvent, 1)
					mgr.PlannerKick("cfg changed, key: " + e.Key)
				}
			}
		}()
	}

	for {
		select {
		case <-mgr.stopCh:
			atomic.AddUint64(&mgr.stats.TotPlannerStop, 1)
			return

		case m := <-mgr.plannerCh:
			atomic.AddUint64(&mgr.stats.TotPlannerOpStart, 1)

			log.Printf("planner: awakes, op: %v, msg: %s", m.op, m.msg)

			var err error

			if m.op == WORK_KICK {
				atomic.AddUint64(&mgr.stats.TotPlannerKickStart, 1)
				changed, err2 := mgr.PlannerOnce(m.msg)
				if err2 != nil {
					log.Warnf("planner: PlannerOnce, err: %v", err2)
					atomic.AddUint64(&mgr.stats.TotPlannerKickErr, 1)
					// Keep looping as perhaps it's a transient issue.
				} else {
					if changed {
						atomic.AddUint64(&mgr.stats.TotPlannerKickChanged, 1)
						mgr.JanitorKick("the plans have changed")
					}
					atomic.AddUint64(&mgr.stats.TotPlannerKickOk, 1)
				}
			} else if m.op == WORK_NOOP {
				atomic.AddUint64(&mgr.stats.TotPlannerNOOPOk, 1)
			} else {
				err = fmt.Errorf("planner: unknown op: %s, m: %#v", m.op, m)
				atomic.AddUint64(&mgr.stats.TotPlannerUnknownErr, 1)
			}

			atomic.AddUint64(&mgr.stats.TotPlannerOpRes, 1)

			if m.resCh != nil {
				if err != nil {
					atomic.AddUint64(&mgr.stats.TotPlannerOpErr, 1)
					m.resCh <- err
				}
				close(m.resCh)
			}

			atomic.AddUint64(&mgr.stats.TotPlannerOpDone, 1)
		}
	}
}

// PlannerOnce is the main body of a PlannerLoop.
func (mgr *Manager) PlannerOnce(reason string) (bool, error) {
	log.Printf("planner: once, reason: %s", reason)

	if mgr.cfg == nil { // Can occur during testing.
		return false, fmt.Errorf("planner: skipped due to nil cfg")
	}

	return Plan(mgr.cfg, mgr.version, mgr.uuid, mgr.server,
		mgr.Options(), nil)
}

// A PlannerFilter callback func should return true if the plans for
// an indexDef should be updated during CalcPlan(), and should return
// false if the plans for the indexDef should be remain untouched.
type PlannerFilter func(indexDef *IndexDef,
	planPIndexesPrev, planPIndexes *PlanPIndexes) bool

// Plan runs the planner once.
func Plan(cfg Cfg, version, uuid, server string, options map[string]string,
	plannerFilter PlannerFilter) (bool, error) {
	indexDefs, nodeDefs, planPIndexesPrev, cas, err :=
		PlannerGetPlan(cfg, version, uuid)
	if err != nil {
		return false, err
	}

	// use the effective version while calculating the new plan
	eVersion := CfgGetVersion(cfg)
	if eVersion != version {
		log.Printf("planner: Plan, incoming version: %s, effective"+
			"Cfg version used: %s", version, eVersion)
		version = eVersion
	}

	planPIndexes, err := CalcPlan("", indexDefs, nodeDefs,
		planPIndexesPrev, version, server, options, plannerFilter)
	if err != nil {
		return false, fmt.Errorf("planner: CalcPlan, err: %v", err)
	}

	if SamePlanPIndexes(planPIndexes, planPIndexesPrev) {
		return false, nil
	}

	_, err = CfgSetPlanPIndexes(cfg, planPIndexes, cas)
	if err != nil {
		return false, fmt.Errorf("planner: could not save new plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
	}

	return true, nil
}

// PlannerGetPlan retrieves plan related info from the Cfg.
func PlannerGetPlan(cfg Cfg, version string, uuid string) (
	indexDefs *IndexDefs,
	nodeDefs *NodeDefs,
	planPIndexes *PlanPIndexes,
	planPIndexesCAS uint64,
	err error) {
	// use the incoming version for a potential version bump
	err = PlannerCheckVersion(cfg, version)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	indexDefs, err = PlannerGetIndexDefs(cfg, version)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	nodeDefs, err = PlannerGetNodeDefs(cfg, version, uuid)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	planPIndexes, planPIndexesCAS, err = PlannerGetPlanPIndexes(cfg, version)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	return indexDefs, nodeDefs, planPIndexes, planPIndexesCAS, nil
}

// PlannerCheckVersion errors if a version string is too low.
func PlannerCheckVersion(cfg Cfg, version string) error {
	ok, err := CheckVersion(cfg, version)
	if err != nil {
		return fmt.Errorf("planner: CheckVersion err: %v", err)
	}
	if !ok {
		return fmt.Errorf("planner: version too low: %v", version)
	}
	return nil
}

// PlannerGetIndexDefs retrives index definitions from a Cfg.
func PlannerGetIndexDefs(cfg Cfg, version string) (*IndexDefs, error) {
	indexDefs, _, err := CfgGetIndexDefs(cfg)
	if err != nil {
		return nil, fmt.Errorf("planner: CfgGetIndexDefs err: %v", err)
	}
	if indexDefs == nil {
		indexDefs = NewIndexDefs(CfgGetVersion(cfg))
	}
	if VersionGTE(version, indexDefs.ImplVersion) == false {
		return nil, fmt.Errorf("planner: indexDefs.ImplVersion: %s"+
			" > version: %s", indexDefs.ImplVersion, version)
	}
	return indexDefs, nil
}

// PlannerGetNodeDefs retrieves node definitions from a Cfg.
func PlannerGetNodeDefs(cfg Cfg, version, uuid string) (
	*NodeDefs, error) {
	nodeDefs, _, err := CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil {
		return nil, fmt.Errorf("planner: CfgGetNodeDefs err: %v", err)
	}
	if nodeDefs == nil {
		nodeDefs = NewNodeDefs(CfgGetVersion(cfg))
	}
	if VersionGTE(version, nodeDefs.ImplVersion) == false {
		return nil, fmt.Errorf("planner: nodeDefs.ImplVersion: %s"+
			" > version: %s", nodeDefs.ImplVersion, version)
	}
	if uuid == "" { // The caller may not be a node, so has empty uuid.
		return nodeDefs, nil
	}
	nodeDef, exists := nodeDefs.NodeDefs[uuid]
	if !exists || nodeDef == nil {
		return nil, fmt.Errorf("planner: no NodeDef, uuid: %s", uuid)
	}
	if nodeDef.ImplVersion != version {
		return nil, fmt.Errorf("planner: ended since NodeDef, uuid: %s,"+
			" NodeDef.ImplVersion: %s != version: %s",
			uuid, nodeDef.ImplVersion, version)
	}
	if nodeDef.UUID != uuid {
		return nil, fmt.Errorf("planner: ended since NodeDef, uuid: %s,"+
			" NodeDef.UUID: %s != uuid: %s",
			uuid, nodeDef.UUID, uuid)
	}
	isPlanner := true
	if nodeDef.Tags != nil && len(nodeDef.Tags) > 0 {
		isPlanner = false
		for _, tag := range nodeDef.Tags {
			if tag == "planner" {
				isPlanner = true
			}
		}
	}
	if !isPlanner {
		return nil, fmt.Errorf("planner: ended since node, uuid: %s,"+
			" is not a planner, tags: %#v", uuid, nodeDef.Tags)
	}
	return nodeDefs, nil
}

// PlannerGetPlanPIndexes retrieves the planned pindexes from a Cfg.
func PlannerGetPlanPIndexes(cfg Cfg, version string) (
	*PlanPIndexes, uint64, error) {
	planPIndexesPrev, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil {
		return nil, 0, fmt.Errorf("planner: CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexesPrev == nil {
		planPIndexesPrev = NewPlanPIndexes(CfgGetVersion(cfg))
	}
	if VersionGTE(version, planPIndexesPrev.ImplVersion) == false {
		return nil, 0, fmt.Errorf("planner: planPIndexesPrev.ImplVersion: %s"+
			" > version: %s", planPIndexesPrev.ImplVersion, version)
	}
	return planPIndexesPrev, cas, nil
}

// Split logical indexes into PIndexes and assign PIndexes to nodes.
// As part of this, planner hook callbacks will be invoked to allow
// advanced applications to adjust the planning outcome.
func CalcPlan(mode string, indexDefs *IndexDefs, nodeDefs *NodeDefs,
	planPIndexesPrev *PlanPIndexes, version, server string,
	options map[string]string, plannerFilter PlannerFilter) (
	*PlanPIndexes, error) {
	plannerHook := PlannerHooks[options["plannerHookName"]]
	if plannerHook == nil {
		plannerHook = NoopPlannerHook
	}

	var nodeUUIDsAll []string
	var nodeUUIDsToAdd []string
	var nodeUUIDsToRemove []string
	var nodeWeights map[string]int
	var nodeHierarchy map[string]string
	var planPIndexes *PlanPIndexes

	numPlanPIndexes := len(planPIndexesPrev.PlanPIndexes)
	nodeTotalActives := make(map[string]int)
	nodeSourceActives := make(map[string]int)
	nodeSourceReplicas := make(map[string]int)
	nodePartitionCount := make(map[string]int)
	existingPlans := new(PlanPIndexes)
	existingPlans.PlanPIndexes = make(map[string]*PlanPIndex)

	for name, pindex := range planPIndexesPrev.PlanPIndexes {
		for nodeUUID, planNode := range pindex.Nodes {
			nodePartitionCount[nodeUUID]++
			if planNode.Priority == 0 {
				nodeTotalActives[nodeUUID]++
				nodeSourceActives[nodeUUID+":"+pindex.SourceName]++
			} else {
				nodeSourceReplicas[nodeUUID+":"+pindex.SourceName]++
			}
		}
		existingPlans.PlanPIndexes[name] = pindex
	}

	plannerHookCall := func(phase string, indexDef *IndexDef,
		planPIndexesForIndex map[string]*PlanPIndex) (
		PlannerHookInfo, bool, error) {
		pho, skip, err := plannerHook(PlannerHookInfo{
			PlannerHookPhase:     phase,
			Mode:                 mode,
			Version:              version,
			Server:               server,
			Options:              options,
			IndexDefs:            indexDefs,
			IndexDef:             indexDef,
			NodeDefs:             nodeDefs,
			NodeUUIDsAll:         nodeUUIDsAll,
			NodeUUIDsToAdd:       nodeUUIDsToAdd,
			NodeUUIDsToRemove:    nodeUUIDsToRemove,
			NodeWeights:          nodeWeights,
			NodeHierarchy:        nodeHierarchy,
			PlannerFilter:        plannerFilter,
			PlanPIndexesPrev:     planPIndexesPrev,
			PlanPIndexes:         planPIndexes,
			PlanPIndexesForIndex: planPIndexesForIndex,
			NumPlanPIndexes:      numPlanPIndexes,
			NodeTotalActives:     nodeTotalActives,
			NodeSourceActives:    nodeSourceActives,
			NodeSourceReplicas:   nodeSourceReplicas,
			NodePartitionCount:   nodePartitionCount,
			ExistingPlans:        existingPlans,
		})

		mode = pho.Mode
		version = pho.Version
		server = pho.Server
		options = pho.Options
		indexDefs = pho.IndexDefs
		nodeDefs = pho.NodeDefs
		nodeUUIDsAll = pho.NodeUUIDsAll
		nodeUUIDsToAdd = pho.NodeUUIDsToAdd
		nodeUUIDsToRemove = pho.NodeUUIDsToRemove
		nodeWeights = pho.NodeWeights
		nodeHierarchy = pho.NodeHierarchy
		plannerFilter = pho.PlannerFilter
		planPIndexesPrev = pho.PlanPIndexesPrev
		planPIndexes = pho.PlanPIndexes
		numPlanPIndexes = pho.NumPlanPIndexes
		nodeSourceActives = pho.NodeSourceActives
		nodeSourceReplicas = pho.NodeSourceReplicas
		nodeTotalActives = pho.NodeTotalActives
		nodePartitionCount = pho.NodePartitionCount
		existingPlans = pho.ExistingPlans

		return pho, skip, err
	}

	if planPIndexes == nil {
		planPIndexes = NewPlanPIndexes(version)
	}

	_, skip, err := plannerHookCall("begin", nil, nil)
	if skip || err != nil {
		return planPIndexes, err
	}

	// This simple planner assigns at most MaxPartitionsPerPIndex
	// number of partitions onto a PIndex.  And then uses blance to
	// assign the PIndex to 1 or more nodes (based on NumReplicas).
	if indexDefs == nil || nodeDefs == nil {
		return nil, nil
	}

	nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove, nodeWeights, nodeHierarchy =
		CalcNodesLayout(indexDefs, nodeDefs, planPIndexesPrev)

	_, skip, err = plannerHookCall("nodes", nil, nil)
	if skip || err != nil {
		return planPIndexes, err
	}

	// Examine every indexDef, ordered by name for stability...
	var indexDefNames []string
	for indexDefName := range indexDefs.IndexDefs {
		indexDefNames = append(indexDefNames, indexDefName)
	}
	sort.Strings(indexDefNames)

	for _, indexDefName := range indexDefNames {
		indexDef := indexDefs.IndexDefs[indexDefName]

		pho, skip2, err2 := plannerHookCall("indexDef.begin", indexDef, nil)
		if skip2 {
			continue
		}
		if err2 != nil {
			return planPIndexes, err2
		}
		indexDef = pho.IndexDef

		// If the plan is frozen, CasePlanFrozen clones the previous
		// plan for this index.
		if CasePlanFrozen(indexDef, planPIndexesPrev, planPIndexes) {
			continue
		}

		// Skip if the plannerFilter returns false.
		if plannerFilter != nil &&
			!plannerFilter(indexDef, planPIndexesPrev, planPIndexes) {
			continue
		}

		// Skip indexDef's with no instantiatable pindexImplType, such
		// as index aliases.
		pindexImplType, exists := PIndexImplTypes[indexDef.Type]
		if !exists ||
			pindexImplType == nil ||
			pindexImplType.New == nil ||
			pindexImplType.Open == nil {
			continue
		}

		// Split each indexDef into 1 or more PlanPIndexes.
		planPIndexesForIndex, err2 := SplitIndexDefIntoPlanPIndexes(
			indexDef, server, options, planPIndexes)
		if err2 != nil {
			log.Warnf("planner: could not SplitIndexDefIntoPlanPIndexes,"+
				" indexDef.Name: %s, server: %s, err: %v",
				indexDef.Name, server, err2)
			continue // Keep planning the other IndexDefs.
		}

		pho, skip, err = plannerHookCall("indexDef.split",
			indexDef, planPIndexesForIndex)
		if skip {
			continue
		}
		if err != nil {
			return planPIndexes, err
		}
		indexDef = pho.IndexDef
		planPIndexesForIndex = pho.PlanPIndexesForIndex

		adjustedWeights := nodeWeights
		// override the node weights for single partitioned index to
		// favour balanced partition assignments.
		if len(planPIndexesForIndex) == 1 {
			// ensure that the node stickiness option is not enabled.
			if enabled, found := options["enablePartitionNodeStickiness"]; !found ||
				enabled == "false" {
				adjustedWeights = NormaliseNodeWeights(nodeWeights,
					planPIndexesPrev, len(planPIndexesPrev.PlanPIndexes))
			}
		}

		// Once we have a 1 or more PlanPIndexes for an IndexDef, use
		// blance to assign the PlanPIndexes to nodes.
		warnings := BlancePlanPIndexes(mode, indexDef,
			planPIndexesForIndex, existingPlans,
			nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove,
			adjustedWeights, nodeHierarchy, false)

		planPIndexes.Warnings[indexDef.Name] = []string{}

		for partitionName, partitionWarning := range warnings {
			if _, exists := planPIndexes.PlanPIndexes[partitionName]; exists {
				if planPIndexes.PlanPIndexes[partitionName].IndexName == indexDef.Name {
					planPIndexes.Warnings[indexDef.Name] =
						append(planPIndexes.Warnings[indexDef.Name], partitionWarning...)
				}
			}
		}

		for _, warning := range warnings {
			log.Printf("planner: indexDef.Name: %s,"+
				" PlanNextMap warning: %s", indexDef.Name, warning)
		}

		_, _, err = plannerHookCall("indexDef.balanced",
			indexDef, planPIndexesForIndex)
		if err != nil {
			return planPIndexes, err
		}
	}

	_, _, err = plannerHookCall("end", nil, nil)

	return planPIndexes, err
}

// Return nodes' UUIDs, weights and hierarchy.
func GetNodeWeightsAndHierarchy(nodeDefs *NodeDefs) (nodeUUIDs []string,
	nodeWeights map[string]int, nodeHierarchy map[string]string,
) {
	// Retrieve nodeUUID's, weights, and hierarchy from the current nodeDefs.
	nodeUUIDs = make([]string, 0)
	nodeWeights = make(map[string]int)
	nodeHierarchy = make(map[string]string)
	for _, nodeDef := range nodeDefs.NodeDefs {
		tags := StringsToMap(nodeDef.Tags)
		// Consider only nodeDef's that can support pindexes.
		if tags == nil || tags["pindex"] {
			nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)

			if nodeDef.Weight > 0 {
				nodeWeights[nodeDef.UUID] = nodeDef.Weight
			}

			child := nodeDef.UUID
			ancestors := strings.Split(nodeDef.Container, "/")
			for i := len(ancestors) - 1; i >= 0; i-- {
				if child != "" && ancestors[i] != "" {
					nodeHierarchy[child] = ancestors[i]
				}
				child = ancestors[i]
			}
		}
	}

	return nodeUUIDs, nodeWeights, nodeHierarchy
}

// CalcNodesLayout computes information about the nodes based on the
// index definitions, node definitions, and the current plan.
func CalcNodesLayout(indexDefs *IndexDefs, nodeDefs *NodeDefs,
	planPIndexesPrev *PlanPIndexes) (
	nodeUUIDsAll []string,
	nodeUUIDsToAdd []string,
	nodeUUIDsToRemove []string,
	nodeWeights map[string]int,
	nodeHierarchy map[string]string,
) {
	// Retrieve nodeUUID's, weights, and hierarchy from the current nodeDefs.
	nodeUUIDs, nodeWeights, nodeHierarchy := GetNodeWeightsAndHierarchy(nodeDefs)

	// Retrieve nodeUUID's from the previous plan.
	nodeUUIDsPrev := make([]string, 0)
	if planPIndexesPrev != nil {
		for _, planPIndexPrev := range planPIndexesPrev.PlanPIndexes {
			for nodeUUIDPrev := range planPIndexPrev.Nodes {
				nodeUUIDsPrev = append(nodeUUIDsPrev, nodeUUIDPrev)
			}
		}
	}

	// Dedupe.
	nodeUUIDsPrev = StringsIntersectStrings(nodeUUIDsPrev, nodeUUIDsPrev)

	// Calculate node deltas (nodes added & nodes removed).
	nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove =
		CalcNodesToAddRemove(nodeUUIDs, nodeUUIDsPrev)

	return nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove,
		nodeWeights, nodeHierarchy
}

// Calculate node deltas (nodes added & nodes removed).
func CalcNodesToAddRemove(currentNodeUUIDs, prevNodeUUIDs []string) (nodeUUIDsAll,
	nodeUUIDsToAdd, nodeUUIDsToRemove []string) {

	// Calculate node deltas (nodes added & nodes removed).
	nodeUUIDsAll = make([]string, 0)
	nodeUUIDsAll = append(nodeUUIDsAll, currentNodeUUIDs...)
	nodeUUIDsAll = append(nodeUUIDsAll, prevNodeUUIDs...)
	nodeUUIDsAll = StringsIntersectStrings(nodeUUIDsAll, nodeUUIDsAll) // Dedupe.
	nodeUUIDsToAdd = StringsRemoveStrings(nodeUUIDsAll, prevNodeUUIDs)
	nodeUUIDsToRemove = StringsRemoveStrings(nodeUUIDsAll, currentNodeUUIDs)

	sort.Strings(nodeUUIDsAll)
	sort.Strings(nodeUUIDsToAdd)
	sort.Strings(nodeUUIDsToRemove)

	return nodeUUIDsAll, nodeUUIDsToAdd, nodeUUIDsToRemove

}

// Split an IndexDef into 1 or more PlanPIndex'es, assigning data
// source partitions from the IndexDef to a PlanPIndex based on
// modulus of MaxPartitionsPerPIndex.
//
// NOTE: If MaxPartitionsPerPIndex isn't a clean divisor of the total
// number of data source partitions (like 1024 split into clumps of
// 10), then one PIndex assigned to the remainder will be smaller than
// the other PIndexes (such as having only a remainder of 4 partitions
// rather than the usual 10 partitions per PIndex).
func SplitIndexDefIntoPlanPIndexes(indexDef *IndexDef, server string,
	options map[string]string, planPIndexesOut *PlanPIndexes) (
	map[string]*PlanPIndex, error) {
	var sourcePartitionsArr []string
	var err error

	// TODO Check the bucket state instead, if possible?

	// If changing to a resuming index, don't attempt to get source partitions
	// since bucket might not be ready.
	if !strings.HasPrefix(indexDef.HibernationPath, UNHIBERNATE_TASK) {
		sourcePartitionsArr, err = DataSourcePartitions(indexDef.SourceType,
			indexDef.SourceName, indexDef.SourceUUID, indexDef.SourceParams,
			server, options)
		if err != nil {
			return nil, fmt.Errorf("planner: could not get partitions,"+
				" indexDef.Name: %s, server: %s, err: %v",
				indexDef.Name, server, err)
		}
	} else {
		for _, sourcePartition := range strings.Split(options["resumeSourcePartitions"], ",") {
			sourcePartitionsArr = append(sourcePartitionsArr, sourcePartition)
		}
	}

	planPIndexesForIndex := map[string]*PlanPIndex{}

	addPlanPIndex := func(sourcePartitionsCurr []string) int {
		sourcePartitions := strings.Join(sourcePartitionsCurr, ",")

		planPIndex := &PlanPIndex{
			Name:             PlanPIndexName(indexDef, sourcePartitions),
			UUID:             NewUUID(),
			IndexType:        indexDef.Type,
			IndexName:        indexDef.Name,
			IndexUUID:        indexDef.UUID,
			IndexParams:      indexDef.Params,
			SourceType:       indexDef.SourceType,
			SourceName:       indexDef.SourceName,
			SourceUUID:       indexDef.SourceUUID,
			SourceParams:     indexDef.SourceParams,
			SourcePartitions: sourcePartitions,
			Nodes:            make(map[string]*PlanPIndexNode),
			HibernationPath:  indexDef.HibernationPath,
		}

		if planPIndexesOut != nil {
			planPIndexesOut.PlanPIndexes[planPIndex.Name] = planPIndex
		}

		planPIndexesForIndex[planPIndex.Name] = planPIndex
		return len(planPIndexesForIndex)
	}

	groupSourcePartitionsIntoPlanPIndexes(
		indexDef.PlanParams.IndexPartitions,
		indexDef.PlanParams.MaxPartitionsPerPIndex,
		sourcePartitionsArr,
		addPlanPIndex,
	)

	return planPIndexesForIndex, nil
}

func groupSourcePartitionsIntoPlanPIndexes(indexPartitions, maxPartitionsPerPIndex int,
	sourcePartitionsArr []string, callback func([]string) int) {
	var currPlanPIndexes int
	if indexPartitions > 0 {
		// IndexPartitions has been defined/determined, which takes precedence;
		// So make certain the plan pindexes count matches this value.
		minPartitionsPerPIndex := int(math.Floor(float64(len(sourcePartitionsArr)) / float64(indexPartitions)))
		sourcePartitionsGroups := make([]int, indexPartitions)
		for i := 0; i < indexPartitions; i++ {
			sourcePartitionsGroups[i] = minPartitionsPerPIndex
		}
		// deficit will always be positive or zero
		deficit := len(sourcePartitionsArr) - (indexPartitions * minPartitionsPerPIndex)
		cursor := 0
		for deficit > 0 {
			sourcePartitionsGroups[cursor]++
			deficit--
			cursor = (cursor + 1) % len(sourcePartitionsGroups)
		}

		sourcePartitionsCurr := []string{}
		cursor = 0
		for _, sourcePartition := range sourcePartitionsArr {
			sourcePartitionsCurr = append(sourcePartitionsCurr, sourcePartition)
			if len(sourcePartitionsCurr) == sourcePartitionsGroups[cursor] {
				_ = callback(sourcePartitionsCurr)
				sourcePartitionsCurr = []string{}
				cursor++
			}
		}
	} else {
		sourcePartitionsCurr := []string{}
		for _, sourcePartition := range sourcePartitionsArr {
			sourcePartitionsCurr = append(sourcePartitionsCurr, sourcePartition)
			if maxPartitionsPerPIndex > 0 &&
				len(sourcePartitionsCurr) >= maxPartitionsPerPIndex {
				currPlanPIndexes = callback(sourcePartitionsCurr)
				sourcePartitionsCurr = []string{}
			}
		}

		if len(sourcePartitionsCurr) > 0 || // Assign any leftover partitions.
			currPlanPIndexes <= 0 { // Assign at least 1 PlanPIndex.
			callback(sourcePartitionsCurr)
		}
	}
}

// --------------------------------------------------------

// BlancePlanPIndexes invokes the blance library's generic
// PlanNextMap() algorithm to create a new pindex layout plan.
func BlancePlanPIndexes(mode string,
	indexDef *IndexDef,
	planPIndexesForIndex map[string]*PlanPIndex,
	planPIndexesPrev *PlanPIndexes,
	nodeUUIDsAll []string,
	nodeUUIDsToAdd []string,
	nodeUUIDsToRemove []string,
	nodeWeights map[string]int,
	nodeHierarchy map[string]string,
	skipExistingPartitions bool) map[string][]string {
	model, modelConstraints := BlancePartitionModel(indexDef)

	// First, reconstruct previous blance map from planPIndexesPrev.
	blancePrevMap := BlanceMap(planPIndexesForIndex, planPIndexesPrev, skipExistingPartitions)

	partitionWeights := indexDef.PlanParams.PIndexWeights

	stateStickiness := map[string]int(nil)
	if mode == "failover" {
		stateStickiness = map[string]int{"primary": 100000}
	}

	// Compute nodeUUIDsAllForIndex by rotating the nodeUUIDsAll based
	// on a function of index name, so that multiple indexes will have
	// layouts that favor different starting nodes, but whose
	// computation is repeatable.
	var nodeUUIDsAllForIndex []string

	h := crc32.NewIEEE()
	io.WriteString(h, indexDef.Name)
	next := sort.SearchStrings(nodeUUIDsAll, fmt.Sprintf("%x", h.Sum32()))

	for range nodeUUIDsAll {
		if next >= len(nodeUUIDsAll) {
			next = 0
		}

		nodeUUIDsAllForIndex =
			append(nodeUUIDsAllForIndex, nodeUUIDsAll[next])

		next++
	}

	// If there are server groups/racks defined and there are no explicit
	// hierarchyRules available then assume a rule which assigns
	// the replica partitions to different server groups/racks.
	// Node hierarchy would look like datacenter/serverGroup/nodeUUID
	// where nodeUUIDs are at level zero, serverGroups are at level one
	// and datacenter is at level two.
	// HierarchyRules specify which levels to include and which levels to
	// exclude while considering the replica assignments.
	// eg: ExcludeLevel: 1 means skip the same rack allocations.
	if indexDef.PlanParams.HierarchyRules == nil &&
		len(nodeHierarchy) > 0 {
		indexDef.PlanParams.HierarchyRules = blance.HierarchyRules{
			"replica": []*blance.HierarchyRule{{
				IncludeLevel: 2,
				ExcludeLevel: 1}}}
	}

	blanceNextMap, warnings := blance.PlanNextMap(blancePrevMap,
		nodeUUIDsAllForIndex, nodeUUIDsToRemove, nodeUUIDsToAdd,
		model, modelConstraints,
		partitionWeights,
		stateStickiness,
		nodeWeights,
		nodeHierarchy,
		indexDef.PlanParams.HierarchyRules)

	for planPIndexName, blancePartition := range blanceNextMap {
		// Update settings for new plan pindexes ONLY
		planPIndex, exists := planPIndexesForIndex[planPIndexName]
		if !exists {
			continue
		}

		planPIndex.Nodes = map[string]*PlanPIndexNode{}

		for i, nodeUUID := range blancePartition.NodesByState["primary"] {
			if i >= model["primary"].Constraints {
				break
			}

			canRead := true
			canWrite := true
			nodePlanParam :=
				GetNodePlanParam(indexDef.PlanParams.NodePlanParams,
					nodeUUID, indexDef.Name, planPIndexName)
			if nodePlanParam != nil {
				canRead = nodePlanParam.CanRead
				canWrite = nodePlanParam.CanWrite
			}

			planPIndex.Nodes[nodeUUID] = &PlanPIndexNode{
				CanRead:  canRead,
				CanWrite: canWrite,
				Priority: 0,
			}
		}

		for i, nodeUUID := range blancePartition.NodesByState["replica"] {
			if i >= model["replica"].Constraints {
				break
			}

			canRead := true
			canWrite := true
			nodePlanParam :=
				GetNodePlanParam(indexDef.PlanParams.NodePlanParams,
					nodeUUID, indexDef.Name, planPIndexName)
			if nodePlanParam != nil {
				canRead = nodePlanParam.CanRead
				canWrite = nodePlanParam.CanWrite
			}

			planPIndex.Nodes[nodeUUID] = &PlanPIndexNode{
				CanRead:  canRead,
				CanWrite: canWrite,
				Priority: i + 1,
			}
		}
	}

	return warnings
}

// NormaliseNodeWeights updates the node weights reflective of the
// existing partition count on those nodes based on the given plans
// and update factor.
func NormaliseNodeWeights(nodeWeights map[string]int,
	planPIndexes *PlanPIndexes, factor int) map[string]int {
	if planPIndexes == nil ||
		len(planPIndexes.PlanPIndexes) == 0 {
		return nodeWeights
	}

	rv := make(map[string]int)
	for uuid, wt := range nodeWeights {
		rv[uuid] = wt
	}

	nodePartitionCount := make(map[string]int)
	for _, pindex := range planPIndexes.PlanPIndexes {
		for nodeUUID := range pindex.Nodes {
			nodePartitionCount[nodeUUID]++
		}
	}

	for uuid, count := range nodePartitionCount {
		if weight, ok := nodeWeights[uuid]; ok {
			rv[uuid] = -factor * (weight + count)
		}
	}

	return rv
}

// BlancePartitionModel returns a blance library PartitionModel and
// model constraints based on an input index definition.
func BlancePartitionModel(indexDef *IndexDef) (
	model blance.PartitionModel,
	modelConstraints map[string]int,
) {
	// We're using multiple model states to better utilize blance's
	// node hierarchy features (shelf/rack/zone/row awareness).
	return blance.PartitionModel{
		"primary": &blance.PartitionModelState{
			Priority:    0,
			Constraints: 1,
		},
		"replica": &blance.PartitionModelState{
			Priority:    1,
			Constraints: indexDef.PlanParams.NumReplicas,
		},
	}, map[string]int(nil)
}

func getPrevPlanName(newPlan *PlanPIndex,
	planPIndexesPrev map[string]*PlanPIndex) string {
	for _, plan := range planPIndexesPrev {
		if plan.IndexName == newPlan.IndexName &&
			plan.SourcePartitions == newPlan.SourcePartitions {
			return plan.Name
		}
	}
	return ""
}

// BlanceMap reconstructs a blance map from an existing plan.
func BlanceMap(
	planPIndexesForIndex map[string]*PlanPIndex,
	planPIndexes *PlanPIndexes,
	skipExistingPartitions bool,
) blance.PartitionMap {
	m := blance.PartitionMap{}

	// Map to track prev plans that don't need to be added to blance map
	// on account of index definition updates.
	updatedPlans := make(map[string]struct{})

	sortNodesByState := func(nodes map[string]*PlanPIndexNode) map[string][]string {
		nodesByState := make(map[string][]string)

		// Sort by planPIndexNode.Priority for stability.
		planPIndexNodeRefs := PlanPIndexNodeRefs{}
		for nodeUUIDPrev, planPIndexNode := range nodes {
			planPIndexNodeRefs =
				append(planPIndexNodeRefs, &PlanPIndexNodeRef{
					UUID: nodeUUIDPrev,
					Node: planPIndexNode,
				})
		}
		sort.Sort(planPIndexNodeRefs)

		for _, planPIndexNodeRef := range planPIndexNodeRefs {
			state := "replica"
			if planPIndexNodeRef.Node.Priority <= 0 {
				state = "primary"
			}
			nodesByState[state] = append(nodesByState[state], planPIndexNodeRef.UUID)
		}

		return nodesByState
	}

	// Account for the new partitions (current index)
	for _, planPIndex := range planPIndexesForIndex {
		blancePartition := &blance.Partition{
			Name:         planPIndex.Name,
			NodesByState: map[string][]string{},
		}
		m[planPIndex.Name] = blancePartition

		if planPIndexes != nil {
			p, exists := planPIndexes.PlanPIndexes[planPIndex.Name]
			if !exists {
				// In case of indexDefinition updates, the planName
				// would have changed, hence pick the planName from the
				// prev pindex. This doesn't affect the new plans
				// or reloadability of the indexDefinition changes.
				// It only helps to feed blance with the existing
				// partition layouts while creating the new partition
				// assignments.
				p, exists = planPIndexes.PlanPIndexes[getPrevPlanName(
					planPIndex,
					planPIndexes.PlanPIndexes)]
			}
			if exists && p != nil {
				updatedPlans[p.Name] = struct{}{}
				blancePartition.NodesByState = sortNodesByState(p.Nodes)
			}
		}
	}

	// Now account for any pre-existing partitions
	if !skipExistingPartitions && planPIndexes != nil {
		for planName, planPIndex := range planPIndexes.PlanPIndexes {
			// Do not account for indexes whose definition was updated,
			// in which case the plan name would change.
			if _, exists := updatedPlans[planName]; exists {
				continue
			}

			blancePartition := &blance.Partition{
				Name:         planPIndex.Name,
				NodesByState: sortNodesByState(planPIndex.Nodes),
			}
			m[planPIndex.Name] = blancePartition
		}
	}

	return m
}

func sameIndexDefsExceptUUID(def1, def2 *IndexDef) bool {
	return (def1.Type == def2.Type &&
		def1.Name == def2.Name &&
		def1.Params == def2.Params &&
		def1.SourceName == def2.SourceName &&
		def1.SourceType == def2.SourceType &&
		def1.SourceUUID == def2.SourceUUID &&
		def1.SourceParams == def2.SourceParams)
}

// --------------------------------------------------------

// CasePlanFrozen returns true if the plan for the indexDef is frozen,
// in which case it also populates endPlanPIndexes with a clone of the
// indexDef's plans from begPlanPIndexes.
func CasePlanFrozen(indexDef *IndexDef,
	begPlanPIndexes, endPlanPIndexes *PlanPIndexes) bool {
	if !indexDef.PlanParams.PlanFrozen {
		return false
	}

	// Copy over the previous plan, if any, for the index.
	// If there is a change in the PlanFrozen status, then
	// the index definition UUID would have bumped. Need to
	// confirm this before copy over the previous plan.
	if begPlanPIndexes != nil && endPlanPIndexes != nil {
		for n, p := range begPlanPIndexes.PlanPIndexes {
			if p.IndexName == indexDef.Name &&
				(p.IndexUUID == indexDef.UUID ||
					sameIndexDefsExceptUUID(indexDef,
						getIndexDefFromPlanPIndexes([]*PlanPIndex{p}))) {
				endPlanPIndexes.PlanPIndexes[n] = p
			}
		}
	}

	return true
}

// --------------------------------------------------------

// NOTE: PlanPIndex.Name must be unique across the cluster and ideally
// functionally based off of the indexDef so that the SamePlanPIndex()
// comparison works even if concurrent planners are racing to
// calculate plans.
//
// NOTE: We can't use sourcePartitions directly as part of a
// PlanPIndex.Name suffix because in vbucket/hash partitioning the
// string would be too long -- since PIndexes might use
// PlanPIndex.Name for filesystem paths.
func PlanPIndexName(indexDef *IndexDef, sourcePartitions string) string {
	h := crc32.NewIEEE()
	io.WriteString(h, sourcePartitions)
	return indexDef.Name + "_" + indexDef.UUID + "_" +
		fmt.Sprintf("%08x", h.Sum32())
}

// --------------------------------------------------------

// PlanPIndexNodeRef represents an assignment of a pindex to a node.
type PlanPIndexNodeRef struct {
	UUID string
	Node *PlanPIndexNode
}

// PlanPIndexNodeRefs represents assignments of pindexes to nodes.
type PlanPIndexNodeRefs []*PlanPIndexNodeRef

func (pms PlanPIndexNodeRefs) Len() int {
	return len(pms)
}

func (pms PlanPIndexNodeRefs) Less(i, j int) bool {
	return pms[i].Node.Priority < pms[j].Node.Priority
}

func (pms PlanPIndexNodeRefs) Swap(i, j int) {
	pms[i], pms[j] = pms[j], pms[i]
}
