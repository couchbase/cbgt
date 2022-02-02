//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"fmt"
	"strconv"
	"sync"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

var plannerStepsMutex sync.Mutex

// PlannerSteps helps command-line tools implement the planner steps:
// * "unregister" - unregisters nodesRemove from the cfg.
// * "planner" - runs the planner to save a new plan into the cfg.
// * "failover" - a composite step, comprised of "unregister" and "failover_".
// * "failover_" - processes the nodesRemove as nodes to be failover'ed.
//
// The "NODES-REMOVE-ALL" step overrides the nodesRemove with every
// known and wanted node.  This can have a lot of impact, and was
// meant to be used for cluster cleanup/purging situations.
func PlannerSteps(steps map[string]bool,
	cfg cbgt.Cfg, version, server string, options map[string]string,
	nodesRemove []string, dryRun bool, plannerFilter cbgt.PlannerFilter) error {
	// serialise the access to the PlannerSteps to reduce planning conflicts.
	plannerStepsMutex.Lock()
	defer plannerStepsMutex.Unlock()

	if steps != nil && steps["failover"] {
		steps["unregister"] = true
		steps["failover_"] = true
	}

	if steps != nil && steps["NODES-REMOVE-ALL"] {
		nodesRemove = nil

		nodesSeen := map[string]bool{}
		for _, kind := range []string{
			cbgt.NODE_DEFS_WANTED,
			cbgt.NODE_DEFS_KNOWN,
		} {
			nodeDefs, _, err := cbgt.CfgGetNodeDefs(cfg, kind)
			if err != nil {
				return err
			}

			for _, nodeDef := range nodeDefs.NodeDefs {
				if !nodesSeen[nodeDef.UUID] {
					nodesSeen[nodeDef.UUID] = true
					nodesRemove = append(nodesRemove, nodeDef.UUID)
				}
			}
		}
	}

	log.Printf("planner: nodesRemove: %#v", nodesRemove)

	if steps != nil && steps["unregister"] {
		log.Printf("planner: step unregister")

		if !dryRun {
			err := cbgt.UnregisterNodes(cfg, cbgt.CfgGetVersion(cfg), nodesRemove)
			if err != nil {
				return err
			}
		}
	}

	if steps != nil && steps["planner"] {
		log.Printf("planner: step planner")

		if !dryRun {
			_, err :=
				cbgt.Plan(cfg, cbgt.VERSION, "", server, options, plannerFilter)
			if err != nil {
				return err
			}
		}
	}

	if steps != nil && steps["failover_"] {
		log.Printf("planner: step failover_")

		if !dryRun {
			_, err := Failover(cfg, cbgt.VERSION, server, options, nodesRemove)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Failover promotes replicas to primary for the remaining nodes.
func Failover(cfg cbgt.Cfg, version string, server string,
	options map[string]string, nodesFailover []string) (bool, error) {
	mapNodesFailover := cbgt.StringsToMap(nodesFailover)

	uuid := ""

	indexDefs, nodeDefs, planPIndexesPrev, cas, err :=
		cbgt.PlannerGetPlan(cfg, version, uuid)
	if err != nil {
		return false, err
	}

	// use the effective version while calculating the new plan.
	// In a mixed node cluster, if user fails over - one of the upgraded
	// nodes and the failover rebalance picks the master from any other
	// remaining upgraded nodes, then we need to use the effective version
	// rather than the cbgt version.
	eVersion := cbgt.CfgGetVersion(cfg)
	if eVersion != version {
		log.Printf("planner: failover, current version: %s, effective"+
			"Cfg version used: %s", version, eVersion)
		version = eVersion
	}

	planPIndexesCalc, err := cbgt.CalcPlan("failover",
		indexDefs, nodeDefs, planPIndexesPrev, version, server, options, nil)
	if err != nil {
		return false, fmt.Errorf("planner: failover CalcPlan, err: %v", err)
	}

	planPIndexesNext := cbgt.CopyPlanPIndexes(planPIndexesPrev, version)
	for planPIndexName, planPIndex := range planPIndexesNext.PlanPIndexes {
		for node, planPIndexNode := range planPIndex.Nodes {
			if !mapNodesFailover[node] {
				continue
			}

			if planPIndexNode.Priority <= 0 {
				// Failover'ed node used to be a primary for this
				// pindex, so find a replica to promote.
				promoted := ""

			PROMOTE_REPLICA:
				for nodePro, ppnPro := range planPIndex.Nodes {
					if mapNodesFailover[nodePro] {
						continue
					}

					if ppnPro.Priority >= 1 {
						ppnPro.Priority = 0
						planPIndex.Nodes[nodePro] = ppnPro
						promoted = nodePro
						break PROMOTE_REPLICA
					}
				}

				// If we didn't find a replica to promote, and we're
				// configured with the option to
				// "failoverAssignAllPrimaries-IndexName" or
				// "failoverAssignAllPrimaries" (default true), then
				// assign the primary from the calculated plan.
				if promoted == "" && ParseOptionsBool(options,
					"failoverAssignAllPrimaries", planPIndex.IndexName, true) {
					planPIndexCalc, exists :=
						planPIndexesCalc.PlanPIndexes[planPIndexName]
					if exists && planPIndexCalc != nil {
					ASSIGN_PRIMARY:
						for nodeCalc, ppnCalc := range planPIndexCalc.Nodes {
							if ppnCalc.Priority <= 0 &&
								!mapNodesFailover[nodeCalc] {
								planPIndex.Nodes[nodeCalc] = ppnCalc
								break ASSIGN_PRIMARY
							}
						}
					}
				}
			}

			delete(planPIndex.Nodes, node)
		}
	}

	// TODO: Missing under-replication constraint warnings.

	if cbgt.SamePlanPIndexes(planPIndexesNext, planPIndexesPrev) {
		return false, nil
	}

	_, err = cbgt.CfgSetPlanPIndexes(cfg, planPIndexesNext, cas)
	if err != nil {
		return false, fmt.Errorf("planner: failover could not save plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
	}

	return true, nil
}

// ParseOptionsBool parses the options "name-suffix" and then "name"
// as boolean (strconv.ParseBool), otherwise returns defaultVal.
func ParseOptionsBool(options map[string]string, name, suffix string,
	defaultVal bool) bool {
	if options != nil {
		for _, optionName := range []string{name + "-" + suffix, name} {
			if v, exists := options[optionName]; exists {
				vb, err := strconv.ParseBool(v)
				if err == nil {
					return vb
				}
			}
		}
	}

	return defaultVal
}
