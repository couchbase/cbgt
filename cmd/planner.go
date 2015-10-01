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

package cmd

import (
	"fmt"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

// PlannerSteps helps command-line tools implement the planner steps:
// * "unregister" - unregisters nodesToRemove from the cfg.
// * "planner" - runs the planner to save a new plan into the cfg.
//
// The default steps are "unregister" and "planner".
//
// An additional composite step, "FAILOVER" (fully capitalized), is
// used to process the nodesToRemove as nodes to be failover'ed.
// "FAILOVER" is comprised of the lower-level steps of "unregister"
// and "failover" (all lowercase).
func PlannerSteps(steps map[string]bool,
	cfg cbgt.Cfg, version, server string, nodesToRemove []string,
	dryRun bool) error {
	if steps == nil || steps["unregister"] || steps["FAILOVER"] {
		log.Printf("planner: step unregister")

		if !dryRun {
			err := cbgt.UnregisterNodes(cfg, cbgt.VERSION, nodesToRemove)
			if err != nil {
				return err
			}
		}
	}

	if steps == nil || steps["planner"] {
		log.Printf("planner: step planner")

		if !dryRun {
			_, err := cbgt.Plan(cfg, cbgt.VERSION, "", server)
			if err != nil {
				return err
			}
		}
	}

	if steps["failover"] || steps["FAILOVER"] {
		log.Printf("planner: step failover")

		if !dryRun {
			_, err := Failover(cfg, cbgt.VERSION, server, nodesToRemove)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Failover promotes replicas to primary for the remaining nodes.
func Failover(cfg cbgt.Cfg, version string, server string,
	nodesFailover []string) (bool, error) {
	mapNodesFailover := cbgt.StringsToMap(nodesFailover)

	uuid := ""

	indexDefs, nodeDefs, planPIndexesPrev, cas, err :=
		cbgt.PlannerGetPlan(cfg, version, uuid)
	if err != nil {
		return false, err
	}

	planPIndexesCalc, err := cbgt.CalcPlan("failover",
		indexDefs, nodeDefs, planPIndexesPrev, version, server)
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

				if promoted == "" {
					// Didn't find a replica to promote, so consult the
					// calculated plan for the primary assignment.
					planPIndexCalc, exists :=
						planPIndexesCalc.PlanPIndexes[planPIndexName]
					if exists && planPIndexCalc != nil {
					PROMOTE_CALC:
						for nodeCalc, ppnCalc := range planPIndexCalc.Nodes {
							if ppnCalc.Priority <= 0 &&
								!mapNodesFailover[nodeCalc] {
								planPIndex.Nodes[nodeCalc] = ppnCalc
								promoted = nodeCalc
								break PROMOTE_CALC
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
