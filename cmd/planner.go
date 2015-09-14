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
	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
)

func PlannerSteps(steps map[string]bool,
	cfg cbgt.Cfg, version, server string, nodesToRemove []string,
	dryRun bool) error {
	if steps == nil || steps["unregister"] {
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

	return nil
}
