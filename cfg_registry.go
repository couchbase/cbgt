//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

// Manager components that can subscribe to cfg changes
const (
	componentPlanner int = iota
	componentJanitor
	componentIndexDefsCache
	componentPlanPIndexesCache
	componentNodeDefsCache
	componentRebalanceStatus
)

type cfgSubscription struct {
	// manager component -> subscription keys
	keys map[int][]string
}

// -----------------------------------------------------------------------------

// # cfg subscription Keys Registry
// keyed by cfgName
// cfgName: "" is reserved for default subscription keys for all components
var cfgSubscriptions = map[string]*cfgSubscription{
	// default subscriptions
	"": &cfgSubscription{
		keys: map[int][]string{
			componentPlanner: []string{
				INDEX_DEFS_KEY,
				CfgNodeDefsKey(NODE_DEFS_WANTED),
			},
			componentJanitor: []string{
				PLAN_PINDEXES_KEY,
				CfgNodeDefsKey(NODE_DEFS_WANTED),
			},
			componentIndexDefsCache: []string{
				INDEX_DEFS_KEY,
				MANAGER_CLUSTER_OPTIONS_KEY,
			},
			componentPlanPIndexesCache: []string{PLAN_PINDEXES_KEY},
			componentNodeDefsCache: []string{
				CfgNodeDefsKey(NODE_DEFS_KNOWN),
				CfgNodeDefsKey(NODE_DEFS_WANTED),
			},
			componentRebalanceStatus: []string{
				LAST_REBALANCE_STATUS_KEY,
			},
		},
	},

	// metaKV overrides
	CFG_METAKV: &cfgSubscription{
		keys: map[int][]string{
			componentJanitor: []string{
				PLAN_PINDEXES_DIRECTORY_STAMP,
				CfgNodeDefsKey(NODE_DEFS_WANTED),
			},
			componentPlanPIndexesCache: []string{PLAN_PINDEXES_DIRECTORY_STAMP},
		},
	},
}

// Method to get subscription keys for the given component from the registry.
// If not found, try to get the default subscription keys for the component.
func getCfgSubscriptionKeys(cfgName string, component int) []string {
	cfgNames := []string{cfgName, ""} // order matters
	for _, name := range cfgNames {
		if subscription, ok := cfgSubscriptions[name]; ok {
			if keys, ok := subscription.keys[component]; ok {
				return keys
			}
		}
	}

	return nil
}
