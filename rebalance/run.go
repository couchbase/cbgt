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

package rebalance

import (
	"fmt"

	"github.com/couchbase/cbgt"
)

// RunRebalance synchronously runs a rebalance and reports progress
// until the rebalance is done or has errored.
func RunRebalance(cfg cbgt.Cfg, server string, nodesToRemove []string,
	favorMinNodes bool, dryRun bool, verbose int,
	progressToString ProgressToString) error {
	r, err := StartRebalance(cbgt.VERSION, cfg, server,
		nodesToRemove,
		RebalanceOptions{
			FavorMinNodes: favorMinNodes,
			DryRun:        dryRun,
			Verbose:       verbose,
		})
	if err != nil {
		return fmt.Errorf("run: StartRebalance, err: %v", err)
	}

	if progressToString == nil {
		progressToString = ProgressTableString
	}

	err = ReportProgress(r, progressToString)
	if err != nil {
		return fmt.Errorf("run: reportProgress, err: %v", err)
	}

	r.Stop()

	return nil
}
