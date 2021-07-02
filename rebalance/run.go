//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rebalance

import (
	"fmt"

	"github.com/couchbase/cbgt"
)

// RunRebalance synchronously runs a rebalance and reports progress
// until the rebalance is done or has errored.
func RunRebalance(cfg cbgt.Cfg, server string, options map[string]string,
	nodesToRemove []string, favorMinNodes bool, dryRun bool, verbose int,
	progressToString ProgressToString) error {
	r, err := StartRebalance(cbgt.VERSION, cfg, server, options,
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
