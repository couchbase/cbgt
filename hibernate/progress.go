//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package hibernate

func (hm *Manager) ReportProgress(
	onProgress func(progressEntries map[string]float64, errs []error)) error {
	var firstError error
	for progress := range hm.progressCh {
		if progress.Error != nil {
			hm.Logf("progress: error, progress: %+v", progress)

			if firstError == nil {
				firstError = progress.Error
			}

			onProgress(progress.TransferProgress, []error{progress.Error})
			hm.Stop()
			continue
		}

		onProgress(progress.TransferProgress, nil)

		// TransferProgress contains pindexes which belong to the list of indexes to be
		// hibernated.
		for _, transferProgress := range progress.TransferProgress {
			if transferProgress < 1 {
				// if any of the pindexes belonging to the indexes to be hibernated have
				// progress < 1, continue listening to the channel.
				break
			}
		}
	}

	return firstError
}
