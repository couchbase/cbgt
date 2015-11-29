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
	"sort"

	"github.com/couchbase/blance"

	"github.com/couchbase/cbgt"
)

// ProgressToString defines the callback when there's progress and a
// representative progress summary string needs to be generated for
// possible logging.
//
// The progressEntries is a map of pindex -> (source) partition ->
// node -> *ProgressEntry.
type ProgressToString func(maxNodeLen, maxPIndexLen int,
	seenNodes map[string]bool,
	seenNodesSorted []string,
	seenPIndexes map[string]bool,
	seenPIndexesSorted []string,
	progressEntries map[string]map[string]map[string]*ProgressEntry) string

// ProgressEntry represents a record of rebalance progress for a given
// pindex, source partition and node.
type ProgressEntry struct {
	PIndex, SourcePartition, Node string // Immutable.

	StateOp     StateOp
	InitUUIDSeq cbgt.UUIDSeq
	CurrUUIDSeq cbgt.UUIDSeq
	WantUUIDSeq cbgt.UUIDSeq

	Move int
	Done bool
}

// ReportProgress tracks progress in progress entries and invokes the
// progressToString handler, whose output will be logged.
func ReportProgress(r *Rebalancer,
	progressToString ProgressToString) error {
	var firstError error

	var lastEmit string

	maxNodeLen := 0
	maxPIndexLen := 0

	seenNodes := map[string]bool{}
	seenNodesSorted := []string(nil)

	// Map of pindex -> (source) partition -> node -> *ProgressEntry
	progressEntries := map[string]map[string]map[string]*ProgressEntry{}

	seenPIndexes := map[string]bool{}
	seenPIndexesSorted := []string(nil)

	updateProgressEntry := func(pindex, sourcePartition, node string,
		cb func(*ProgressEntry)) {
		if !seenNodes[node] {
			seenNodes[node] = true
			seenNodesSorted = append(seenNodesSorted, node)
			sort.Strings(seenNodesSorted)

			if maxNodeLen < len(node) {
				maxNodeLen = len(node)
			}
		}

		if maxPIndexLen < len(pindex) {
			maxPIndexLen = len(pindex)
		}

		sourcePartitions, exists := progressEntries[pindex]
		if !exists || sourcePartitions == nil {
			sourcePartitions = map[string]map[string]*ProgressEntry{}
			progressEntries[pindex] = sourcePartitions
		}

		nodes, exists := sourcePartitions[sourcePartition]
		if !exists || nodes == nil {
			nodes = map[string]*ProgressEntry{}
			sourcePartitions[sourcePartition] = nodes
		}

		progressEntry, exists := nodes[node]
		if !exists || progressEntry == nil {
			progressEntry = &ProgressEntry{
				PIndex:          pindex,
				SourcePartition: sourcePartition,
				Node:            node,
				Move:            -1,
			}
			nodes[node] = progressEntry
		}

		cb(progressEntry)

		// TODO: Check UUID matches, too.

		if !seenPIndexes[pindex] {
			seenPIndexes[pindex] = true
			seenPIndexesSorted =
				append(seenPIndexesSorted, pindex)

			sort.Strings(seenPIndexesSorted)
		}
	}

	for progress := range r.ProgressCh() {
		if progress.Error != nil {
			r.Logf("progress: error, progress: %+v", progress)

			if firstError == nil {
				firstError = progress.Error
			}

			r.Stop()

			continue
		}

		UpdateProgressEntries(r, updateProgressEntry)

		currEmit := progressToString(maxNodeLen, maxPIndexLen,
			seenNodes,
			seenNodesSorted,
			seenPIndexes,
			seenPIndexesSorted,
			progressEntries)
		if currEmit != lastEmit {
			r.Logf("%s", currEmit)
		}

		lastEmit = currEmit
	}

	return firstError
}

// UpdateProgressEntries invokes the updateProgressEntry callback to
// help maintain progress entries information.
func UpdateProgressEntries(
	r *Rebalancer,
	updateProgressEntry func(pindex, sourcePartition, node string,
		cb func(*ProgressEntry)),
) {
	r.Visit(func(
		currStates CurrStates,
		currSeqs CurrSeqs,
		wantSeqs WantSeqs,
		mapNextMoves map[string]*blance.NextMoves,
	) {
		for _, pindexes := range currStates {
			for pindex, nodes := range pindexes {
				for node, stateOp := range nodes {
					updateProgressEntry(pindex, "", node,
						func(pe *ProgressEntry) {
							pe.StateOp = stateOp
						})
				}
			}
		}

		for pindex, sourcePartitions := range currSeqs {
			for sourcePartition, nodes := range sourcePartitions {
				for node, currUUIDSeq := range nodes {
					updateProgressEntry(pindex,
						sourcePartition, node,
						func(pe *ProgressEntry) {
							pe.CurrUUIDSeq = currUUIDSeq

							if pe.InitUUIDSeq.UUID == "" {
								pe.InitUUIDSeq = currUUIDSeq
							}
						})
				}
			}
		}

		for pindex, sourcePartitions := range wantSeqs {
			for sourcePartition, nodes := range sourcePartitions {
				for node, wantUUIDSeq := range nodes {
					updateProgressEntry(pindex,
						sourcePartition, node,
						func(pe *ProgressEntry) {
							pe.WantUUIDSeq = wantUUIDSeq
						})
				}
			}
		}

		for pindex, nextMoves := range mapNextMoves {
			for i, nodeStateOp := range nextMoves.Moves {
				updateProgressEntry(pindex, "", nodeStateOp.Node,
					func(pe *ProgressEntry) {
						pe.Move = i
						pe.Done = i < nextMoves.Next
					})
			}
		}
	})
}
