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
	"strings"
	"sync"
)

// ConsistencyParams represent the consistency requirements of a
// client's request.
type ConsistencyParams struct {
	// A Level value of "" means stale is ok; "at_plus" means we need
	// consistency at least at or beyond the consistency vector but
	// not before.
	Level string `json:"level"`

	// Keyed by indexName.
	Vectors map[string]ConsistencyVector `json:"vectors"`

	// A Results value of "complete" means that results will be
	// returned only if results are acquired from all the partitions,
	// a default of "" means that results will be returned no matter
	// if all pindexes are reachable or not (partial or full).
	Results string `json:"results,omitempty"`
}

// Key is partition or partition/partitionUUID.  Value is seq.
// For example, a DCP data source might have the key as either
// "vbucketId" or "vbucketId/vbucketUUID".
type ConsistencyVector map[string]uint64

// ConsistencyWaiter interface represents a service that can wait for
// consistency.
type ConsistencyWaiter interface {
	ConsistencyWait(partition, partitionUUID string,
		consistencyLevel string,
		consistencySeq uint64,
		cancelCh <-chan bool) error
}

// A ConsistencyWaitReq represents a runtime consistency wait request
// for a partition.
type ConsistencyWaitReq struct {
	PartitionUUID    string
	ConsistencyLevel string
	ConsistencySeq   uint64
	CancelCh         <-chan bool
	DoneCh           chan error
}

// An ErrorConsistencyWait represents an error or timeout while
// waiting for a partition to reach some consistency requirements.
type ErrorConsistencyWait struct {
	Err    error  // The underlying, wrapped error.
	Status string // Short status reason, like "timeout", "cancelled", etc.

	// Keyed by partitionId, value is pair of start/end seq's.
	StartEndSeqs map[string][]uint64
}

func (e *ErrorConsistencyWait) Error() string {
	return fmt.Sprintf("ErrorConsistencyWait, startEndSeqs: %#v,"+
		" err: %v", e.StartEndSeqs, e.Err)
}

// ErrorLocalPIndexHealth represents the unavailable pindexes and
// the corresponding error details which is discovered during the
// consistency checks.
type ErrorLocalPIndexHealth struct {
	IndexErrMap map[string]error
}

func (e *ErrorLocalPIndexHealth) Error() string {
	return "pindex_consistency: some pindexes not available"
}

// ---------------------------------------------------------

// ConsistencyWaitDone() waits for either the cancelCh or doneCh to
// finish, and provides the partition's seq if it was the cancelCh.
func ConsistencyWaitDone(partition string,
	cancelCh <-chan bool,
	doneCh chan error,
	currSeq func() uint64) error {
	seqStart := currSeq()

	select {
	case <-cancelCh:
		rv := map[string][]uint64{}
		rv[partition] = []uint64{seqStart, currSeq()}

		err := fmt.Errorf("pindex_consistency: ConsistencyWaitDone cancelled")

		return &ErrorConsistencyWait{ // TODO: track stats.
			Err:          err,
			Status:       "cancelled",
			StartEndSeqs: rv,
		}

	case err := <-doneCh:
		return err // TODO: track stats.
	}
}

// ConsistencyWaitPIndex waits for all the partitions in a pindex to
// reach the required consistency level.
func ConsistencyWaitPIndex(pindex *PIndex, t ConsistencyWaiter,
	consistencyParams *ConsistencyParams, cancelCh <-chan bool) error {
	if consistencyParams != nil &&
		consistencyParams.Level != "" &&
		consistencyParams.Vectors != nil {
		consistencyVector := consistencyParams.Vectors[pindex.IndexName]
		if consistencyVector != nil {
			err := ConsistencyWaitPartitions(t, pindex.sourcePartitionsMap,
				consistencyParams.Level, consistencyVector, cancelCh)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ConsistencyWaitGroup waits for all the partitions from a group of
// pindexes to reach a required consistency level.
func ConsistencyWaitGroup(indexName string,
	consistencyParams *ConsistencyParams, cancelCh <-chan bool,
	localPIndexes []*PIndex,
	addLocalPIndex func(*PIndex) error) error {
	var errConsistencyM sync.Mutex
	var errConsistency error
	var wg sync.WaitGroup
	indexErrMap := make(map[string]error)

	for _, localPIndex := range localPIndexes {
		err := addLocalPIndex(localPIndex)
		if err != nil {
			indexErrMap[localPIndex.Name] = err
			continue
		}

		if consistencyParams != nil &&
			consistencyParams.Level != "" &&
			consistencyParams.Vectors != nil {
			consistencyVector := consistencyParams.Vectors[indexName]
			if consistencyVector != nil {
				wg.Add(1)
				go func(localPIndex *PIndex,
					consistencyVector map[string]uint64) {
					defer wg.Done()

					err := ConsistencyWaitPartitions(localPIndex.Dest,
						localPIndex.sourcePartitionsMap,
						consistencyParams.Level,
						consistencyVector,
						cancelCh)
					if err != nil {
						errConsistencyM.Lock()
						errConsistency = err
						errConsistencyM.Unlock()
					}
				}(localPIndex, consistencyVector)
			}
		}
	}

	wg.Wait()

	if errConsistency != nil {
		return errConsistency
	}

	if cancelCh != nil {
		select {
		case <-cancelCh:
			return fmt.Errorf("pindex_consistency: ConsistencyWaitGroup cancelled")
		default:
		}
	}

	if len(indexErrMap) > 0 {
		// If there are unhealthy local pindexes, then return the
		// details to propagate to the final search results.
		return &ErrorLocalPIndexHealth{IndexErrMap: indexErrMap}
	}

	// TODO: There's likely a race here where at this point we've now
	// waited for all the (local) pindexes to reach the requested
	// consistency levels, but before we actually can use the
	// constructed alias and kick off a query, an adversary does a
	// rollback.  Using the alias to query after that might now be
	// incorrectly running against data some time back in the past.

	return nil
}

// ConsistencyWaitPartitions waits for the given partitions to reach
// the required consistency level.
func ConsistencyWaitPartitions(
	t ConsistencyWaiter,
	partitions map[string]bool,
	consistencyLevel string,
	consistencyVector map[string]uint64,
	cancelCh <-chan bool) error {
	// Key of consistencyVector looks like either just "partition" or
	// like "partition/partitionUUID".
	for k, consistencySeq := range consistencyVector {
		if consistencySeq > 0 {
			arr := strings.Split(k, "/")
			partition := arr[0]
			_, exists := partitions[partition]
			if exists {
				partitionUUID := ""
				if len(arr) > 1 {
					partitionUUID = arr[1]
				}
				err := t.ConsistencyWait(partition, partitionUUID,
					consistencyLevel, consistencySeq, cancelCh)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------

// A CwrQueue is a consistency wait request queue, implementing the
// heap.Interface for ConsistencyWaitReq's, and is heap ordered by
// sequence number.
type CwrQueue []*ConsistencyWaitReq

func (pq CwrQueue) Len() int { return len(pq) }

func (pq CwrQueue) Less(i, j int) bool {
	return pq[i].ConsistencySeq < pq[j].ConsistencySeq
}

func (pq CwrQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *CwrQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*ConsistencyWaitReq))
}

func (pq *CwrQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
