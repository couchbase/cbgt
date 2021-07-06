//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

func init() {
	RegisterFeedType("primary", &FeedType{
		Start: func(mgr *Manager, feedName, indexName, indexUUID,
			sourceType, sourceName, sourceUUID, params string,
			dests map[string]Dest) error {
			return mgr.registerFeed(NewPrimaryFeed(feedName, indexName,
				BasicPartitionFunc, dests))
		},
		Partitions:  PrimaryFeedPartitions,
		Public:      false,
		Description: "general/primary - a primary data source",
		StartSample: &PrimarySourceParams{},
	})
}

// A PrimaryFeed implements both the Feed and Dest interfaces, for
// chainability; and is also useful for testing.
//
// One motivation for a PrimaryFeed implementation is from the
// realization that some pindex backends might not actually be
// secondary indexes, but are instead better considered as primary
// data sources in their own right.  For example, you can imagine some
// kind of KeyValuePIndex backend.  The system design, however, still
// requires hooking such "primary pindexes" up to a feed.  Instead of
// using a NILFeed, you might instead use a PrimaryFeed, as unlike a
// NILFeed the PrimaryFeed provides a "NumPartitions" functionality.
type PrimaryFeed struct {
	name      string
	indexName string
	pf        DestPartitionFunc
	dests     map[string]Dest
}

func NewPrimaryFeed(name, indexName string, pf DestPartitionFunc,
	dests map[string]Dest) *PrimaryFeed {
	return &PrimaryFeed{
		name:      name,
		indexName: indexName,
		pf:        pf,
		dests:     dests,
	}
}

func (t *PrimaryFeed) Name() string {
	return t.name
}

func (t *PrimaryFeed) IndexName() string {
	return t.indexName
}

func (t *PrimaryFeed) Start() error {
	return nil
}

func (t *PrimaryFeed) Close() error {
	return nil
}

func (t *PrimaryFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *PrimaryFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}

// -----------------------------------------------------

// PrimarySourceParams represents the JSON for the sourceParams for a
// primary feed.
type PrimarySourceParams struct {
	NumPartitions int `json:"numPartitions"`
}

// PrimaryFeedPartitions generates partition strings based on a
// PrimarySourceParams.NumPartitions parameter.
func PrimaryFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	server string, options map[string]string) ([]string, error) {
	dsp := &PrimarySourceParams{}
	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), dsp)
		if err != nil {
			return nil, fmt.Errorf("feed_primary: PrimaryFeedPartitions"+
				" could not parse sourceParams: %s, err: %v", sourceParams, err)
		}
	}
	numPartitions := dsp.NumPartitions
	rv := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		rv[i] = strconv.Itoa(i)
	}
	return rv, nil
}

// -----------------------------------------------------

func (t *PrimaryFeed) DataUpdate(partition string,
	key []byte, seq uint64, val []byte,
	cas uint64,
	extrasType DestExtrasType, extras []byte) error {
	dest, err := t.pf(partition, key, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.DataUpdate(partition, key, seq, val, cas, extrasType, extras)
}

func (t *PrimaryFeed) DataDelete(partition string,
	key []byte, seq uint64,
	cas uint64,
	extrasType DestExtrasType, extras []byte) error {
	dest, err := t.pf(partition, key, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.DataDelete(partition, key, seq, cas, extrasType, extras)
}

func (t *PrimaryFeed) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.SnapshotStart(partition, snapStart, snapEnd)
}

func (t *PrimaryFeed) OpaqueSet(partition string,
	value []byte) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.OpaqueSet(partition, value)
}

func (t *PrimaryFeed) OpaqueGet(partition string) (
	value []byte, lastSeq uint64, err error) {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return nil, 0, fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.OpaqueGet(partition)
}

func (t *PrimaryFeed) Rollback(partition string,
	rollbackSeq uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.Rollback(partition, rollbackSeq)
}

func (t *PrimaryFeed) RollbackEx(partition string,
	vBucketUUID uint64, rollbackSeq uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}

	if destEx, ok := dest.(DestEx); ok {
		return destEx.RollbackEx(partition, vBucketUUID, rollbackSeq)
	}
	return dest.Rollback(partition, rollbackSeq)
}

func (t *PrimaryFeed) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.ConsistencyWait(partition, partitionUUID,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *PrimaryFeed) Count(pindex *PIndex, cancelCh <-chan bool) (
	uint64, error) {
	return 0, fmt.Errorf("feed_primary: PrimaryFeed.Count unimplemented")
}

func (t *PrimaryFeed) Query(pindex *PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return fmt.Errorf("feed_primary: PrimaryFeed.Query unimplemented")
}
