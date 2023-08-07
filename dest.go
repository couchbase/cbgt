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
	"io"
	"sync/atomic"

	log "github.com/couchbase/clog"
	"github.com/rcrowley/go-metrics"
)

// Dest interface defines the data sink or destination for data that
// cames from a data-source.  In other words, a data-source (or a Feed
// instance) is hooked up to one or more Dest instances.  As a Feed
// receives incoming data, the Feed will invoke methods on its Dest
// instances.
type Dest interface {
	// Invoked by PIndex.Close().
	Close(remove bool) error

	// Invoked when there's a new mutation from a data source for a
	// partition.  Dest implementation is responsible for making its
	// own copies of the key, val and extras data.
	DataUpdate(partition string, key []byte, seq uint64, val []byte,
		cas uint64,
		extrasType DestExtrasType, extras []byte) error

	// Invoked by the data source when there's a data deletion in a
	// partition.  Dest implementation is responsible for making its
	// own copies of the key and extras data.
	DataDelete(partition string, key []byte, seq uint64,
		cas uint64,
		extrasType DestExtrasType, extras []byte) error

	// An callback invoked by the data source when there's a start of
	// a new snapshot for a partition.  The Receiver implementation,
	// for example, might choose to optimize persistence perhaps by
	// preparing a batch write to application-specific storage.
	SnapshotStart(partition string, snapStart, snapEnd uint64) error

	// OpaqueGet() should return the opaque value previously
	// provided by an earlier call to OpaqueSet().  If there was no
	// previous call to OpaqueSet(), such as in the case of a brand
	// new instance of a Dest (as opposed to a restarted or reloaded
	// Dest), the Dest should return (nil, 0, nil) for (value,
	// lastSeq, err), respectively.  The lastSeq should be the last
	// sequence number received and persisted during calls to the
	// Dest's DataUpdate() & DataDelete() methods.
	OpaqueGet(partition string) (value []byte, lastSeq uint64, err error)

	// The Dest implementation should persist the value parameter of
	// OpaqueSet() for retrieval during some future call to
	// OpaqueGet() by the system.  The metadata value should be
	// considered "in-stream", or as part of the sequence history of
	// mutations.  That is, a later Rollback() to some previous
	// sequence number for a particular partition should rollback
	// both persisted metadata and regular data.  The Dest
	// implementation should make its own copy of the value data.
	OpaqueSet(partition string, value []byte) error

	// Invoked by when the datasource signals a rollback during dest
	// initialization.  Note that both regular data and opaque data
	// should be rolled back to at a maximum of the rollbackSeq.  Of
	// note, the Dest is allowed to rollback even further, even all
	// the way back to the start or to zero.
	Rollback(partition string, rollbackSeq uint64) error

	// Blocks until the Dest has reached the desired consistency for
	// the partition or until the cancelCh is readable or closed by
	// some goroutine related to the calling goroutine.  The error
	// response might be a ErrorConsistencyWait instance, which has
	// StartEndSeqs information.  The seqStart is the seq number when
	// the operation started waiting and the seqEnd is the seq number
	// at the end of operation (even when cancelled or error), so that
	// the caller might get a rough idea of ingest velocity.
	ConsistencyWait(partition, partitionUUID string,
		consistencyLevel string,
		consistencySeq uint64,
		cancelCh <-chan bool) error

	// Counts the underlying pindex implementation.
	Count(pindex *PIndex, cancelCh <-chan bool) (uint64, error)

	// Queries the underlying pindex implementation, blocking if
	// needed for the Dest to reach the desired consistency.
	Query(pindex *PIndex, req []byte, w io.Writer,
		cancelCh <-chan bool) error

	Stats(io.Writer) error
}

// DestEx interface defines the data sink or destination for data that
// comes from a data-source for any generic implementations.
// For eg: xattrs listeners may choose to implement this interface
type DestEx interface {
	// Invoked when there's a new mutation from a data source for a
	// partition. DestEx implementation is responsible for interpreting
	// the header and body contents of *gomemcached.MCRequest
	// and making its own copies of the key, val extras' data.
	DataUpdateEx(partition string, key []byte, seq uint64, val []byte,
		cas uint64,
		extrasType DestExtrasType, req interface{}) error

	// Invoked by the data source when there's a data deletion in a
	// partition. DestEx implementation is responsible for interpreting
	// the header and body contents of *gomemcached.MCRequest
	// and making its own copies of the key, val extras' data.
	DataDeleteEx(partition string, key []byte, seq uint64,
		cas uint64,
		extrasType DestExtrasType, req interface{}) error

	// Invoked by when the datasource signals a rollback during dest
	// initialization.  Note that both regular data and opaque data
	// should be rolled back to at a maximum of the rollbackSeq. Of
	// note, the DestEx is allowed to rollback even further, even all
	// the way back to the start or to zero.
	RollbackEx(partition string, partitionUUID uint64, rollbackSeq uint64) error
}

// DestExtrasType represents the encoding for the
// Dest.DataUpdate/DataDelete() extras parameter.
type DestExtrasType uint16

// DEST_EXTRAS_TYPE_NIL means there are no extras as part of a
// Dest.DataUpdate/DataDelete invocation.
const DEST_EXTRAS_TYPE_NIL = DestExtrasType(0)

// DestCollection interface needs to be implemented by the dest/pindex
// implementations which consumes data from the collections.
type DestCollection interface {
	// PrepareFeedParams provides a way for the pindex
	// implementation to customise any DCPFeedParams.
	PrepareFeedParams(partition string, params *DCPFeedParams) error

	// Invoked when there's a DCP message for a connection that uses
	// OSOBackfill indicating the start or the end of an OSO snapshot.
	// snapshotType:
	//   - 0x01: start
	//   - 0x02: end
	OSOSnapshot(partition string, snapshotType uint32) error

	// Invoked when there's a DCP message indicating that the consumer
	// should not expect sequence numbers up until the advertised
	// sequence number.
	SeqNoAdvanced(partition string, seq uint64) error

	// Invoked when there's a DCP message indicating a collection that
	// the feed has subscribed to was created.
	CreateCollection(partition string, manifestUid uint64,
		scopeId, collectionId uint32, seq uint64) error

	// Invoked when there's a DCP message indicating a collection that
	// the feed has subscribed to was deleted.
	DeleteCollection(partition string, manifestUid uint64,
		scopeId, collectionId uint32, seq uint64) error

	// Invoked when there's a DCP message indicating a collection that
	// the feed has subscribed to was flushed.
	FlushCollection(partition string, manifestUid uint64,
		scopeId, collectionId uint32, seq uint64) error

	// Invoked when there's a DCP message indicating a collection that
	// the feed has subscribed to was modified.
	ModifyCollection(partition string, manifestUid uint64,
		scopeId, collectionId uint32, seq uint64) error
}

// DestStats holds the common stats or metrics for a Dest.
type DestStats struct {
	TotError uint64

	TimerDataUpdate       metrics.Timer
	TimerDataDelete       metrics.Timer
	TimerSnapshotStart    metrics.Timer
	TimerOpaqueGet        metrics.Timer
	TimerOpaqueSet        metrics.Timer
	TimerRollback         metrics.Timer
	TimerCreateCollection metrics.Timer
	TimerSeqNoAdvanced    metrics.Timer
}

// NewDestStats creates a new, ready-to-use DestStats.
func NewDestStats() *DestStats {
	return &DestStats{
		TimerDataUpdate:       metrics.NewTimer(),
		TimerDataDelete:       metrics.NewTimer(),
		TimerSnapshotStart:    metrics.NewTimer(),
		TimerOpaqueGet:        metrics.NewTimer(),
		TimerOpaqueSet:        metrics.NewTimer(),
		TimerRollback:         metrics.NewTimer(),
		TimerCreateCollection: metrics.NewTimer(),
		TimerSeqNoAdvanced:    metrics.NewTimer(),
	}
}

func (d *DestStats) WriteJSON(w io.Writer) {
	t := atomic.LoadUint64(&d.TotError)
	fmt.Fprintf(w, `{"TotError":%d`, t)

	w.Write([]byte(`,"TimerDataUpdate":`))
	WriteTimerJSON(w, d.TimerDataUpdate)
	w.Write([]byte(`,"TimerDataDelete":`))
	WriteTimerJSON(w, d.TimerDataDelete)
	w.Write([]byte(`,"TimerSnapshotStart":`))
	WriteTimerJSON(w, d.TimerSnapshotStart)
	w.Write([]byte(`,"TimerOpaqueGet":`))
	WriteTimerJSON(w, d.TimerOpaqueGet)
	w.Write([]byte(`,"TimerOpaqueSet":`))
	WriteTimerJSON(w, d.TimerOpaqueSet)
	w.Write([]byte(`,"TimerRollback":`))
	WriteTimerJSON(w, d.TimerRollback)
	w.Write([]byte(`,"TimerCreateCollection":`))
	WriteTimerJSON(w, d.TimerCreateCollection)
	w.Write([]byte(`,"TimerSeqNoAdvanced":`))
	WriteTimerJSON(w, d.TimerSeqNoAdvanced)

	w.Write(JsonCloseBrace)
}

// A DestPartitionFunc allows a level of indirection/abstraction for
// the Feed-to-Dest relationship.  A Feed is hooked up in a
// one-to-many relationship with multiple Dest instances.  The
// DestPartitionFunc provided to a Feed instance defines the mapping
// of which Dest the Feed should invoke when the Feed receives an
// incoming data item.
//
// The partition parameter is encoded as a string, instead of a uint16
// or number, to allow for future range partitioning functionality.
type DestPartitionFunc func(partition string, key []byte,
	dests map[string]Dest) (Dest, error)

// This basic partition func first tries a direct lookup by partition
// string, else it tries the "" partition.
func BasicPartitionFunc(partition string, key []byte,
	dests map[string]Dest) (Dest, error) {
	dest, exists := dests[partition]
	if exists {
		return dest, nil
	}
	dest, exists = dests[""]
	if exists {
		return dest, nil
	}
	return nil, fmt.Errorf("dest: no dest for key: %s,"+
		" partition: %s, dests: %#v", log.Tag(log.UserData, key),
		partition, dests)
}
