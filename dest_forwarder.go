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
)

// A DestForwarder implements the Dest interface by forwarding method
// calls to the Dest returned by a DestProvider.
//
// It is useful for pindex backend implementations that have their own
// level-of-indirection features.  One example would be pindex
// backends that track a separate batch per partition.
type DestForwarder struct {
	DestProvider DestProvider
}

// A DestProvider returns the Dest to use for different kinds of
// operations and is used in conjunction with a DestForwarder.
type DestProvider interface {
	Dest(partition string) (Dest, error)

	Count(pindex *PIndex, cancelCh <-chan bool) (uint64, error)

	Query(pindex *PIndex, req []byte, res io.Writer,
		cancelCh <-chan bool) error

	Stats(io.Writer) error

	Close(bool) error
}

func (t *DestForwarder) Close(remove bool) error {
	return t.DestProvider.Close(remove)
}

func (t *DestForwarder) DataUpdate(partition string,
	key []byte, seq uint64, val []byte,
	cas uint64,
	extrasType DestExtrasType, extras []byte) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.DataUpdate(partition, key, seq, val,
		cas, extrasType, extras)
}

func (t *DestForwarder) DataUpdateEx(partition string,
	key []byte, seq uint64, val []byte, cas uint64,
	extrasType DestExtrasType, req interface{}) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	if destEx, ok := dest.(DestEx); ok {
		return destEx.DataUpdateEx(partition, key, seq, val,
			cas, extrasType, req)
	}

	return fmt.Errorf("dest_forwarder: no DestEx"+
		" implementation found for partition %s", partition)
}

func (t *DestForwarder) DataDelete(partition string,
	key []byte, seq uint64,
	cas uint64,
	extrasType DestExtrasType, extras []byte) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.DataDelete(partition, key, seq,
		cas, extrasType, extras)
}

func (t *DestForwarder) DataDeleteEx(partition string,
	key []byte, seq uint64, cas uint64,
	extrasType DestExtrasType, req interface{}) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	if destEx, ok := dest.(DestEx); ok {
		return destEx.DataDeleteEx(partition, key, seq,
			cas, extrasType, req)
	}

	return fmt.Errorf("dest_forwarder: no DestEx "+
		"implementation found for partition %s", partition)
}

func (t *DestForwarder) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.SnapshotStart(partition, snapStart, snapEnd)
}

func (t *DestForwarder) PrepareFeedParams(partition string,
	params *DCPFeedParams) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.PrepareFeedParams(partition, params)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (PrepareFeedParams) for partition %s",
		partition)
}

func (t *DestForwarder) OSOSnapshot(partition string,
	snapshotType uint32) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.OSOSnapshot(partition, snapshotType)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (OSOSnapshot) for partition %s",
		partition)
}

func (t *DestForwarder) SeqNoAdvanced(partition string,
	seq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.SeqNoAdvanced(partition, seq)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (SeqNoAdvanced) for partition %s",
		partition)
}

func (t *DestForwarder) CreateCollection(partition string,
	manifestUid uint64, scopeId, collectionId uint32, seq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.CreateCollection(partition, manifestUid,
			scopeId, collectionId, seq)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (CreateCollection) for partition %s",
		partition)
}

func (t *DestForwarder) DeleteCollection(partition string,
	manifestUid uint64, scopeId, collectionId uint32, seq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.DeleteCollection(partition, manifestUid,
			scopeId, collectionId, seq)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (DeleteCollection) for partition %s",
		partition)
}

func (t *DestForwarder) FlushCollection(partition string,
	manifestUid uint64, scopeId, collectionId uint32, seq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.FlushCollection(partition, manifestUid,
			scopeId, collectionId, seq)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (FlushCollection) for partition %s",
		partition)
}

func (t *DestForwarder) ModifyCollection(partition string,
	manifestUid uint64, scopeId, collectionId uint32, seq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destColl, ok := dest.(DestCollection); ok {
		return destColl.ModifyCollection(partition, manifestUid,
			scopeId, collectionId, seq)
	}

	return fmt.Errorf("dest_forwarder: no DestCollection "+
		"implementation found (ModifyCollection) for partition %s",
		partition)
}

func (t *DestForwarder) OpaqueGet(partition string) (
	value []byte, lastSeq uint64, err error) {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return nil, 0, err
	}

	return dest.OpaqueGet(partition)
}

func (t *DestForwarder) OpaqueSet(partition string, value []byte) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.OpaqueSet(partition, value)
}

func (t *DestForwarder) Rollback(partition string, rollbackSeq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.Rollback(partition, rollbackSeq)
}

func (t *DestForwarder) RollbackEx(partition string,
	vBucketUUID uint64,
	rollbackSeq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}
	if destEx, ok := dest.(DestEx); ok {
		return destEx.RollbackEx(partition, vBucketUUID, rollbackSeq)
	}
	return fmt.Errorf("dest_forwarder: no DestEx implementation found for"+
		" partition %s", partition)
}

func (t *DestForwarder) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.ConsistencyWait(partition, partitionUUID,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *DestForwarder) Count(pindex *PIndex, cancelCh <-chan bool) (
	uint64, error) {
	return t.DestProvider.Count(pindex, cancelCh)
}

func (t *DestForwarder) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	return t.DestProvider.Query(pindex, req, res, cancelCh)
}

func (t *DestForwarder) Stats(w io.Writer) error {
	return t.DestProvider.Stats(w)
}

func (t *DestForwarder) IsFeedable() (bool, error) {
	if f, ok := t.DestProvider.(Feedable); ok {
		return f.IsFeedable()
	}
	return true, nil
}
