//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore/v10"
)

// DEST_EXTRAS_TYPE_GOCBCORE_DCP represents gocb DCP mutation/deletion metadata
// not included in DataUpdate/DataDelete (GocbcoreDCPExtras).
const DEST_EXTRAS_TYPE_GOCBCORE_DCP = DestExtrasType(0x0004)

// DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION represents gocb DCP mutation/deletion
// scope id and collection id written to []byte of len=8 (4bytes each)
const DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION = DestExtrasType(0x0005)

// GocbcoreDCPExtras packages additional DCP mutation metadata for use by
// DataUpdateEx, DataDeleteEx.
type GocbcoreDCPExtras struct {
	ScopeId      uint32
	CollectionId uint32
	Expiry       uint32
	Flags        uint32
	Datatype     uint8
	Value        []byte // carries xattr information (if available) for DataDeleteEx
}

// ----------------------------------------------------------------

// GocbcoreDCPFeed's implementation of the gocbcore.StreamObserver interface.

func (f *GocbcoreDCPFeed) SnapshotMarker(sm gocbcore.DcpSnapshotMarker) {
	if f.currVBs[sm.VbID] == nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] SnapshotMarker, invalid vb", sm.VbID, sm.StreamID))
		return
	}

	f.currVBs[sm.VbID].snapStart = sm.StartSeqNo
	f.currVBs[sm.VbID].snapEnd = sm.EndSeqNo
	f.currVBs[sm.VbID].snapSaved = false

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, sm.VbID, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if f.stopAfter != nil {
			uuidSeq, exists := f.stopAfter[partition]
			if exists && sm.EndSeqNo > uuidSeq.Seq { // TODO: Check UUID.
				// Clamp the snapEnd so batches are executed.
				sm.EndSeqNo = uuidSeq.Seq
			}
		}

		return dest.SnapshotStart(partition, sm.StartSeqNo, sm.EndSeqNo)
	}, f.stats.TimerSnapshotStart)

	if err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] SnapshotMarker, err: %v", sm.VbID, sm.StreamID, err))
		return
	}

	atomic.AddUint64(&f.dcpStats.TotDCPSnapshotMarkers, 1)
}

func (f *GocbcoreDCPFeed) Mutation(m gocbcore.DcpMutation) {
	if err := f.checkAndUpdateVBucketState(m.VbID); err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] Mutation, %v", m.VbID, m.StreamID, err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, m.VbID, m.Key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			extras := GocbcoreDCPExtras{
				Expiry:   m.Expiry,
				Flags:    m.Flags,
				Datatype: m.Datatype,
			}
			if f.agent.HasCollectionsSupport() {
				extras.ScopeId = f.streamOptions.FilterOptions.ScopeID
				extras.CollectionId = m.CollectionID
			}
			err = destEx.DataUpdateEx(partition, m.Key, m.SeqNo, m.Value, m.Cas,
				DEST_EXTRAS_TYPE_GOCBCORE_DCP, extras)
		} else {
			extras := make([]byte, 8) // 8 bytes needed to hold 2 uint32s
			binary.LittleEndian.PutUint32(extras[0:], f.scopeID)
			binary.LittleEndian.PutUint32(extras[4:], m.CollectionID)
			err = dest.DataUpdate(partition, m.Key, m.SeqNo, m.Value, m.Cas,
				DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION, extras)
		}

		if err != nil {
			return fmt.Errorf("name: %s, partition: %s, key: %v, seq: %d, err: %v",
				f.Name(), partition, log.Tag(log.UserData, m.Key), m.SeqNo, err)
		}

		f.updateStopAfter(partition, m.SeqNo)

		return nil
	}, f.stats.TimerDataUpdate)

	if err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] Mutation, err: %v", m.VbID, m.StreamID, err))
		return
	}

	f.lastReceivedSeqno[m.VbID] = m.SeqNo

	atomic.AddUint64(&f.dcpStats.TotDCPMutations, 1)
}

func (f *GocbcoreDCPFeed) Deletion(d gocbcore.DcpDeletion) {
	if err := f.checkAndUpdateVBucketState(d.VbID); err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] Deletion, %v", d.VbID, d.StreamID, err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, d.VbID, d.Key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			extras := GocbcoreDCPExtras{
				Datatype: d.Datatype,
				Value:    d.Value,
			}
			if f.agent.HasCollectionsSupport() {
				extras.ScopeId = f.streamOptions.FilterOptions.ScopeID
				extras.CollectionId = d.CollectionID
			}
			err = destEx.DataDeleteEx(partition, d.Key, d.SeqNo, d.Cas,
				DEST_EXTRAS_TYPE_GOCBCORE_DCP, extras)
		} else {
			extras := make([]byte, 8) // 8 bytes needed to hold 2 uint32s
			binary.LittleEndian.PutUint32(extras[0:], f.scopeID)
			binary.LittleEndian.PutUint32(extras[4:], d.CollectionID)
			err = dest.DataDelete(partition, d.Key, d.SeqNo, d.Cas,
				DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION, extras)
		}

		if err != nil {
			return fmt.Errorf("name: %s, partition: %s, key: %v, seq: %d, err: %v",
				f.Name(), partition, log.Tag(log.UserData, d.Key), d.SeqNo, err)
		}

		f.updateStopAfter(partition, d.SeqNo)

		return nil
	}, f.stats.TimerDataDelete)

	if err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] Deletion, err: %v", d.VbID, d.StreamID, err))
		return
	}

	f.lastReceivedSeqno[d.VbID] = d.SeqNo

	atomic.AddUint64(&f.dcpStats.TotDCPDeletions, 1)
}

func (f *GocbcoreDCPFeed) Expiration(e gocbcore.DcpExpiration) {
	f.Deletion(gocbcore.DcpDeletion{
		SeqNo:        e.SeqNo,
		RevNo:        e.RevNo,
		Cas:          e.Cas,
		DeleteTime:   e.DeleteTime,
		CollectionID: e.CollectionID,
		VbID:         e.VbID,
		StreamID:     e.StreamID,
		Key:          e.Key,
	})
}

func (f *GocbcoreDCPFeed) End(e gocbcore.DcpStreamEnd, err error) {
	atomic.AddUint64(&f.dcpStats.TotDCPStreamEnds, 1)

	var sid int16
	if f.streamOptions.StreamOptions != nil {
		sid = int16(f.streamOptions.StreamOptions.StreamID)
	}
	updateDCPAgentsDetails(f.bucketName, f.bucketUUID, f.name, f.agent, sid, false)

	lastReceivedSeqno := f.lastReceivedSeqno[e.VbID]
	if err == nil {
		f.complete(e.VbID)
		log.Printf("feed_dcp_gocbcore: [%s] DCP stream [%v] ended for vb: %v,"+
			" last seq: %v", f.Name(), e.StreamID, e.VbID, lastReceivedSeqno)
	} else if errors.Is(err, gocbcore.ErrShutdown) ||
		errors.Is(err, gocbcore.ErrSocketClosed) ||
		errors.Is(err, gocbcore.ErrDCPStreamFilterEmpty) {
		f.complete(e.VbID)
		f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s], End, vb: %v, err: %v",
			f.Name(), e.VbID, err))
	} else if errors.Is(err, gocbcore.ErrDCPStreamStateChanged) ||
		errors.Is(err, gocbcore.ErrDCPStreamTooSlow) ||
		errors.Is(err, gocbcore.ErrDCPStreamDisconnected) ||
		errors.Is(err, gocbcore.ErrForcedReconnect) {
		log.Printf("feed_dcp_gocbcore: [%s] DCP stream [%v] for vb: %v, closed due to"+
			" `%s`, last seq: %v, reconnecting ...",
			f.Name(), e.StreamID, e.VbID, err.Error(), lastReceivedSeqno)
		go func(vb uint16) {
			vbuuid, lastSeq, _ := f.lastVbUUIDSeqFromFailOverLog(vb)
			f.initiateStreamEx(vb, false, gocbcore.VbUUID(vbuuid),
				gocbcore.SeqNo(lastSeq), maxEndSeqno)
		}(e.VbID)
	} else if errors.Is(err, gocbcore.ErrDCPStreamClosed) {
		f.complete(e.VbID)
		log.Debugf("feed_dcp_gocbcore: [%s] DCP stream [%v] for vb: %v,"+
			" closed by consumer", f.Name(), e.StreamID, e.VbID)
	} else {
		f.complete(e.VbID)
		log.Warnf("feed_dcp_gocbcore: [%s] DCP stream [%v] closed for vb: %v,"+
			" last seq: %v, err: `%s`",
			f.Name(), e.StreamID, e.VbID, lastReceivedSeqno, err.Error())
	}
}

func (f *GocbcoreDCPFeed) CreateCollection(c gocbcore.DcpCollectionCreation) {
	if err := f.checkAndUpdateVBucketState(c.VbID); err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] CreateCollection, %v", c.VbID, c.StreamID, err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, c.VbID, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destColl, ok := dest.(DestCollection); ok {
			// A CreateCollection message for a collection is received only
			// if the feed has subscribed to the collection, so update seqno
			// received for the feed.
			err = destColl.CreateCollection(partition, c.ManifestUID, c.ScopeID,
				c.CollectionID, c.SeqNo)
		}

		if err != nil {
			return fmt.Errorf("name: %s, partition: %s,"+
				" seq: %d, err: %v", f.Name(), partition, c.SeqNo, err)
		}

		f.updateStopAfter(partition, c.SeqNo)

		return nil
	}, f.stats.TimerCreateCollection)

	if err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] CreateCollection, err: %v", c.VbID, c.StreamID, err))
		return
	}

	f.lastReceivedSeqno[c.VbID] = c.SeqNo

	atomic.AddUint64(&f.dcpStats.TotDCPCreateCollections, 1)
}

func (f *GocbcoreDCPFeed) DeleteCollection(d gocbcore.DcpCollectionDeletion) {
	// initiate a feed closure on collection delete
	f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s] DeleteCollection,"+
		" vb: %v, stream: %v, collection uid: %d",
		f.Name(), d.VbID, d.StreamID, d.CollectionID))
}

func (f *GocbcoreDCPFeed) FlushCollection(fl gocbcore.DcpCollectionFlush) {
	// FIXME: not supported
}

func (f *GocbcoreDCPFeed) CreateScope(c gocbcore.DcpScopeCreation) {
	// Don't expect to see a CreateScope message as indexes CANNOT surpass scopes
}

func (f *GocbcoreDCPFeed) DeleteScope(d gocbcore.DcpScopeDeletion) {
	// initiate a feed closure on scope delete
	f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s] DeleteScope,"+
		" vb: %v, stream: %v, scope uid: %d",
		f.Name(), d.VbID, d.StreamID, d.ScopeID))
}

func (f *GocbcoreDCPFeed) ModifyCollection(m gocbcore.DcpCollectionModification) {
	// FIXME: not supported
}

func (f *GocbcoreDCPFeed) OSOSnapshot(o gocbcore.DcpOSOSnapshot) {
	if err := f.checkAndUpdateVBucketState(o.VbID); err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] OSOSnapshot, %v", o.VbID, o.StreamID, err))
		return
	}

	partition, dest, err :=
		VBucketIdToPartitionDest(f.pf, f.dests, o.VbID, nil)
	if err == nil && !f.checkStopAfter(partition) {
		if destColl, ok := dest.(DestCollection); ok {
			err = destColl.OSOSnapshot(partition, o.SnapshotType)
		}
	}

	if err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] OSOSnapshot, err: %v", o.VbID, o.StreamID, err))
		return
	}
}

func (f *GocbcoreDCPFeed) SeqNoAdvanced(s gocbcore.DcpSeqNoAdvanced) {
	if err := f.checkAndUpdateVBucketState(s.VbID); err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] SeqNoAdvanced, %v", s.VbID, s.StreamID, err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, s.VbID, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destColl, ok := dest.(DestCollection); ok {
			// A SeqNoAdvanced message is received when the feed has subscribed
			// to collection(s), and it is to be interpreted as a SnapshotEnd
			// message, indicating that the feed should not expect any more
			// sequence numbers up until this.
			err = destColl.SeqNoAdvanced(partition, s.SeqNo)
		}

		if err != nil {
			return fmt.Errorf("name: %s, partition: %s,"+
				" seq: %d, err: %v", f.Name(), partition, s.SeqNo, err)
		}

		f.updateStopAfter(partition, s.SeqNo)

		return nil
	}, f.stats.TimerSeqNoAdvanced)

	if err != nil {
		f.onError(true,
			fmt.Errorf("[vb:%v stream:%v] SeqNoAdvanced, err: %v", s.VbID, s.StreamID, err))
		return
	}

	f.lastReceivedSeqno[s.VbID] = s.SeqNo

	atomic.AddUint64(&f.dcpStats.TotDCPSeqNoAdvanceds, 1)
}
