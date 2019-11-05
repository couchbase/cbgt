//  Copyright (c) 2018 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"
	"gopkg.in/couchbase/gocb.v1"
	"gopkg.in/couchbase/gocbcore.v7"
)

const source_gocb = "gocb"

// DEST_EXTRAS_TYPE_GOCB_DCP represents gocb DCP mutation/deletion metadata
// not included in DataUpdate/DataDelete (GocbDCPExtras).
const DEST_EXTRAS_TYPE_GOCB_DCP = DestExtrasType(0x0004)

// GocbDCPExtras packages additional DCP mutation metadata for use by
// DataUpdateEx, DataDeleteEx.
type GocbDCPExtras struct {
	Expiry   uint32
	Flags    uint32
	Datatype uint8
	Value    []byte // carries xattr information (if available) for DataDeleteEx
}

var max_end_seqno = gocbcore.SeqNo(0xffffffffffffffff)

var StatsTimeout = time.Duration(30 * time.Second)
var OpenStreamTimeout = time.Duration(60 * time.Second)

func init() {
	RegisterFeedType(source_gocb, &FeedType{
		Start:           StartGocbDCPFeed,
		Partitions:      CBPartitions,
		PartitionSeqs:   CBPartitionSeqs,
		Stats:           CBStats,
		PartitionLookUp: CBVBucketLookUp,
		Public:          true,
		Description: "general/" + source_gocb +
			" - a Couchbase Server bucket will be the data source",
		StartSample: NewDCPFeedParams(),
	})
}

func StartGocbDCPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]Dest) error {
	server, _, bucketName :=
		CouchbaseParseSourceName(mgr.server, "default", sourceName)

	feed, err := NewGocbDCPFeed(feedName, indexName, server,
		bucketName, bucketUUID, params, BasicPartitionFunc, dests,
		mgr.tagsMap != nil && !mgr.tagsMap["feed"], mgr)
	if err != nil {
		return fmt.Errorf("feed_dcp_gocb:"+
			" could not prepare DCP feed, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_dcp_gocb:"+
			" could not start, server: %s, err: %v",
			mgr.server, err)
	}
	err = mgr.registerFeed(feed)
	if err != nil {
		feed.Close()
		return err
	}
	return nil
}

// A GocbDCPFeed implements both Feed and gocb.StreamObserver
// interfaces, and forwards any incoming gocb.StreamObserver
// callbacks to the relevant, hooked-up Dest instances.
//
// url: single URL or multiple URLs delimited by ';'
type GocbDCPFeed struct {
	name       string
	indexName  string
	url        string
	bucketName string
	bucketUUID string
	paramsStr  string
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	stopAfter  map[string]UUIDSeq
	agent      *gocbcore.Agent
	mgr        *Manager

	seqnoM            sync.Mutex
	lastReceivedSeqno map[uint16]uint64

	m                sync.Mutex
	remaining        sync.WaitGroup
	active           map[uint16]bool
	closed           bool
	lastErr          error
	stats            *DestStats
	stopAfterReached map[string]bool // May be nil.

	closeCh chan struct{}
}

func NewGocbDCPFeed(name, indexName, url,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*GocbDCPFeed, error) {
	log.Printf("feed_dcp_gocb: NewGocbDCPFeed, name: %s, indexName: %s",
		name, indexName)

	var optionsMgr map[string]string
	if mgr != nil {
		optionsMgr = mgr.Options()
	}

	auth, err := gocbAuth(paramsStr, optionsMgr)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocb: NewGocbDCPFeed gocbAuth, err: %v", err)
	}

	var stopAfter map[string]UUIDSeq

	params := NewDCPFeedParams()

	if paramsStr != "" {
		err := json.Unmarshal([]byte(paramsStr), params)
		if err != nil {
			return nil, err
		}

		stopAfterSourceParams := StopAfterSourceParams{}
		err = json.Unmarshal([]byte(paramsStr), &stopAfterSourceParams)
		if err != nil {
			return nil, err
		}

		if stopAfterSourceParams.StopAfter == "markReached" {
			stopAfter = stopAfterSourceParams.MarkPartitionSeqs
		}
	}

	// TODO: Using default settings for flow control; includes parameters:
	// buffer size, buffer ack threshold, noop time interval;
	// Maybe make these configurable?

	vbucketIds, err := ParsePartitionsToVBucketIds(dests)
	if err != nil {
		return nil, err
	}
	if len(vbucketIds) == 0 {
		return nil, fmt.Errorf("feed_dcp_gocb: No vbucketids for this feed")
	}

	feed := &GocbDCPFeed{
		name:              name,
		indexName:         indexName,
		url:               url,
		bucketName:        bucketName,
		bucketUUID:        bucketUUID,
		paramsStr:         paramsStr,
		params:            params,
		pf:                pf,
		dests:             dests,
		disable:           disable,
		stopAfter:         stopAfter,
		mgr:               mgr,
		lastReceivedSeqno: make(map[uint16]uint64),
		stats:             NewDestStats(),
		active:            make(map[uint16]bool),
		closeCh:           make(chan struct{}),
	}

	for _, vbid := range vbucketIds {
		feed.lastReceivedSeqno[vbid] = 0
	}

	config := &gocbcore.AgentConfig{
		UserString:           name,
		BucketName:           bucketName,
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		Auth:                 auth,
	}

	urls := strings.Split(url, ";")
	if len(urls) <= 0 {
		return nil, fmt.Errorf("feed_dcp_gocb: NewGocbDCPFeed, no urls provided")
	}

	err = config.FromConnStr(urls[0])
	if err != nil {
		return nil, err
	}

	flags := gocbcore.DcpOpenFlagProducer | gocbcore.DcpOpenFlagIncludeXattrs

	feed.agent, err = gocbcore.CreateDcpAgent(config, name, flags)
	if err != nil {
		return nil, err
	}

	return feed, nil
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) Name() string {
	return f.name
}

func (f *GocbDCPFeed) IndexName() string {
	return f.indexName
}

func (f *GocbDCPFeed) Start() error {
	if f.disable {
		log.Printf("feed_dcp_gocb: disabled, name: %s", f.Name())
		return nil
	}

	log.Printf("feed_dcp_gocb: start, name: %s", f.Name())

	for vbid := range f.lastReceivedSeqno {
		err := f.initiateStream(uint16(vbid), true)
		if err != nil {
			return fmt.Errorf("feed_dcp_gocb: start, name: %s, vbid: %v, err: %v",
				f.Name(), vbid, err)
		}
	}

	return nil
}

func (f *GocbDCPFeed) Close() error {
	f.m.Lock()
	if f.closed {
		f.m.Unlock()
		return nil
	}
	f.agent.Close()
	f.closed = true
	f.forceCompleteLOCKED()
	f.m.Unlock()

	close(f.closeCh)
	f.wait()

	log.Printf("feed_dcp_gocb: close, name: %s", f.Name())
	return nil
}

func (f *GocbDCPFeed) Dests() map[string]Dest {
	return f.dests
}

var prefixAgentDCPStats = []byte(`{"agentDCPStats":`)

func (f *GocbDCPFeed) Stats(w io.Writer) error {
	signal := make(chan error, 1)
	op, err := f.agent.StatsEx(gocbcore.StatsOptions{Key: "dcp"},
		func(resp *gocbcore.StatsResult, er error) {
			if resp != nil {
				stats := resp.Servers
				w.Write(prefixAgentDCPStats)
				json.NewEncoder(w).Encode(stats)

				w.Write(prefixDestStats)
				f.stats.WriteJSON(w)

				w.Write(JsonCloseBrace)
			}

			select {
			case <-f.closeCh:
			case signal <- er:
			}
		})

	if err != nil {
		return err
	}

	timeoutTmr := gocbcore.AcquireTimer(StatsTimeout)
	select {
	case err := <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return err
	case <-f.closeCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return gocb.ErrStreamDisconnected
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if op != nil && !op.Cancel() {
			select {
			case err = <-signal:
			case <-f.closeCh:
				err = gocb.ErrStreamDisconnected
			}
			return err
		}
		return gocb.ErrTimeout
	}
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) initiateStream(vbId uint16, isNewStream bool) error {
	metadataBytes, lastSeq, err := f.getMetaData(vbId)
	if err != nil {
		return err
	}

	var vbuuid uint64
	if metadataBytes != nil {
		var metadata metaData
		err = json.Unmarshal(metadataBytes, &metadata)
		if err != nil {
			return err
		}

		if len(metadata.FailOverLog) > 0 {
			vbuuid = metadata.FailOverLog[0][0]
		}
	}

	return f.initiateStreamEx(vbId, isNewStream, gocbcore.VbUuid(vbuuid),
		gocbcore.SeqNo(lastSeq), max_end_seqno)
}

func (f *GocbDCPFeed) initiateStreamEx(vbId uint16, isNewStream bool,
	vbuuid gocbcore.VbUuid, seqStart, seqEnd gocbcore.SeqNo) error {
	if isNewStream {
		f.m.Lock()
		if !f.active[vbId] {
			f.remaining.Add(1)
			f.active[vbId] = true
		}
		f.m.Unlock()
	}

	snapStart := seqStart

	signal := make(chan bool, 1)
	log.Debugf("feed_dcp_gocb: Initiating DCP stream request for vb: %v,"+
		" vbUUID: %v, seqStart: %v", vbId, vbuuid, seqStart)
	op, err := f.agent.OpenStream(vbId, gocbcore.DcpStreamAddFlagStrictVBUUID,
		vbuuid, seqStart, seqEnd, snapStart, snapStart, f,
		func(entries []gocbcore.FailoverEntry, er error) {
			if er == gocb.ErrShutdown {
				log.Printf("feed_dcp_gocb: DCP stream for vb: %v was shutdown", vbId)
				f.complete(vbId)
			} else if er == gocb.ErrNetwork {
				// TODO: Add a maximum retry-count here maybe?
				log.Printf("feed_dcp_gocb: Network error received on DCP stream for"+
					" vb: %v", vbId)
				f.initiateStreamEx(vbId, false, vbuuid, seqStart, seqEnd)
			} else if er == gocb.ErrRollback {
				log.Printf("feed_dcp_gocb: Received rollback, for vb: %v,"+
					" seqno requested: %v", vbId, seqStart)
				f.complete(vbId)
				go f.rollback(vbId, entries)
			} else if er != nil {
				log.Printf("feed_dcp_gocb: Received error on DCP stream for vb: %v,"+
					" err: %v", vbId, er)
				f.complete(vbId)
			}

			select {
			case signal <- true:
			case <-f.closeCh:
			}
		})
	if err != nil && err != gocb.ErrShutdown {
		log.Warnf("feed_dcp_gocb: DCP stream closed for vbID: %v, due to client"+
			" error: `%s`", vbId, err)
	}

	go func() {
		timeoutTmr := gocbcore.AcquireTimer(OpenStreamTimeout)
		select {
		case <-f.closeCh:
			gocbcore.ReleaseTimer(timeoutTmr, false)
			return
		case <-signal:
			gocbcore.ReleaseTimer(timeoutTmr, false)
			return
		case <-timeoutTmr.C:
			gocbcore.ReleaseTimer(timeoutTmr, true)
			if op != nil && !op.Cancel() {
				select {
				case <-signal:
				case <-f.closeCh:
				}
				return
			}

			// TODO: On stream request timeout, configure a maximum number
			// of retry attempts perhaps?
			f.complete(vbId)
			return
		}
	}()

	return err
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, snapshotType gocbcore.SnapshotState) {
	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if f.stopAfter != nil {
			uuidSeq, exists := f.stopAfter[partition]
			if exists && endSeqNo > uuidSeq.Seq { // TODO: Check UUID.
				// Clamp the snapEnd so batches are executed.
				endSeqNo = uuidSeq.Seq
			}
		}

		return dest.SnapshotStart(partition, startSeqNo, endSeqNo)
	}, f.stats.TimerSnapshotStart)

	if err != nil {
		log.Warnf("feed_dcp_gocb: Error in accepting a DCP snapshot marker,"+
			" vbId: %v, err: %v", vbId, err)
		return
	}

	// Get Latest failover log
	var failoverLog [][]uint64
	f.agent.GetFailoverLog(vbId, func(entries []gocbcore.FailoverEntry, er error) {
		if er != nil {
			err = er
			return
		}

		failoverLog = make([][]uint64, len(entries))
		// Entries are ordered from highest seqno to lower
		for i := 0; i < len(entries); i++ {
			uuid := uint64(entries[i].VbUuid)
			seqno := uint64(entries[i].SeqNo)
			failoverLog[i] = []uint64{uuid, seqno}
		}
	})
	if err != nil {
		log.Warnf("feed_dcp_gocb: Error in fetching failover log,"+
			" vbId: %v, err: %v", vbId, err)
		return
	}

	// TODO: Ensure that this is the right place to store the
	// internal metadata, or does this need to be done after
	// the first mutation of this snapshot has been received.
	//
	// Set internal metadata with info from the snapshot marker
	f.setMetaData(vbId, &metaData{
		SeqStart:    0,
		SeqEnd:      0,
		SnapStart:   startSeqNo,
		SnapEnd:     endSeqNo,
		FailOverLog: failoverLog,
	})
}

func (f *GocbDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	key, value []byte) {
	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			extras := GocbDCPExtras{
				Expiry:   expiry,
				Flags:    flags,
				Datatype: datatype,
			}
			err = destEx.DataUpdateEx(partition, key, seqNo, value, cas,
				DEST_EXTRAS_TYPE_GOCB_DCP, extras)
		} else {
			err = dest.DataUpdate(partition, key, seqNo, value, cas, 0, nil)
		}

		if err != nil {
			return fmt.Errorf("Mutation => name: %s, partition: %s,"+
				" key: %v, seq: %d, err: %v",
				f.name, partition, log.Tag(log.UserData, key), seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerDataUpdate)

	if err != nil {
		log.Warnf("feed_dcp_gocb: Error in accepting a DCP mutation,"+
			" vbId: %v, err: %v", vbId, err)
	} else {
		f.updateLastReceivedSeqno(vbId, seqNo)
	}
}

func (f *GocbDCPFeed) Deletion(seqNo, revNo, cas uint64, datatype uint8,
	vbId uint16, key, value []byte) {
	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			extras := GocbDCPExtras{
				Datatype: datatype,
				Value:    value,
			}
			err = destEx.DataDeleteEx(partition, key, seqNo, cas,
				DEST_EXTRAS_TYPE_GOCB_DCP, extras)
		} else {
			err = dest.DataDelete(partition, key, seqNo, cas, 0, nil)
		}

		if err != nil {
			return fmt.Errorf("Deletion => name: %s, partition: %s,"+
				"key: %v, seq: %d, err: %v",
				f.name, partition, log.Tag(log.UserData, key), seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerDataDelete)

	if err != nil {
		log.Warnf("feed_dcp_gocb: Error in accepting a DCP deletion,"+
			" vbId: %v, err: %v", vbId, err)
	} else {
		f.updateLastReceivedSeqno(vbId, seqNo)
	}
}

func (f *GocbDCPFeed) Expiration(seqNo, revNo, cas uint64, vbId uint16,
	key []byte) {
	f.Deletion(seqNo, revNo, cas, 0, vbId, key, nil)
}

func (f *GocbDCPFeed) End(vbId uint16, err error) {
	lastReceivedSeqno := f.fetchLastReceivedSeqno(vbId)
	if err == nil {
		log.Printf("feed_dcp_gocb: DCP stream ended for vb: %v, last seq: %v",
			vbId, lastReceivedSeqno)
		f.complete(vbId)
	} else if err == gocb.ErrStreamStateChanged || err == gocb.ErrStreamTooSlow ||
		err == gocb.ErrStreamDisconnected {
		log.Printf("feed_dcp_gocb: DCP stream for vb: %v, closed due to"+
			" `%s`, will reconnect", vbId, err.Error())
		f.initiateStreamEx(vbId, false, gocbcore.VbUuid(0),
			gocbcore.SeqNo(lastReceivedSeqno), max_end_seqno)
	} else if err == gocb.ErrStreamClosed {
		log.Printf("feed_dcp_gocb: DCP stream for vb: %v, closed by consumer", vbId)
		f.complete(vbId)
	} else if err == gocb.ErrNetwork {
		// TODO: Add a maximum retry-count here maybe?
		log.Printf("feed_dcp_gocb: Network error received on DCP stream for vb: %v",
			vbId)
		f.initiateStreamEx(vbId, false, gocbcore.VbUuid(0),
			gocbcore.SeqNo(lastReceivedSeqno), max_end_seqno)
	} else {
		log.Printf("feed_dcp_gocb: DCP stream closed for vb: %v, last seq: %v,"+
			" err: `%s`", vbId, lastReceivedSeqno, err)
		f.complete(vbId)
	}
}

// ----------------------------------------------------------------

// This struct is to remain AS IS, it follows the same format
// used in go-couchbase/cbdatasource (lookup: VBucketMetaData).
type metaData struct {
	SeqStart    uint64     `json:"seqStart"`
	SeqEnd      uint64     `json:"seqEnd"`
	SnapStart   uint64     `json:"snapStart"`
	SnapEnd     uint64     `json:"snapEnd"`
	FailOverLog [][]uint64 `json:"failOverLog"`
}

func (f *GocbDCPFeed) setMetaData(vbId uint16, m *metaData) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		value, err := json.Marshal(m)
		if err != nil {
			return err
		}

		return dest.OpaqueSet(partition, value)
	}, f.stats.TimerOpaqueSet)
}

func (f *GocbDCPFeed) getMetaData(vbId uint16) (value []byte, lastSeq uint64, err error) {
	err = Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		value, lastSeq, err = dest.OpaqueGet(partition)

		return err
	}, f.stats.TimerOpaqueGet)

	return value, lastSeq, err
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) rollback(vbId uint16, entries []gocbcore.FailoverEntry) {
	var rollbackVbuuid uint64
	var rollbackSeqno uint64

	if len(entries) != 0 {
		metadataBytes, _, err := f.getMetaData(vbId)
		if err == nil && metadataBytes != nil {

			var metadata *metaData
			err = json.Unmarshal(metadataBytes, metadata)
			if err == nil && len(metadata.FailOverLog) > 0 {

				rollbackPointDetermined := false
				for i := 0; i < len(entries); i++ {
					for j := 0; j < len(metadata.FailOverLog); j++ {
						if metadata.FailOverLog[j][1] <= uint64(entries[i].SeqNo) {
							rollbackVbuuid = metadata.FailOverLog[j][0]
							rollbackSeqno = metadata.FailOverLog[j][1]
							rollbackPointDetermined = true
							break
						}
					}
					if rollbackPointDetermined {
						break
					}
				}
			}
		}
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			return destEx.RollbackEx(partition, rollbackVbuuid, rollbackSeqno)
		}
		return dest.Rollback(partition, rollbackSeqno)
	}, f.stats.TimerRollback)

	if err != nil {
		// TODO: Better error handling
		log.Warnf("feed_dcp_gocb: Rollback to seqno: %v, vbuuid: %v for vb: %v,"+
			" failed with err: %v", rollbackSeqno, rollbackVbuuid, vbId, err)
	}
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) wait() {
	f.remaining.Wait()
}

func (f *GocbDCPFeed) complete(vbId uint16) {
	f.m.Lock()
	if f.active[vbId] {
		f.active[vbId] = false
		f.remaining.Done()
	}
	f.m.Unlock()
}

func (f *GocbDCPFeed) forceCompleteLOCKED() {
	for i := range f.active {
		if f.active[i] {
			f.active[i] = false
			f.remaining.Done()
		}
	}
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) updateLastReceivedSeqno(vbId uint16, seqNo uint64) {
	f.seqnoM.Lock()
	f.lastReceivedSeqno[vbId] = seqNo
	f.seqnoM.Unlock()
}

func (f *GocbDCPFeed) fetchLastReceivedSeqno(vbId uint16) uint64 {
	f.seqnoM.Lock()
	seqno := f.lastReceivedSeqno[vbId]
	f.seqnoM.Unlock()
	return seqno
}

// checkStopAfter checks to see if we've already reached the
// stopAfterReached state for a partition.
func (f *GocbDCPFeed) checkStopAfter(partition string) bool {
	f.m.Lock()
	reached := f.stopAfterReached != nil && f.stopAfterReached[partition]
	f.m.Unlock()

	return reached
}

// updateStopAfter checks and maintains the stopAfterReached tracking
// maps, which are used for so-called "one-time indexing". Once we've
// reached the stopping point, we close the feed (after all partitions
// have reached their stopAfter sequence numbers).
func (f *GocbDCPFeed) updateStopAfter(partition string, seqNo uint64) {
	if f.stopAfter == nil {
		return
	}

	uuidSeq, exists := f.stopAfter[partition]
	if !exists {
		return
	}

	// TODO: check UUID matches?
	if seqNo >= uuidSeq.Seq {
		f.m.Lock()

		if f.stopAfterReached == nil {
			f.stopAfterReached = map[string]bool{}
		}
		f.stopAfterReached[partition] = true

		allDone := len(f.stopAfterReached) >= len(f.stopAfter)

		f.m.Unlock()

		if allDone {
			go f.Close()
		}
	}
}
