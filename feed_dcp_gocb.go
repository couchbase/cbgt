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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore"
)

const source_gocb = "gocb"

// DEST_EXTRAS_TYPE_GOCB_DCP represents gocb DCP mutation/deletion metadata
// not included in DataUpdate/DataDelete (GocbDCPExtras).
const DEST_EXTRAS_TYPE_GOCB_DCP = DestExtrasType(0x0004)

// DEST_EXTRAS_TYPE_GOCB_SCOPE_COLLECTION represents gocb DCP mutation/deletion
// scope id and collection id written to []byte of len=8 (4bytes each)
const DEST_EXTRAS_TYPE_GOCB_SCOPE_COLLECTION = DestExtrasType(0x0005)

// GocbDCPExtras packages additional DCP mutation metadata for use by
// DataUpdateEx, DataDeleteEx.
type GocbDCPExtras struct {
	ScopeId      uint32
	CollectionId uint32
	Expiry       uint32
	Flags        uint32
	Datatype     uint8
	Value        []byte // carries xattr information (if available) for DataDeleteEx
}

var max_end_seqno = gocbcore.SeqNo(0xffffffffffffffff)

var GocbStatsTimeout = time.Duration(30 * time.Second)
var GocbOpenStreamTimeout = time.Duration(60 * time.Second)
var GocbConnectTimeout = time.Duration(60000 * time.Millisecond)
var GocbServerConnectTimeout = time.Duration(7000 * time.Millisecond)
var GocbNmvRetryDelay = time.Duration(100 * time.Millisecond)

func setupAgentConfig() *gocbcore.AgentConfig {
	return &gocbcore.AgentConfig{
		ConnectTimeout:       GocbConnectTimeout,
		ServerConnectTimeout: GocbServerConnectTimeout,
		NmvRetryDelay:        GocbNmvRetryDelay,
		UseKvErrorMaps:       true,
		UseCollections:       true,
		AuthMechanisms: []gocbcore.AuthMechanism{
			gocbcore.ScramSha512AuthMechanism,
			gocbcore.PlainAuthMechanism,
		},
	}
}

// ----------------------------------------------------------------

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
		return fmt.Errorf("feed_dcp_gocb: StartGocbDCPFeed,"+
			" could not prepare DCP feed, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_dcp_gocb: StartGocbDCPFeed,"+
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

type vbucketState struct {
	snapStart   uint64
	snapEnd     uint64
	failoverLog [][]uint64
	snapSaved   bool // True when snapStart/snapEnd have been persisted
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
	config     *gocbcore.AgentConfig
	agent      *gocbcore.Agent
	mgr        *Manager

	scope       string
	collections []string

	vbucketIds        []uint16
	lastReceivedSeqno []uint64
	currVBs           []*vbucketState
	shutdownVbs       uint32
	closed            uint32 // sync.atomic

	streamFilter *gocbcore.StreamFilter

	m                sync.Mutex
	remaining        sync.WaitGroup
	active           map[uint16]bool
	stats            *DestStats
	stopAfterReached map[string]bool // May be nil.

	closeCh chan struct{}
}

func NewGocbDCPFeed(name, indexName, url,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*GocbDCPFeed, error) {
	var options map[string]string
	if mgr != nil {
		options = mgr.Options()
	}

	auth, err := gocbAuth(paramsStr, options)
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
		return nil, fmt.Errorf("feed_dcp_gocb: NewGocbDCPFeed," +
			" No vbucketids for this feed")
	}

	feed := &GocbDCPFeed{
		name:       name,
		indexName:  indexName,
		url:        url,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		paramsStr:  paramsStr,
		params:     params,
		pf:         pf,
		dests:      dests,
		disable:    disable,
		stopAfter:  stopAfter,
		mgr:        mgr,
		vbucketIds: vbucketIds,
		stats:      NewDestStats(),
		active:     make(map[uint16]bool),
		closeCh:    make(chan struct{}),
	}

	if len(params.Scope) > 0 {
		feed.scope = params.Scope
	} else {
		feed.scope = "_default"
	}

	if len(params.Collections) > 0 {
		feed.collections = params.Collections
	} else {
		feed.collections = []string{"_default"}
	}

	// sort the vbucketIds list to determine the largest vbucketId
	sort.Slice(vbucketIds, func(i, j int) bool { return vbucketIds[i] < vbucketIds[j] })
	largestVBId := vbucketIds[len(vbucketIds)-1]
	feed.lastReceivedSeqno = make([]uint64, largestVBId+1)

	feed.currVBs = make([]*vbucketState, largestVBId+1)
	for _, vbid := range vbucketIds {
		feed.currVBs[vbid] = &vbucketState{}
	}

	config := setupAgentConfig()
	config.UserString = name
	config.BucketName = bucketName
	config.Auth = auth

	urls := strings.Split(url, ";")
	if len(urls) <= 0 {
		return nil, fmt.Errorf("feed_dcp_gocb: NewGocbDCPFeed, no urls provided")
	}

	err = config.FromConnStr(urls[0])
	if err != nil {
		return nil, err
	}

	tlsConfig, err := FetchSecuritySetting(options)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocb: NewGocbDCPFeed,"+
			" error in fetching tlsConfig, err: %v", err)
	}
	config.TlsConfig = tlsConfig

	feed.config = config

	flags := gocbcore.DcpOpenFlagProducer

	if params.IncludeXAttrs {
		flags |= gocbcore.DcpOpenFlagIncludeXattrs
	}

	if params.NoValue {
		flags |= gocbcore.DcpOpenFlagNoValue
	}

	dcpConnName := fmt.Sprintf("%s%s-%x", DCPFeedPrefix, name, rand.Int31())
	feed.agent, err = gocbcore.CreateDcpAgent(config, dcpConnName, flags)
	if err != nil {
		return nil, err
	}

	log.Printf("feed_dcp_gocb: NewGocbDCPFeed, name: %s, indexName: %s,"+
		" server: %v, connection name: %s",
		name, indexName, urls[0], dcpConnName)

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
		log.Printf("feed_dcp_gocb: Start, DISABLED, name: %s", f.Name())
		return nil
	}

	f.streamFilter = &gocbcore.StreamFilter{}

	signal := make(chan error, 1)
	for _, coll := range f.collections {
		op, err := f.agent.GetCollectionID(f.scope, coll,
			gocbcore.GetCollectionIDOptions{},
			func(manifestID uint64, collectionID uint32, er error) {
				// FIXME sdk to support fetching scope ID as well
				// FIXME potential race in fetching ManifetUid/collection ids here?
				if er == nil {
					f.streamFilter.ManifestUid = manifestID
					f.streamFilter.Collections =
						append(f.streamFilter.Collections, collectionID)
				}

				select {
				case <-f.closeCh:
				case signal <- er:
				}
			})
		if err != nil {
			return fmt.Errorf("feed_dcp_gocb: Start, GetCollectionID,"+
				" collection: %v, err: %v", coll, err)
		}

		timeoutTmr := gocbcore.AcquireTimer(GocbStatsTimeout)
		select {
		case err := <-signal:
			gocbcore.ReleaseTimer(timeoutTmr, false)
			if err != nil {
				return err
			}
		case <-f.closeCh:
			gocbcore.ReleaseTimer(timeoutTmr, false)
			return gocbcore.ErrStreamDisconnected
		case <-timeoutTmr.C:
			gocbcore.ReleaseTimer(timeoutTmr, true)
			if op != nil && !op.Cancel() {
				select {
				case err = <-signal:
				case <-f.closeCh:
					err = gocbcore.ErrStreamDisconnected
				}
				return err
			}
			return gocbcore.ErrTimeout
		}
	}

	log.Printf("feed_dcp_gocb: Start, name: %s, num streams: %d,"+
		" streamFilter: %+v", f.Name(), len(f.vbucketIds), f.streamFilter)

	for _, vbid := range f.vbucketIds {
		err := f.initiateStream(uint16(vbid))
		if err != nil {
			return fmt.Errorf("feed_dcp_gocb: Start, name: %s, vbid: %v, err: %v",
				f.Name(), vbid, err)
		}
	}

	return nil
}

func (f *GocbDCPFeed) Close() error {
	if !atomic.CompareAndSwapUint32(&f.closed, 0, 1) {
		return nil
	}

	if f.mgr != nil {
		f.mgr.unregisterFeed(f.Name())
	}

	f.m.Lock()
	f.agent.Close()
	f.forceCompleteLOCKED()
	f.m.Unlock()

	close(f.closeCh)
	f.wait()

	log.Printf("feed_dcp_gocb: Close, name: %s", f.Name())
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

	timeoutTmr := gocbcore.AcquireTimer(GocbStatsTimeout)
	select {
	case <-f.closeCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return gocbcore.ErrStreamDisconnected
	case err = <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return err
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if op != nil && !op.Cancel() {
			select {
			case err = <-signal:
			case <-f.closeCh:
				err = gocbcore.ErrStreamDisconnected
			}
			return err
		}
		return gocbcore.ErrTimeout
	}
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) initiateStream(vbId uint16) error {
	vbMetaData, lastSeq, err := f.getMetaData(vbId)
	if err != nil {
		return err
	}

	var vbuuid uint64
	if len(vbMetaData.FailOverLog) > 0 {
		vbuuid = vbMetaData.FailOverLog[0][0]
	}

	return f.initiateStreamEx(vbId, true, gocbcore.VbUuid(vbuuid),
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
	signal := make(chan error, 1)
	log.Debugf("feed_dcp_gocb: Initiating DCP stream request for vb: %v,"+
		" vbUUID: %v, seqStart: %v, seqEnd: %v, streamFilter: %+v",
		vbId, vbuuid, seqStart, seqEnd, f.streamFilter)
	op, err := f.agent.OpenStream(vbId, gocbcore.DcpStreamAddFlagStrictVBUUID,
		vbuuid, seqStart, seqEnd, snapStart, snapStart, f, f.streamFilter,
		func(entries []gocbcore.FailoverEntry, er error) {
			if er == gocbcore.ErrShutdown {
				log.Printf("feed_dcp_gocb: DCP stream for vb: %v was shutdown", vbId)
				f.complete(vbId)
			} else if er == gocbcore.ErrNetwork {
				// TODO: Add a maximum retry-count here maybe?
				log.Warnf("feed_dcp_gocb: Network error received on DCP stream for"+
					" vb: %v", vbId)
				f.initiateStreamEx(vbId, false, vbuuid, seqStart, seqEnd)
			} else if er == gocbcore.ErrRollback {
				log.Printf("feed_dcp_gocb: Received rollback, for vb: %v,"+
					" seqno requested: %v", vbId, seqStart)
				f.complete(vbId)
				go f.rollback(vbId, entries)
			} else if er != nil {
				log.Warnf("feed_dcp_gocb: Received error on DCP stream for vb: %v,"+
					" err: %v", vbId, er)
				f.complete(vbId)
			} else {
				// er == nil
				failoverLog := make([][]uint64, len(entries))
				for i := 0; i < len(entries); i++ {
					failoverLog[i] = []uint64{
						uint64(entries[i].VbUuid),
						uint64(entries[i].SeqNo),
					}
				}

				f.currVBs[vbId].failoverLog = failoverLog

				v, _, err := f.getMetaData(vbId)
				if err == nil {
					v.FailOverLog = failoverLog
					err = f.setMetaData(vbId, v)
				}

				if err != nil {
					er = fmt.Errorf("error in fetching/setting metadata"+
						" for vb: %d, err: %v", vbId, err)
				}
			}

			select {
			case <-f.closeCh:
			case signal <- er:
			}
		})

	if err != nil && err != gocbcore.ErrShutdown {
		log.Warnf("feed_dcp_gocb: DCP stream closed for vbID: %v, due to client"+
			" error: `%s`", vbId, err)
		return err
	}

	timeoutTmr := gocbcore.AcquireTimer(GocbOpenStreamTimeout)
	select {
	case <-f.closeCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return gocbcore.ErrStreamDisconnected
	case err = <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return err
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if op != nil && !op.Cancel() {
			select {
			case err = <-signal:
			case <-f.closeCh:
				err = gocbcore.ErrStreamDisconnected
			}
			return err
		}

		// TODO: On stream request timeout, configure a maximum number
		// of retry attempts perhaps?
		f.complete(vbId)
		return gocbcore.ErrTimeout
	}
}

// ----------------------------------------------------------------

// onError is to be invoked in case of errors encountered while
// processing DCP messages.
func (f *GocbDCPFeed) onError(isShutdown bool, err error) {
	log.Warnf("feed_dcp_gocb: onError, name: %s,"+
		" bucketName: %s, bucketUUID: %s, err: %v",
		f.name, f.bucketName, f.bucketUUID, err)

	if atomic.LoadUint32(&f.closed) == 0 {
		go f.Close()
	}

	if isShutdown && f.mgr != nil && f.mgr.meh != nil {
		go f.mgr.meh.OnFeedError("couchbase", f, err)
	}
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, streamId uint16, snapshotType gocbcore.SnapshotState) {
	// FIXME Handle streamId
	if f.currVBs[vbId] == nil {
		f.onError(false, fmt.Errorf("SnapshotMarker, invalid vb: %d", vbId))
		return
	}

	f.currVBs[vbId].snapStart = startSeqNo
	f.currVBs[vbId].snapEnd = endSeqNo
	f.currVBs[vbId].snapSaved = false

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
		f.onError(false, fmt.Errorf("SnapshotMarker, vb: %d, err: %v", vbId, err))
	}
}

func (f *GocbDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	collectionId uint32, streamId uint16, key, value []byte) {
	// FIXME To obtain scopeId as well: GOCBC-684
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(false, fmt.Errorf("Mutation, %v", err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			extras := GocbDCPExtras{
				ScopeId:      0, // FIXME GOCBC-684
				CollectionId: collectionId,
				Expiry:       expiry,
				Flags:        flags,
				Datatype:     datatype,
			}
			err = destEx.DataUpdateEx(partition, key, seqNo, value, cas,
				DEST_EXTRAS_TYPE_GOCB_DCP, extras)
		} else {
			extras := make([]byte, 8)                    // 8 bytes needed to hold 2 uint32s
			binary.LittleEndian.PutUint32(extras[0:], 0) // FIXME GOCBC-684
			binary.LittleEndian.PutUint32(extras[4:], collectionId)
			err = dest.DataUpdate(partition, key, seqNo, value, cas,
				DEST_EXTRAS_TYPE_GOCB_SCOPE_COLLECTION, extras)
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
		f.onError(false, fmt.Errorf("Mutation, vb: %d, err: %v", vbId, err))
	} else {
		f.lastReceivedSeqno[vbId] = seqNo
	}
}

func (f *GocbDCPFeed) Deletion(seqNo, revNo, cas uint64, datatype uint8,
	vbId uint16, collectionId uint32, streamId uint16, key, value []byte) {
	// FIXME To obtain scopeId as well: GOCBC-684
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(false, fmt.Errorf("Deletion, %v", err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			extras := GocbDCPExtras{
				ScopeId:      0, // FIXME GOCBC-684
				CollectionId: collectionId,
				Datatype:     datatype,
				Value:        value,
			}
			err = destEx.DataDeleteEx(partition, key, seqNo, cas,
				DEST_EXTRAS_TYPE_GOCB_DCP, extras)
		} else {
			extras := make([]byte, 8)                    // 8 bytes needed to hold 2 uint32s
			binary.LittleEndian.PutUint32(extras[0:], 0) // FIXME GOCBC-684
			binary.LittleEndian.PutUint32(extras[4:], collectionId)
			err = dest.DataDelete(partition, key, seqNo, cas,
				DEST_EXTRAS_TYPE_GOCB_SCOPE_COLLECTION, extras)
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
		f.onError(false, fmt.Errorf("Deletion, vb: %d, err: %v", vbId, err))
	} else {
		f.lastReceivedSeqno[vbId] = seqNo
	}
}

func (f *GocbDCPFeed) Expiration(seqNo, revNo, cas uint64, vbId uint16,
	collectionId uint32, streamId uint16, key []byte) {
	f.Deletion(seqNo, revNo, cas, 0, vbId, collectionId, streamId, key, nil)
}

func (f *GocbDCPFeed) End(vbId uint16, streamId uint16, err error) {
	// FIXME Handle streamId
	lastReceivedSeqno := f.lastReceivedSeqno[vbId]
	if err == nil {
		log.Printf("feed_dcp_gocb: DCP stream ended for vb: %v, last seq: %v",
			vbId, lastReceivedSeqno)
		f.complete(vbId)
	} else if err == gocbcore.ErrShutdown || err == gocbcore.ErrNetwork {
		// count the number of Shutdowns received
		if atomic.AddUint32(&f.shutdownVbs, 1) >= uint32(len(f.vbucketIds)) {
			// initiate a feed closure
			f.onError(true, err)
		}
	} else if err == gocbcore.ErrStreamStateChanged || err == gocbcore.ErrStreamTooSlow ||
		err == gocbcore.ErrStreamDisconnected {
		log.Printf("feed_dcp_gocb: DCP stream for vb: %v, closed due to"+
			" `%s`, will reconnect", vbId, err.Error())
		f.initiateStreamEx(vbId, false, gocbcore.VbUuid(0),
			gocbcore.SeqNo(lastReceivedSeqno), max_end_seqno)
	} else if err == gocbcore.ErrStreamClosed {
		log.Printf("feed_dcp_gocb: DCP stream for vb: %v, closed by consumer", vbId)
		f.complete(vbId)
	} else {
		log.Printf("feed_dcp_gocb: DCP stream closed for vb: %v, last seq: %v,"+
			" err: `%s`", vbId, lastReceivedSeqno, err)
		f.complete(vbId)
	}
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) CreateCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32,
	ttl uint32, streamId uint16, key []byte) {
	// FIXME
}

func (f *GocbDCPFeed) DeleteCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32,
	streamId uint16) {
	// FIXME
}

func (f *GocbDCPFeed) FlushCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, collectionId uint32) {
	// FIXME
}

func (f *GocbDCPFeed) CreateScope(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, scopeId uint32, streamId uint16, key []byte) {
	// FIXME
}

func (f *GocbDCPFeed) DeleteScope(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, scopeId uint32, streamId uint16) {
	// FIXME
}

func (f *GocbDCPFeed) ModifyCollection(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, collectionId uint32, ttl uint32, streamId uint16) {
	// FIXME
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

func (f *GocbDCPFeed) checkAndUpdateVBucketState(vbId uint16) error {
	if f.currVBs[vbId] == nil {
		return fmt.Errorf("invalid vb: %v", vbId)
	}

	if !f.currVBs[vbId].snapSaved {
		v := &metaData{
			SnapStart:   f.currVBs[vbId].snapStart,
			SnapEnd:     f.currVBs[vbId].snapEnd,
			FailOverLog: f.currVBs[vbId].failoverLog,
		}

		err := f.setMetaData(vbId, v)
		if err != nil {
			return err
		}

		f.currVBs[vbId].snapSaved = true
	}

	return nil
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

func (f *GocbDCPFeed) getMetaData(vbId uint16) (*metaData, uint64, error) {
	vbMetaData := &metaData{}
	var lastSeq uint64
	err := Timer(func() error {
		partition, dest, er :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if er != nil || f.checkStopAfter(partition) {
			return er
		}

		buf, seq, er := dest.OpaqueGet(partition)
		if er != nil {
			return er
		}

		if len(buf) > 0 {
			if er = json.Unmarshal(buf, vbMetaData); er != nil {
				return er
			}
			lastSeq = seq
		}

		return nil
	}, f.stats.TimerOpaqueGet)

	return vbMetaData, lastSeq, err
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) rollback(vbId uint16, entries []gocbcore.FailoverEntry) {
	var rollbackVbuuid uint64
	var rollbackSeqno uint64

	if len(entries) != 0 {
		vbMetaData, _, err := f.getMetaData(vbId)
		if err == nil && len(vbMetaData.FailOverLog) > 0 {
			rollbackPointDetermined := false
			for i := 0; i < len(entries); i++ {
				for j := 0; j < len(vbMetaData.FailOverLog); j++ {
					if vbMetaData.FailOverLog[j][1] <= uint64(entries[i].SeqNo) {
						rollbackVbuuid = vbMetaData.FailOverLog[j][0]
						rollbackSeqno = vbMetaData.FailOverLog[j][1]
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

// ----------------------------------------------------------------

func (f *GocbDCPFeed) VerifyBucketNotExists() (bool, error) {
	agent, err := gocbcore.CreateAgent(f.config)
	if err != nil {
		if err == gocbcore.ErrNoBucket || err == gocbcore.ErrAuthError {
			// bucket not found
			return true, err
		}
		return false, err
	}

	uuid := agent.BucketUUID()
	agent.Close()

	if uuid != f.bucketUUID {
		// bucket UUID mismatched, so the bucket being looked
		// up must've been deleted
		return true, err
	}

	return false, nil
}

func (f *GocbDCPFeed) GetBucketDetails() (string, string) {
	return f.bucketName, f.bucketUUID
}
