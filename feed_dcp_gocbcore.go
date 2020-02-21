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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore"
)

const SOURCE_GOCBCORE = "gocbcore"

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

var max_end_seqno = gocbcore.SeqNo(0xffffffffffffffff)

var GocbcoreStatsTimeout = time.Duration(30 * time.Second)
var GocbcoreOpenStreamTimeout = time.Duration(60 * time.Second)
var GocbcoreConnectTimeout = time.Duration(60000 * time.Millisecond)
var GocbcoreServerConnectTimeout = time.Duration(7000 * time.Millisecond)

// ----------------------------------------------------------------

func setupAgentConfig(name, bucketName string,
	auth gocbcore.AuthProvider) *gocbcore.AgentConfig {
	return &gocbcore.AgentConfig{
		UserAgent:        name,
		BucketName:       bucketName,
		Auth:             auth,
		ConnectTimeout:   GocbcoreConnectTimeout,
		UseCollections:   true,
		DcpAgentPriority: gocbcore.DcpAgentPriorityMed,
	}
}

func waitForResponse(signal <-chan error, closeCh <-chan struct{},
	op gocbcore.PendingOp, timeout time.Duration) error {
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case err := <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return err
	case <-closeCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return gocbcore.ErrDCPStreamDisconnected
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if op != nil {
			op.Cancel(gocbcore.ErrTimeout)
		}
		return gocbcore.ErrTimeout
	}
}

// ----------------------------------------------------------------

func init() {
	RegisterFeedType(SOURCE_GOCBCORE, &FeedType{
		Start:           StartGocbcoreDCPFeed,
		Partitions:      CBPartitions,
		PartitionSeqs:   CBPartitionSeqs,
		Stats:           CBStats,
		PartitionLookUp: CBVBucketLookUp,
		Public:          true,
		Description: "general/" + SOURCE_GOCBCORE +
			" - a Couchbase Server bucket will be the data source",
		StartSample: NewDCPFeedParams(),
	})
}

func StartGocbcoreDCPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]Dest) error {
	server, _, bucketName :=
		CouchbaseParseSourceName(mgr.server, "default", sourceName)

	feed, err := NewGocbcoreDCPFeed(feedName, indexName, server,
		bucketName, bucketUUID, params, BasicPartitionFunc, dests,
		mgr.tagsMap != nil && !mgr.tagsMap["feed"], mgr)
	if err != nil {
		return fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed,"+
			" could not prepare DCP feed, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = mgr.registerFeed(feed)
	if err != nil {
		return feed.onError(false, err)
	}
	err = feed.Start()
	if err != nil {
		return feed.onError(false,
			fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed,"+
				" could not start, server: %s, err: %v",
				mgr.server, err))
	}

	return nil
}

type vbucketState struct {
	snapStart   uint64
	snapEnd     uint64
	failoverLog [][]uint64
	snapSaved   bool // True when snapStart/snapEnd have been persisted
}

// A GocbcoreDCPFeed implements both Feed and gocb.StreamObserver
// interfaces, and forwards any incoming gocb.StreamObserver
// callbacks to the relevant, hooked-up Dest instances.
//
// url: single URL or multiple URLs delimited by ';'
type GocbcoreDCPFeed struct {
	name       string
	indexName  string
	url        string
	bucketName string
	bucketUUID string
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

	scopeID       uint32
	collectionIDs []uint32
	streamFilter  *gocbcore.StreamFilter

	vbucketIds        []uint16
	lastReceivedSeqno []uint64
	currVBs           []*vbucketState
	shutdownVbs       uint32

	dcpStats *gocbcoreDCPFeedStats

	m                sync.Mutex
	remaining        sync.WaitGroup
	closed           bool
	active           map[uint16]bool
	stats            *DestStats
	stopAfterReached map[string]bool // May be nil.

	closeCh chan struct{}
}

type gocbcoreDCPFeedStats struct {
	// TODO: Add more stats
	TotDCPStreamReqs uint64
	TotDCPStreamEnds uint64
	TotDCPRollbacks  uint64

	TotDCPSnapshotMarkers uint64
	TotDCPMutations       uint64
	TotDCPDeletions       uint64
}

// atomicCopyTo copies metrics from s to r (or, from source to
// result), and also applies an optional fn function. The fn is
// invoked with metrics from s and r, and can be used to compute
// additions, subtractions, negatoions, etc. When fn is nil,
// atomicCopyTo behaves as a straight copier.
func (s *gocbcoreDCPFeedStats) atomicCopyTo(r *gocbcoreDCPFeedStats,
	fn func(sv uint64, rv uint64) uint64) {
	// Using reflection rather than a whole slew of explicit
	// invocations of atomic.LoadUint64()/StoreUint64()'s.
	if fn == nil {
		fn = func(sv uint64, rv uint64) uint64 { return sv }
	}
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			rv := atomic.LoadUint64(rvefp.(*uint64))
			sv := atomic.LoadUint64(svefp.(*uint64))
			atomic.StoreUint64(rvefp.(*uint64), fn(sv, rv))
		}
	}
}

func NewGocbcoreDCPFeed(name, indexName, url,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*GocbcoreDCPFeed, error) {
	var options map[string]string
	if mgr != nil {
		options = mgr.Options()
	}

	auth, err := gocbAuth(paramsStr, options)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocbcore: NewGocbcoreDCPFeed gocbAuth, err: %v", err)
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
		return nil, fmt.Errorf("feed_dcp_gocbcore: NewGocbcoreDCPFeed," +
			" No vbucketids for this feed")
	}

	feed := &GocbcoreDCPFeed{
		name:       name,
		indexName:  indexName,
		url:        url,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		params:     params,
		pf:         pf,
		dests:      dests,
		disable:    disable,
		stopAfter:  stopAfter,
		mgr:        mgr,
		vbucketIds: vbucketIds,
		dcpStats:   &gocbcoreDCPFeedStats{},
		stats:      NewDestStats(),
		active:     make(map[uint16]bool),
		closeCh:    make(chan struct{}),
	}

	if len(params.Scope) == 0 && len(params.Collections) == 0 {
		feed.scope = "_default"
		feed.collections = []string{"_default"}
	} else {
		feed.scope = params.Scope
		feed.collections = params.Collections
	}

	// sort the vbucketIds list to determine the largest vbucketId
	sort.Slice(vbucketIds, func(i, j int) bool { return vbucketIds[i] < vbucketIds[j] })
	largestVBId := vbucketIds[len(vbucketIds)-1]
	feed.lastReceivedSeqno = make([]uint64, largestVBId+1)

	feed.currVBs = make([]*vbucketState, largestVBId+1)
	for _, vbid := range vbucketIds {
		feed.currVBs[vbid] = &vbucketState{}
	}

	config := setupAgentConfig(name, bucketName, auth)

	urls := strings.Split(url, ";")
	if len(urls) <= 0 {
		return nil, fmt.Errorf("feed_dcp_gocbcore: NewGocbcoreDCPFeed, no urls provided")
	}

	err = config.FromConnStr(urls[0])
	if err != nil {
		return nil, err
	}

	tlsConfig, err := FetchSecuritySetting(options)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocbcore: NewGocbcoreDCPFeed,"+
			" error in fetching tlsConfig, err: %v", err)
	}
	if tlsConfig != nil {
		config.TLSRootCAs = tlsConfig.RootCAs
	} else {
		config.TLSSkipVerify = true
	}

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

	feed.agent.SetServerConnectTimeout(GocbcoreServerConnectTimeout)

	log.Printf("feed_dcp_gocbcore: NewGocbcoreDCPFeed, name: %s, indexName: %s,"+
		" server: %v, connection name: %s",
		name, indexName, urls[0], dcpConnName)

	return feed, nil
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) Name() string {
	return f.name
}

func (f *GocbcoreDCPFeed) IndexName() string {
	return f.indexName
}

func (f *GocbcoreDCPFeed) Start() error {
	if f.disable {
		log.Printf("feed_dcp_gocbcore: Start, DISABLED, name: %s", f.Name())
		return nil
	}

	signal := make(chan error, 1)

	var manifest gocbcore.Manifest
	op, err := f.agent.GetCollectionManifest(
		gocbcore.GetCollectionManifestOptions{},
		func(manifestBytes []byte, er error) {
			if er == nil {
				er = manifest.UnmarshalJSON(manifestBytes)
			}

			select {
			case <-f.closeCh:
			case signal <- er:
			}
		})

	if err != nil {
		return fmt.Errorf("feed_dcp_gocbcore: Start, GetCollectionManifest,"+
			" err: %v", err)
	}

	err = waitForResponse(signal, f.closeCh, op, GocbcoreStatsTimeout)
	if err != nil {
		return fmt.Errorf("feed_dcp_gocbcore: Start, Failed to get manifest,"+
			"err: %v", err)
	}

	var scopeIDFound bool
	for _, manifestScope := range manifest.Scopes {
		if manifestScope.Name == f.scope {
			f.scopeID = manifestScope.UID
			scopeIDFound = true
			break
		}
	}

	if !scopeIDFound {
		return fmt.Errorf("feed_dcp_gocbcore: Start, scope not found: %v",
			f.scope)
	}

	f.streamFilter = gocbcore.NewStreamFilter()
	f.streamFilter.ManifestUID = manifest.UID

	if len(f.collections) == 0 {
		// if no collections were specified, set up stream requests for
		// the entire scope.
		f.streamFilter.Scope = f.scopeID
	} else {
		for _, coll := range f.collections {
			op, err = f.agent.GetCollectionID(f.scope, coll,
				gocbcore.GetCollectionIDOptions{},
				func(manifestID uint64, collectionID uint32, er error) {
					if er == nil {
						if manifestID != f.streamFilter.ManifestUID {
							er = fmt.Errorf("manifestID mismatch, %v != %v",
								manifestID, f.streamFilter.ManifestUID)
						} else {
							f.collectionIDs =
								append(f.collectionIDs, collectionID)
						}
					}

					select {
					case <-f.closeCh:
					case signal <- er:
					}
				})
			if err != nil {
				return fmt.Errorf("feed_dcp_gocbcore: Start, GetCollectionID,"+
					" collection: %v, err: %v", coll, err)
			}

			err = waitForResponse(signal, f.closeCh, op, GocbcoreStatsTimeout)
			if err != nil {
				return fmt.Errorf("feed_dcp_gocbcore: Start, Failed to get collection ID,"+
					"err : %v", err)
			}
		}

		f.streamFilter.Collections = f.collectionIDs
	}

	log.Printf("feed_dcp_gocbcore: Start, name: %s, num streams: %d,"+
		" streamFilter: %+v", f.Name(), len(f.vbucketIds), f.streamFilter)

	for _, vbid := range f.vbucketIds {
		err := f.initiateStream(uint16(vbid))
		if err != nil {
			return fmt.Errorf("feed_dcp_gocbcore: Start, name: %s, vbid: %v, err: %v",
				f.Name(), vbid, err)
		}
	}

	return nil
}

func (f *GocbcoreDCPFeed) Close() error {
	f.m.Lock()
	if f.closed {
		f.m.Unlock()
		return nil
	}
	f.closed = true
	f.agent.Close()
	f.forceCompleteLOCKED()
	f.m.Unlock()

	if f.mgr != nil {
		f.mgr.unregisterFeed(f.Name())
	}

	close(f.closeCh)
	f.wait()

	log.Printf("feed_dcp_gocbcore: Close, name: %s", f.Name())
	return nil
}

func (f *GocbcoreDCPFeed) Dests() map[string]Dest {
	return f.dests
}

var prefixAgentDCPStats = []byte(`{"agentDCPStats":`)

func (f *GocbcoreDCPFeed) Stats(w io.Writer) error {
	dcpStats := &gocbcoreDCPFeedStats{}
	f.dcpStats.atomicCopyTo(dcpStats, nil)

	_, err := w.Write(prefixAgentDCPStats)
	if err != nil {
		return err
	}

	err = json.NewEncoder(w).Encode(dcpStats)
	if err != nil {
		return err
	}

	_, err = w.Write(prefixDestStats)
	if err != nil {
		return err
	}

	f.stats.WriteJSON(w)

	_, err = w.Write(JsonCloseBrace)
	return err
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) initiateStream(vbId uint16) error {
	vbMetaData, lastSeq, err := f.getMetaData(vbId)
	if err != nil {
		return err
	}

	var vbuuid uint64
	if len(vbMetaData.FailOverLog) > 0 {
		vbuuid = vbMetaData.FailOverLog[0][0]
	}

	go f.initiateStreamEx(vbId, true, gocbcore.VbUUID(vbuuid),
		gocbcore.SeqNo(lastSeq), max_end_seqno)

	return nil
}

func (f *GocbcoreDCPFeed) initiateStreamEx(vbId uint16, isNewStream bool,
	vbuuid gocbcore.VbUUID, seqStart, seqEnd gocbcore.SeqNo) {
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
	log.Debugf("feed_dcp_gocbcore: Initiating DCP stream request for vb: %v,"+
		" vbUUID: %v, seqStart: %v, seqEnd: %v, streamFilter: %+v",
		vbId, vbuuid, seqStart, seqEnd, f.streamFilter)
	op, err := f.agent.OpenStream(vbId, gocbcore.DcpStreamAddFlagStrictVBUUID,
		vbuuid, seqStart, seqEnd, snapStart, snapStart, f, f.streamFilter,
		func(entries []gocbcore.FailoverEntry, er error) {
			if errors.Is(er, gocbcore.ErrShutdown) {
				log.Printf("feed_dcp_gocbcore: DCP stream for vb: %v was shutdown", vbId)
				f.complete(vbId)
			} else if errors.Is(er, gocbcore.ErrSocketClosed) {
				// TODO: Add a maximum retry-count here maybe?
				log.Warnf("feed_dcp_gocbcore: Network error received on DCP stream for"+
					" vb: %v", vbId)
				f.initiateStreamEx(vbId, false, vbuuid, seqStart, seqEnd)
			} else if errors.Is(er, gocbcore.ErrMemdRollback) {
				log.Printf("feed_dcp_gocbcore: Received rollback, for vb: %v,"+
					" seqno requested: %v", vbId, seqStart)
				f.complete(vbId)
				go f.rollback(vbId, entries)
			} else if er != nil {
				log.Warnf("feed_dcp_gocbcore: Received error on DCP stream for vb: %v,"+
					" err: %v", vbId, er)
				f.complete(vbId)
			} else {
				// er == nil
				atomic.AddUint64(&f.dcpStats.TotDCPStreamReqs, 1)
				failoverLog := make([][]uint64, len(entries))
				for i := 0; i < len(entries); i++ {
					failoverLog[i] = []uint64{
						uint64(entries[i].VbUUID),
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

	if err != nil && errors.Is(err, gocbcore.ErrShutdown) {
		f.onError(false, fmt.Errorf("OpenStream error for vb: %v, err: %v",
			vbId, err))
	}

	err = waitForResponse(signal, f.closeCh, op, GocbcoreOpenStreamTimeout)
	if err != nil {
		f.onError(false, fmt.Errorf("OpenStream, error waiting for vb: %v, err: %v",
			vbId, err))
	}
}

// ----------------------------------------------------------------

// onError is to be invoked in case of errors encountered while
// processing DCP messages.
func (f *GocbcoreDCPFeed) onError(isShutdown bool, err error) error {
	log.Warnf("feed_dcp_gocbcore: onError, name: %s,"+
		" bucketName: %s, bucketUUID: %s, isShutdown: %v, err: %v",
		f.name, f.bucketName, f.bucketUUID, isShutdown, err)

	go f.Close()

	if isShutdown && f.mgr != nil && f.mgr.meh != nil {
		go f.mgr.meh.OnFeedError(SOURCE_GOCBCORE, f, err)
	}

	return err
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, streamId uint16, snapshotType gocbcore.SnapshotState) {
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

	atomic.AddUint64(&f.dcpStats.TotDCPSnapshotMarkers, 1)
}

func (f *GocbcoreDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	collectionId uint32, streamId uint16, key, value []byte) {
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
			extras := GocbcoreDCPExtras{
				ScopeId:      f.streamFilter.Scope,
				CollectionId: collectionId,
				Expiry:       expiry,
				Flags:        flags,
				Datatype:     datatype,
			}
			err = destEx.DataUpdateEx(partition, key, seqNo, value, cas,
				DEST_EXTRAS_TYPE_GOCBCORE_DCP, extras)
		} else {
			extras := make([]byte, 8) // 8 bytes needed to hold 2 uint32s
			binary.LittleEndian.PutUint32(extras[0:], f.scopeID)
			binary.LittleEndian.PutUint32(extras[4:], collectionId)
			err = dest.DataUpdate(partition, key, seqNo, value, cas,
				DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION, extras)
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

	atomic.AddUint64(&f.dcpStats.TotDCPMutations, 1)
}

func (f *GocbcoreDCPFeed) Deletion(seqNo, revNo, cas uint64, datatype uint8,
	vbId uint16, collectionId uint32, streamId uint16, key, value []byte) {
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
			extras := GocbcoreDCPExtras{
				ScopeId:      f.streamFilter.Scope,
				CollectionId: collectionId,
				Datatype:     datatype,
				Value:        value,
			}
			err = destEx.DataDeleteEx(partition, key, seqNo, cas,
				DEST_EXTRAS_TYPE_GOCBCORE_DCP, extras)
		} else {
			extras := make([]byte, 8) // 8 bytes needed to hold 2 uint32s
			binary.LittleEndian.PutUint32(extras[0:], f.scopeID)
			binary.LittleEndian.PutUint32(extras[4:], collectionId)
			err = dest.DataDelete(partition, key, seqNo, cas,
				DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION, extras)
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

	atomic.AddUint64(&f.dcpStats.TotDCPDeletions, 1)
}

func (f *GocbcoreDCPFeed) Expiration(seqNo, revNo, cas uint64, vbId uint16,
	collectionId uint32, streamId uint16, key []byte) {
	f.Deletion(seqNo, revNo, cas, 0, vbId, collectionId, streamId, key, nil)
}

func (f *GocbcoreDCPFeed) End(vbId uint16, streamId uint16, err error) {
	atomic.AddUint64(&f.dcpStats.TotDCPStreamEnds, 1)
	lastReceivedSeqno := f.lastReceivedSeqno[vbId]
	if err == nil {
		log.Printf("feed_dcp_gocbcore: DCP stream ended for vb: %v, last seq: %v",
			vbId, lastReceivedSeqno)
		f.complete(vbId)
	} else if errors.Is(err, gocbcore.ErrShutdown) ||
		errors.Is(err, gocbcore.ErrSocketClosed) {
		if atomic.AddUint32(&f.shutdownVbs, 1) >= uint32(len(f.vbucketIds)) {
			// initiate a feed closure, if a shutdown message has been received
			// for all vbuckets accounted for
			f.onError(true, err)
		}
	} else if errors.Is(err, gocbcore.ErrDCPStreamStateChanged) ||
		errors.Is(err, gocbcore.ErrDCPStreamTooSlow) ||
		errors.Is(err, gocbcore.ErrDCPStreamDisconnected) {
		log.Printf("feed_dcp_gocbcore: DCP stream for vb: %v, closed due to"+
			" `%s`, will reconnect", vbId, err.Error())
		f.initiateStreamEx(vbId, false, gocbcore.VbUUID(0),
			gocbcore.SeqNo(lastReceivedSeqno), max_end_seqno)
	} else if errors.Is(err, gocbcore.ErrDCPStreamClosed) {
		log.Printf("feed_dcp_gocbcore: DCP stream for vb: %v, closed by consumer", vbId)
		f.complete(vbId)
	} else {
		log.Debugf("feed_dcp_gocbcore: DCP stream closed for vb: %v, last seq: %v,"+
			" err: `%s`", vbId, lastReceivedSeqno, err.Error())
	}
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) CreateCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32,
	ttl uint32, streamId uint16, key []byte) {
	// FIXME
}

func (f *GocbcoreDCPFeed) DeleteCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32,
	streamId uint16) {
	// FIXME
}

func (f *GocbcoreDCPFeed) FlushCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, collectionId uint32) {
	// FIXME
}

func (f *GocbcoreDCPFeed) CreateScope(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, scopeId uint32, streamId uint16, key []byte) {
	// FIXME
}

func (f *GocbcoreDCPFeed) DeleteScope(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, scopeId uint32, streamId uint16) {
	// FIXME
}

func (f *GocbcoreDCPFeed) ModifyCollection(seqNo uint64, version uint8, vbId uint16,
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

func (f *GocbcoreDCPFeed) checkAndUpdateVBucketState(vbId uint16) error {
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

func (f *GocbcoreDCPFeed) setMetaData(vbId uint16, m *metaData) error {
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

func (f *GocbcoreDCPFeed) getMetaData(vbId uint16) (*metaData, uint64, error) {
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

func (f *GocbcoreDCPFeed) rollback(vbId uint16, entries []gocbcore.FailoverEntry) {
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
		log.Warnf("feed_dcp_gocbcore: Rollback to seqno: %v, vbuuid: %v for vb: %v,"+
			" failed with err: %v", rollbackSeqno, rollbackVbuuid, vbId, err)
	}

	atomic.AddUint64(&f.dcpStats.TotDCPRollbacks, 1)
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) wait() {
	f.remaining.Wait()
}

func (f *GocbcoreDCPFeed) complete(vbId uint16) {
	f.m.Lock()
	if f.active[vbId] {
		f.active[vbId] = false
		f.remaining.Done()
	}
	f.m.Unlock()
}

func (f *GocbcoreDCPFeed) forceCompleteLOCKED() {
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
func (f *GocbcoreDCPFeed) checkStopAfter(partition string) bool {
	f.m.Lock()
	reached := f.stopAfterReached != nil && f.stopAfterReached[partition]
	f.m.Unlock()

	return reached
}

// updateStopAfter checks and maintains the stopAfterReached tracking
// maps, which are used for so-called "one-time indexing". Once we've
// reached the stopping point, we close the feed (after all partitions
// have reached their stopAfter sequence numbers).
func (f *GocbcoreDCPFeed) updateStopAfter(partition string, seqNo uint64) {
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

func (f *GocbcoreDCPFeed) VerifyBucketNotExists() (bool, error) {
	agent, err := gocbcore.CreateAgent(f.config)
	if err != nil {
		if errors.Is(err, gocbcore.ErrBucketNotFound) ||
			errors.Is(err, gocbcore.ErrAuthenticationFailure) {
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

func (f *GocbcoreDCPFeed) GetBucketDetails() (string, string) {
	return f.bucketName, f.bucketUUID
}
