//  Copyright (c) 2014 Couchbase, Inc.
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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/couchbase/clog"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
)

const source_gocouchbase = "couchbase"
const source_gocouchbase_dcp = "couchbase-dcp"

// DEST_EXTRAS_TYPE_DCP represents the extras that comes from DCP
// protocol.
const DEST_EXTRAS_TYPE_DCP = DestExtrasType(0x0002)

// DEST_EXTRAS_TYPE_MCREQUEST represents the MCRequest from DCP
// protocol.
const DEST_EXTRAS_TYPE_MCREQUEST = DestExtrasType(0x0003)

// DCPFeedPrefix should be immutable after process init()'ialization.
var DCPFeedPrefix string

// DCPFeedBufferSizeBytes is representative of connection_buffer_size
// for DCP to enable flow control, defaults at 20MB.
var DCPFeedBufferSizeBytes = uint32(20000000)

// DCPFeedBufferAckThreshold is representative of the percentage of
// the connection_buffer_size when the consumer will ack back to
// the producer.
var DCPFeedBufferAckThreshold = float32(0.8)

// DCPNoopTimeIntervalSecs is representative of set_noop_interval
// for DCP to enable no-op messages, defaults at 2min.
var DCPNoopTimeIntervalSecs = uint32(120)

func init() {
	RegisterFeedType(source_gocouchbase, &FeedType{
		Start:           StartDCPFeed,
		Partitions:      CouchbasePartitions,
		PartitionSeqs:   CouchbasePartitionSeqs,
		Stats:           CouchbaseStats,
		PartitionLookUp: CouchbaseSourceVBucketLookUp,
		Public:          true,
		Description: "general/" + source_gocouchbase +
			" - a Couchbase Server bucket will be the data source",
		StartSample: NewDCPFeedParams(),
	})
	RegisterFeedType(source_gocouchbase_dcp, &FeedType{
		Start:           StartDCPFeed,
		Partitions:      CouchbasePartitions,
		PartitionSeqs:   CouchbasePartitionSeqs,
		Stats:           CouchbaseStats,
		PartitionLookUp: CouchbaseSourceVBucketLookUp,
		Public:          false, // Won't be listed in /api/managerMeta output.
		Description: "general/" + source_gocouchbase_dcp +
			" - a Couchbase Server bucket will be the data source," +
			" via DCP protocol",
		StartSample: NewDCPFeedParams(),
	})
}

type UpdateBucketDataSourceOptionsFunc func(
	params *cbdatasource.BucketDataSourceOptions) *cbdatasource.BucketDataSourceOptions

// Registry of callback functions (of type UpdateBucketDataSourceOptionsFunc)
// for indexes created against a couchbase cluster (tracked with manager's uuid).
// The key for this map is the indexname concatenated with manager's uuid.
// The client can optionally register the callback before invoking the manager
// api: CreateIndex. This callback if registered will be invoked during DCP feed
// setup and the client will be allowed to update the feed options (of type
// go-couchbase/cbdatasource's BucketDataSourceOptions).
var bucketDataSourceOptionsM sync.RWMutex
var bucketDataSourceOptionsCBMap = make(map[string]UpdateBucketDataSourceOptionsFunc)

func RegisterBucketDataSourceOptionsCallback(
	indexName, managerUUID string, f UpdateBucketDataSourceOptionsFunc) {
	bucketDataSourceOptionsM.Lock()
	bucketDataSourceOptionsCBMap[indexName+managerUUID] = f
	bucketDataSourceOptionsM.Unlock()
}

func fetchBucketDataSourceOptionsCallback(
	indexName, managerUUID string) UpdateBucketDataSourceOptionsFunc {
	bucketDataSourceOptionsM.RLock()
	f := bucketDataSourceOptionsCBMap[indexName+managerUUID]
	bucketDataSourceOptionsM.RUnlock()
	return f
}

// StartDCPFeed starts a DCP related feed and is registered at
// init/startup time with the system via RegisterFeedType().
func StartDCPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]Dest) error {
	server, poolName, bucketName :=
		CouchbaseParseSourceName(mgr.server, "default", sourceName)

	feed, err := NewDCPFeed(feedName, indexName, server, poolName,
		bucketName, bucketUUID, params, BasicPartitionFunc, dests,
		mgr.tagsMap != nil && !mgr.tagsMap["feed"], mgr)
	if err != nil {
		return fmt.Errorf("feed_dcp_gocouchbase:"+
			" could not prepare DCP feed, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_dcp_gocouchbase:"+
			" could not start, server: %s, err: %v",
			mgr.server, err)
	}

	securityConfigRefreshHandler := func() error {
		log.Printf("feed_dcp_gocouchbase: %s security refresh kick: %s",
			feed.name, cbdatasource.UpdateSecuritySettings)
		return feed.bds.Kick(cbdatasource.UpdateSecuritySettings)
	}

	RegisterConfigRefreshCallback("DCPFeed_"+feed.name,
		securityConfigRefreshHandler)

	err = mgr.registerFeed(feed)
	if err != nil {
		feed.Close()
		return err
	}
	return nil
}

// A DCPFeed implements both Feed and cbdatasource.Receiver
// interfaces, and forwards any incoming cbdatasource.Receiver
// callbacks to the relevant, hooked-up Dest instances.
//
// url: single URL or multiple URLs delimited by ';'
type DCPFeed struct {
	name       string
	indexName  string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	paramsStr  string
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	stopAfter  map[string]UUIDSeq // May be nil.
	bds        cbdatasource.BucketDataSource
	mgr        *Manager
	auth       couchbase.AuthHandler

	m       sync.Mutex // Protects the fields that follow.
	closed  bool
	lastErr error
	stats   *DestStats

	stopAfterReached map[string]bool // May be nil.
}

// NewDCPFeed creates a new, ready-to-be-started DCP feed.
//
// url: single URL or multiple URLs delimited by ';'
func NewDCPFeed(name, indexName, url, poolName,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*DCPFeed, error) {
	log.Printf("feed_dcp_gocouchbase: NewDCPFeed, name: %s, indexName: %s",
		name, indexName)

	var optionsMgr map[string]string
	if mgr != nil {
		optionsMgr = mgr.Options()
	}

	auth, err := cbAuth(bucketName, paramsStr, optionsMgr)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocouchbase: NewDCPFeed cbAuth, err: %v", err)
	}

	var stopAfter map[string]UUIDSeq

	params := NewDCPFeedParams()

	if paramsStr != "" {
		err = json.Unmarshal([]byte(paramsStr), params)
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

	// If feedBufferSizeBytes (to enable DCP flow control) isn't
	// set, initialize it to the default.
	if !strings.Contains(paramsStr, "feedBufferSizeBytes") {
		params.FeedBufferSizeBytes = DCPFeedBufferSizeBytes
	}

	// If feedBufferAckThreshold (acking for flow control) isn't
	// set, initialize it to the default.
	if !strings.Contains(paramsStr, "feedBufferAckThreshold") {
		params.FeedBufferAckThreshold = DCPFeedBufferAckThreshold
	}

	// If noopTimeIntervalSecs (to enable DCP noops) isn't
	// set, initialize it to the default.
	if !strings.Contains(paramsStr, "noopTimeIntervalSecs") {
		params.NoopTimeIntervalSecs = DCPNoopTimeIntervalSecs
	}

	vbucketIds, err := ParsePartitionsToVBucketIds(dests)
	if err != nil {
		return nil, err
	}
	if len(vbucketIds) <= 0 {
		vbucketIds = nil
	}

	urls := strings.Split(url, ";")

	options := &cbdatasource.BucketDataSourceOptions{
		Name:                        fmt.Sprintf("%s%s-%x", DCPFeedPrefix, name, rand.Int31()),
		ClusterManagerBackoffFactor: params.ClusterManagerBackoffFactor,
		ClusterManagerSleepInitMS:   params.ClusterManagerSleepInitMS,
		ClusterManagerSleepMaxMS:    params.ClusterManagerSleepMaxMS,
		DataManagerBackoffFactor:    params.DataManagerBackoffFactor,
		DataManagerSleepInitMS:      params.DataManagerSleepInitMS,
		DataManagerSleepMaxMS:       params.DataManagerSleepMaxMS,
		FeedBufferSizeBytes:         params.FeedBufferSizeBytes,
		FeedBufferAckThreshold:      params.FeedBufferAckThreshold,
		NoopTimeIntervalSecs:        params.NoopTimeIntervalSecs,
		Logf:                        log.Printf,
		TraceCapacity:               20,
		IncludeXAttrs:               params.IncludeXAttrs,
	}

	// Allow client to update cbdatasource.BucketDataSourceOptions
	if mgr != nil {
		bucketDataSourceOptionsCB := fetchBucketDataSourceOptionsCallback(
			indexName, mgr.uuid)
		if bucketDataSourceOptionsCB != nil {
			options = bucketDataSourceOptionsCB(options)
		}
	}

	feed := &DCPFeed{
		name:       name,
		indexName:  indexName,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		paramsStr:  paramsStr,
		params:     params,
		pf:         pf,
		dests:      dests,
		disable:    disable,
		stopAfter:  stopAfter,
		mgr:        mgr,
		auth:       auth,
		stats:      NewDestStats(),
	}

	feed.bds, err = cbdatasource.NewBucketDataSource(
		urls, poolName, bucketName, bucketUUID,
		vbucketIds, auth, feed, options)
	if err != nil {
		return nil, err
	}
	return feed, nil
}

func (t *DCPFeed) Name() string {
	return t.name
}

func (t *DCPFeed) IndexName() string {
	return t.indexName
}

func (t *DCPFeed) Start() error {
	if t.disable {
		log.Printf("feed_dcp_gocouchbase: disabled, name: %s", t.Name())
		return nil
	}

	log.Printf("feed_dcp_gocouchbase: start, name: %s", t.Name())
	return t.bds.Start()
}

func (t *DCPFeed) Close() error {
	t.m.Lock()
	if t.closed {
		t.m.Unlock()
		return nil
	}
	t.closed = true
	t.m.Unlock()

	log.Printf("feed_dcp_gocouchbase: close, name: %s", t.Name())
	return t.bds.Close()
}

func (t *DCPFeed) Dests() map[string]Dest {
	return t.dests
}

var prefixBucketDataSourceStats = []byte(`{"bucketDataSourceStats":`)
var prefixDestStats = []byte(`,"destStats":`)

func (t *DCPFeed) Stats(w io.Writer) error {
	bdss := cbdatasource.BucketDataSourceStats{}
	err := t.bds.Stats(&bdss)
	if err != nil {
		return err
	}
	w.Write(prefixBucketDataSourceStats)
	json.NewEncoder(w).Encode(&bdss)

	w.Write(prefixDestStats)
	t.stats.WriteJSON(w)

	_, err = w.Write(JsonCloseBrace)
	return err
}

// --------------------------------------------------------

// checkStopAfter checks to see if we've already reached the
// stopAfterReached state for a partition.
func (r *DCPFeed) checkStopAfter(partition string) bool {
	r.m.Lock()
	reached := r.stopAfterReached != nil && r.stopAfterReached[partition]
	r.m.Unlock()

	return reached
}

// updateStopAfter checks and maintains the stopAfterReached tracking
// maps, which are used for so-called "one-time indexing". Once we've
// reached the stopping point, we close the feed (after all partitions
// have reached their stopAfter sequence numbers).
func (r *DCPFeed) updateStopAfter(partition string, seq uint64) {
	if r.stopAfter == nil {
		return
	}

	uuidSeq, exists := r.stopAfter[partition]
	if !exists {
		return
	}

	// TODO: check UUID matches?
	if seq >= uuidSeq.Seq {
		r.m.Lock()

		if r.stopAfterReached == nil {
			r.stopAfterReached = map[string]bool{}
		}
		r.stopAfterReached[partition] = true

		allDone := len(r.stopAfterReached) >= len(r.stopAfter)

		r.m.Unlock()

		if allDone {
			go r.Close()
		}
	}
}

// --------------------------------------------------------

func (r *DCPFeed) OnError(err error) {
	// TODO: Check the type of the error if it's something
	// serious / not-recoverable / needs user attention.
	log.Warnf("feed_dcp_gocouchbase: OnError, name: %s,"+
		" bucketName: %s, bucketUUID: %s, err: %v",
		r.name, r.bucketName, r.bucketUUID, err)

	atomic.AddUint64(&r.stats.TotError, 1)
	if r.mgr != nil && r.mgr.meh != nil {
		go r.mgr.meh.OnFeedError("couchbase", r, err)
	}

	r.m.Lock()
	r.lastErr = err
	r.m.Unlock()
}

func (r *DCPFeed) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, key)
		if err != nil || r.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			err = destEx.DataUpdateEx(partition, key, seq, req.Body,
				req.Cas, DEST_EXTRAS_TYPE_MCREQUEST, req)
		} else {
			err = dest.DataUpdate(partition, key, seq, req.Body,
				req.Cas, DEST_EXTRAS_TYPE_DCP, req.Extras)
		}

		if err != nil {
			return fmt.Errorf("feed_dcp_gocouchbase: DataUpdate,"+
				" name: %s, partition: %s, key: %v, seq: %d, err: %v",
				r.name, partition,
				log.Tag(log.UserData, key), seq, err)
		}

		r.updateStopAfter(partition, seq)

		return nil
	}, r.stats.TimerDataUpdate)
}

func (r *DCPFeed) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, key)
		if err != nil || r.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			err = destEx.DataDeleteEx(partition, key, seq,
				req.Cas, DEST_EXTRAS_TYPE_MCREQUEST, req)
		} else {
			err = dest.DataDelete(partition, key, seq,
				req.Cas, DEST_EXTRAS_TYPE_DCP, req.Extras)
		}

		if err != nil {
			return fmt.Errorf("feed_dcp_gocouchbase: DataDelete,"+
				" name: %s, partition: %s, key: %v, seq: %d, err: %v",
				r.name, partition,
				log.Tag(log.UserData, key), seq, err)
		}

		r.updateStopAfter(partition, seq)

		return nil
	}, r.stats.TimerDataDelete)
}

func (r *DCPFeed) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil || r.checkStopAfter(partition) {
			return err
		}

		if r.stopAfter != nil {
			uuidSeq, exists := r.stopAfter[partition]
			if exists && snapEnd > uuidSeq.Seq { // TODO: Check UUID.
				// Clamp the snapEnd so batches are executed.
				snapEnd = uuidSeq.Seq
			}
		}

		return dest.SnapshotStart(partition, snapStart, snapEnd)
	}, r.stats.TimerSnapshotStart)
}

func (r *DCPFeed) SetMetaData(vbucketId uint16, value []byte) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil || r.checkStopAfter(partition) {
			return err
		}

		return dest.OpaqueSet(partition, value)
	}, r.stats.TimerOpaqueSet)
}

func (r *DCPFeed) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	err = Timer(func() error {
		partition, dest, err2 :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err2 != nil || r.checkStopAfter(partition) {
			return err2
		}

		value, lastSeq, err2 = dest.OpaqueGet(partition)

		return err2
	}, r.stats.TimerOpaqueGet)

	return value, lastSeq, err
}

func (r *DCPFeed) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil || r.checkStopAfter(partition) {
			return err
		}

		opaqueValue, lastSeq, err := dest.OpaqueGet(partition)
		if err != nil {
			return err
		}

		log.Printf("feed_dcp_gocouchbase: rollback, name: %s: vbucketId: %d,"+
			" rollbackSeq: %d, partition: %s, opaqueValue: %s, lastSeq: %d",
			r.name, vbucketId, rollbackSeq,
			partition, opaqueValue, lastSeq)

		return dest.Rollback(partition, rollbackSeq)
	}, r.stats.TimerRollback)
}

func (r *DCPFeed) RollbackEx(vbucketId uint16, vBucketUUID uint64, rollbackSeq uint64) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil || r.checkStopAfter(partition) {
			return err
		}

		opaqueValue, lastSeq, err := dest.OpaqueGet(partition)
		if err != nil {
			return err
		}

		log.Printf("feed_dcp_gocouchbase: rollback, name: %s: vbucketId: %d,"+
			" rollbackSeq: %d, partition: %s, opaqueValue: %s, lastSeq: %d",
			r.name, vbucketId, rollbackSeq,
			partition, opaqueValue, lastSeq)

		if destEx, ok := dest.(DestEx); ok {
			return destEx.RollbackEx(partition, vBucketUUID, rollbackSeq)
		}
		return dest.Rollback(partition, rollbackSeq)
	}, r.stats.TimerRollback)
}

// VerifyBucketNotExists returns true only if it's sure the bucket
// does not exist anymore (including if UUID's no longer match).  A
// rejected auth or connection failure, for example, results in false.
func (r *DCPFeed) VerifyBucketNotExists() (bool, error) {
	urls := strings.Split(r.url, ";")
	if len(urls) <= 0 {
		return false, nil
	}

	bucket, err := CouchbaseBucket(r.bucketName, r.bucketUUID, r.paramsStr,
		urls[0], r.mgr.Options())
	if err != nil {
		if _, ok := err.(*couchbase.BucketNotFoundError); ok {
			return true, err
		}

		if err == ErrCouchbaseMismatchedBucketUUID {
			return true, err
		}

		return false, err
	}

	bucket.Close()

	return false, nil
}

func (r *DCPFeed) GetBucketDetails() (name, uuid string) {
	return r.bucketName, r.bucketUUID
}
