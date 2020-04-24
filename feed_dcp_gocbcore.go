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
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
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
var GocbcoreConnectTimeout = time.Duration(60 * time.Second)
var GocbcoreKVConnectTimeout = time.Duration(7 * time.Second)
var GocbcoreAgentSetupTimeout = time.Duration(10 * time.Second)

// ----------------------------------------------------------------

func setupDCPAgentConfig(name, bucketName string,
	auth gocbcore.AuthProvider) *gocbcore.DCPAgentConfig {
	return &gocbcore.DCPAgentConfig{
		UserAgent:        name,
		BucketName:       bucketName,
		Auth:             auth,
		ConnectTimeout:   GocbcoreConnectTimeout,
		KVConnectTimeout: GocbcoreKVConnectTimeout,
		UseCollections:   true,
		UseOSOBackfill:   false, // FIXME
		AgentPriority:    gocbcore.DcpAgentPriorityMed,
	}
}

func setupGocbcoreDCPAgent(config *gocbcore.DCPAgentConfig,
	connName string, flags memd.DcpOpenFlag) (
	*gocbcore.DCPAgent, error) {
	agent, err := gocbcore.CreateDcpAgent(config, connName, flags)
	if err != nil {
		return nil, err
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState: gocbcore.ClusterStateOnline,
		ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService},
	}

	signal := make(chan error, 1)
	if _, err = agent.WaitUntilReady(time.Now().Add(GocbcoreAgentSetupTimeout),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		}); err != nil {
		return nil, err
	}

	return agent, <-signal
}

// ----------------------------------------------------------------

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
			op.Cancel()
		}
		return gocbcore.ErrTimeout
	}
}

// ----------------------------------------------------------------

func init() {
	RegisterFeedType(SOURCE_GOCBCORE, &FeedType{
		Start:            StartGocbcoreDCPFeed,
		Partitions:       CBPartitions,
		PartitionSeqs:    CBPartitionSeqs,
		Stats:            CBStats,
		PartitionLookUp:  CBVBucketLookUp,
		SourceUUIDLookUp: CBSourceUUIDLookUp,
		Public:           true,
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

	feed, err := newGocbcoreDCPFeed(feedName, indexName, indexUUID,
		server, bucketName, bucketUUID, params, BasicPartitionFunc,
		dests, mgr.tagsMap != nil && !mgr.tagsMap["feed"], mgr)
	if err != nil {
		return fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed,"+
			" could not prepare DCP feed, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}

	// check if all requested vbuckets are ready to be streamed from
	if err = feed.waitForVBsToBecomeReady(); err != nil {
		return fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed,"+
			" err: %v", err)
	}

	err = mgr.registerFeed(feed)
	if err != nil {
		return feed.onError(false, false, err)
	}

	err = feed.Start()
	if err != nil {
		return feed.onError(false, false,
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

// DestCollection interface needs to be implemented by the dest/pindex
// implementations which consumes data from the collections.
type DestCollection interface {
	// PrepareFeedParams provides a way for the pindex
	// implementation to customise any DCPFeedParams.
	PrepareFeedParams(partition string, params *DCPFeedParams) error
}

// A GocbcoreDCPFeed implements both Feed and gocb.StreamObserver
// interfaces, and forwards any incoming gocb.StreamObserver
// callbacks to the relevant, hooked-up Dest instances.
//
// url: single URL or multiple URLs delimited by ';'
type GocbcoreDCPFeed struct {
	name       string
	indexName  string
	indexUUID  string
	url        string
	bucketName string
	bucketUUID string
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	stopAfter  map[string]UUIDSeq
	config     *gocbcore.DCPAgentConfig
	agent      *gocbcore.DCPAgent
	mgr        *Manager

	scope       string
	collections []string

	scopeID       uint32
	collectionIDs []uint32

	streamOptions gocbcore.OpenStreamOptions

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

func newGocbcoreDCPFeed(name, indexName, indexUUID, url,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*GocbcoreDCPFeed, error) {
	var options map[string]string
	if mgr != nil {
		options = mgr.Options()
	}

	auth, err := gocbAuth(paramsStr, options)
	if err != nil {
		return nil, fmt.Errorf("newGocbcoreDCPFeed gocbAuth, err: %v", err)
	}

	var stopAfter map[string]UUIDSeq

	params := NewDCPFeedParams()

	if paramsStr != "" {
		err := json.Unmarshal([]byte(paramsStr), params)
		if err != nil {
			return nil, fmt.Errorf("newGocbcoreDCPFeed params, err: %v", err)
		}

		stopAfterSourceParams := StopAfterSourceParams{}
		err = json.Unmarshal([]byte(paramsStr), &stopAfterSourceParams)
		if err != nil {
			return nil, fmt.Errorf("newGocbcoreDCPFeed stopAfterSourceParams,"+
				" err: %v", err)
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
		return nil, fmt.Errorf("newGocbcoreDCPFeed, err: %v", err)
	}
	if len(vbucketIds) == 0 {
		return nil, fmt.Errorf("newGocbcoreDCPFeed:" +
			" no vbucketids for this feed")
	}

	feed := &GocbcoreDCPFeed{
		name:       name,
		indexName:  indexName,
		indexUUID:  indexUUID,
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

	for partition, dest := range dests {
		if destColl, ok := dest.(DestCollection); ok {
			err := destColl.PrepareFeedParams(partition, params)
			if err != nil {
				return nil, err
			}
		}
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

	config := setupDCPAgentConfig(name, bucketName, auth)

	urls := strings.Split(url, ";")
	if len(urls) <= 0 {
		return nil, fmt.Errorf("newGocbcoreDCPFeed: no urls provided")
	}

	err = config.FromConnStr(urls[0])
	if err != nil {
		return nil, fmt.Errorf("newGocbcoreDCPFeed config, err: %v", err)
	}

	// setup rootCAs provider
	if options != nil && options["authType"] == "cbauth" {
		config.TLSRootCAProvider = FetchGocbcoreSecurityConfig
	} else {
		// in the case the authType isn't cbauth, check if user has
		// set TLSCertFile
		var rootCAs *x509.CertPool
		if len(TLSCertFile) > 0 {
			certInBytes, err := ioutil.ReadFile(TLSCertFile)
			if err != nil {
				return nil, fmt.Errorf("newGocbcoreDcpFeed tls, err: %v", err)
			}

			rootCAs = x509.NewCertPool()
			ok := rootCAs.AppendCertsFromPEM(certInBytes)
			if !ok {
				return nil, fmt.Errorf("newGocbcoreDCPFeed: " +
					" error appending certificates")
			}
		}

		config.TLSRootCAProvider = func() *x509.CertPool {
			return rootCAs
		}
	}

	feed.config = config

	if err = feed.setupStreamOptions(); err != nil {
		return nil, fmt.Errorf("newGocbcoreDCPFeed:"+
			" error in setting up feed's stream options, err: %v", err)
	}

	flags := memd.DcpOpenFlagProducer

	if params.IncludeXAttrs {
		flags |= memd.DcpOpenFlagIncludeXattrs
	}

	if params.NoValue {
		flags |= memd.DcpOpenFlagNoValue
	}

	dcpConnName := fmt.Sprintf("%s%s-%x", DCPFeedPrefix, name, rand.Int31())
	feed.agent, err = setupGocbcoreDCPAgent(config, dcpConnName, flags)
	if err != nil {
		return nil, fmt.Errorf("newGocbcoreFeed DCPAgent, err: %v", err)
	}

	log.Printf("feed_dcp_gocbcore: NewGocbcoreDCPFeed, name: %s, indexName: %s,"+
		" server: %v, bucketName: %s, bucketUUID: %s, connection name: %s",
		name, indexName, urls[0], feed.bucketName, feed.bucketUUID, dcpConnName)

	return feed, nil
}

func (f *GocbcoreDCPFeed) setupStreamOptions() error {
	config := setupAgentConfig(f.name, f.bucketName, f.config.Auth)
	err := config.FromConnStr(strings.Split(f.url, ";")[0])
	if err != nil {
		return err
	}

	agent, err := setupGocbcoreAgent(config)
	if err != nil {
		return err
	}
	defer agent.Close()

	if len(f.bucketUUID) == 0 {
		// the sourceUUID setting in the index definition is optional,
		// so make sure the feed's bucketUUID is set to the correct
		// value if in case it wasn't provided
		snapshot, err := agent.ConfigSnapshot()
		if err != nil {
			return err
		}

		f.bucketUUID = snapshot.BucketUUID()
	}

	signal := make(chan error, 1)
	var manifest gocbcore.Manifest
	op, err := agent.GetCollectionManifest(
		gocbcore.GetCollectionManifestOptions{},
		func(res *gocbcore.GetCollectionManifestResult, er error) {
			if er == nil && res == nil {
				er = fmt.Errorf("manifest not retrieved")
			}

			if er == nil {
				er = manifest.UnmarshalJSON(res.Manifest)
			}

			signal <- er
		})

	if err != nil {
		return fmt.Errorf("GetCollectionManifest, err: %v", err)
	}

	err = waitForResponse(signal, f.closeCh, op, GocbcoreStatsTimeout)
	if err != nil {
		return fmt.Errorf("failed to get manifest, err: %v", err)
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
		return fmt.Errorf("scope not found: %v", f.scope)
	}

	f.streamOptions = gocbcore.OpenStreamOptions{}
	f.streamOptions.ManifestOptions = &gocbcore.OpenStreamManifestOptions{
		ManifestUID: manifest.UID,
	}

	if len(f.collections) == 0 {
		// if no collections were specified, set up stream requests for
		// the entire scope.
		f.streamOptions.FilterOptions = &gocbcore.OpenStreamFilterOptions{
			ScopeID: f.scopeID,
		}
	} else {
		for _, coll := range f.collections {
			op, err = agent.GetCollectionID(f.scope, coll,
				gocbcore.GetCollectionIDOptions{},
				func(res *gocbcore.GetCollectionIDResult, er error) {
					if er == nil && res == nil {
						er = fmt.Errorf("collection ID not retrieved")
					}

					if er == nil {
						if res.ManifestID != f.streamOptions.ManifestOptions.ManifestUID {
							er = fmt.Errorf("manifestID mismatch, %v != %v",
								res.ManifestID, f.streamOptions.ManifestOptions.ManifestUID)
						} else {
							f.collectionIDs =
								append(f.collectionIDs, res.CollectionID)
						}
					}

					signal <- er
				})
			if err != nil {
				return fmt.Errorf("GetCollectionID, collection: %v, err: %v",
					coll, err)
			}

			err = waitForResponse(signal, f.closeCh, op, GocbcoreStatsTimeout)
			if err != nil {
				return fmt.Errorf("failed to get collection ID, err : %v", err)
			}
		}

		f.streamOptions.FilterOptions = &gocbcore.OpenStreamFilterOptions{
			CollectionIDs: f.collectionIDs,
		}
	}

	return nil
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

	log.Printf("feed_dcp_gocbcore: Start, name: %s, num streams: %d,"+
		" streamOptions: {ManifestOptions: %+v, FilterOptions: %+v}",
		f.Name(), len(f.vbucketIds),
		f.streamOptions.ManifestOptions, f.streamOptions.FilterOptions)

	for _, vbid := range f.vbucketIds {
		err := f.initiateStream(uint16(vbid))
		if err != nil {
			return fmt.Errorf("Start, name: %s, vbid: %v, err: %v",
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
	go f.agent.Close()
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
	f.m.Lock()
	if f.closed {
		f.m.Unlock()
		return
	}
	if isNewStream {
		if !f.active[vbId] {
			f.remaining.Add(1)
			f.active[vbId] = true
		}
	}
	f.m.Unlock()

	snapStart := seqStart
	signal := make(chan error, 1)
	log.Debugf("feed_dcp_gocbcore: Initiating DCP stream request for vb: %v,"+
		" vbUUID: %v, seqStart: %v, seqEnd: %v, streamOptions: {%+v, %+v}",
		vbId, vbuuid, seqStart, seqEnd,
		f.streamOptions.ManifestOptions, f.streamOptions.FilterOptions)
	op, err := f.agent.OpenStream(vbId, memd.DcpStreamAddFlagStrictVBUUID,
		vbuuid, seqStart, seqEnd, snapStart, snapStart, f, f.streamOptions,
		func(entries []gocbcore.FailoverEntry, er error) {
			if errors.Is(er, gocbcore.ErrShutdown) ||
				errors.Is(er, gocbcore.ErrSocketClosed) {
				log.Printf("feed_dcp_gocbcore: DCP stream for vb: %v was shutdown,"+
					"err: %v", vbId, er)
				f.complete(vbId)
			} else if errors.Is(er, gocbcore.ErrMemdRollback) {
				log.Printf("feed_dcp_gocbcore: [%s] Received rollback, for vb: %v,"+
					" seqno requested: %v", f.Name(), vbId, seqStart)
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

			signal <- er
		})

	if err != nil && (errors.Is(err, gocbcore.ErrShutdown) ||
		errors.Is(err, gocbcore.ErrSocketClosed)) {
		f.onError(true, false, fmt.Errorf("OpenStream error for vb: %v, err: %v",
			vbId, err))
		return
	}

	err = waitForResponse(signal, f.closeCh, op, GocbcoreKVConnectTimeout)
	if err != nil {
		// notify mgr on feed closure due to timeout
		f.onError(false, err == gocbcore.ErrTimeout,
			fmt.Errorf("OpenStream, error waiting for vb: %v, err: %v", vbId, err))
	}
}

// ----------------------------------------------------------------

// onError is to be invoked in case of errors encountered while
// processing DCP messages.
func (f *GocbcoreDCPFeed) onError(isShutdown, alertMgr bool, err error) error {
	log.Debugf("feed_dcp_gocbcore: onError, name: %s,"+
		" bucketName: %s, bucketUUID: %s, isShutdown: %v, err: %v",
		f.name, f.bucketName, f.bucketUUID, isShutdown, err)

	if isShutdown && f.mgr != nil && f.mgr.meh != nil {
		f.mgr.meh.OnFeedError(SOURCE_GOCBCORE, f, err)
	}

	f.Close()

	if alertMgr && f.mgr != nil {
		f.mgr.Kick("gocbcore-feed")
	}

	return err
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, streamId uint16, snapshotType gocbcore.SnapshotState) {
	if f.currVBs[vbId] == nil {
		f.onError(false, false, fmt.Errorf("SnapshotMarker, invalid vb: %d", vbId))
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
		f.onError(false, false, fmt.Errorf("SnapshotMarker, vb: %d, err: %v", vbId, err))
		return
	}

	atomic.AddUint64(&f.dcpStats.TotDCPSnapshotMarkers, 1)
}

func (f *GocbcoreDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	collectionId uint32, streamId uint16, key, value []byte) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(false, false, fmt.Errorf("Mutation, %v", err))
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
				ScopeId:      f.streamOptions.FilterOptions.ScopeID,
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
		f.onError(false, false, fmt.Errorf("Mutation, vb: %d, err: %v", vbId, err))
		return
	}

	f.lastReceivedSeqno[vbId] = seqNo

	atomic.AddUint64(&f.dcpStats.TotDCPMutations, 1)
}

func (f *GocbcoreDCPFeed) Deletion(seqNo, revNo uint64, deleteTime uint32,
	cas uint64, datatype uint8, vbId uint16, collectionId uint32, streamId uint16,
	key, value []byte) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(false, false, fmt.Errorf("Deletion, %v", err))
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
				ScopeId:      f.streamOptions.FilterOptions.ScopeID,
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
		f.onError(false, false, fmt.Errorf("Deletion, vb: %d, err: %v", vbId, err))
		return
	}

	f.lastReceivedSeqno[vbId] = seqNo

	atomic.AddUint64(&f.dcpStats.TotDCPDeletions, 1)
}

func (f *GocbcoreDCPFeed) Expiration(seqNo, revNo uint64, deleteTime uint32,
	cas uint64, vbId uint16, collectionId uint32, streamId uint16, key []byte) {
	f.Deletion(seqNo, revNo, deleteTime, cas, 0, vbId, collectionId, streamId, key, nil)
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
			// initiate a feed closure, if a shutdown message or socket closure
			// has been received for all vbuckets accounted for
			f.onError(true, false, err)
		}
	} else if errors.Is(err, gocbcore.ErrDCPStreamStateChanged) {
		log.Warnf("feed_dcp_gocbcore: DCP stream for vb: %v, closed due to"+
			" `%s`, closing feed and alert the mgr", vbId, err.Error())
		f.onError(false, true, err)
	} else if errors.Is(err, gocbcore.ErrDCPStreamTooSlow) ||
		errors.Is(err, gocbcore.ErrDCPStreamDisconnected) {
		log.Printf("feed_dcp_gocbcore: DCP stream for vb: %v, closed due to"+
			" `%s`, will reconnect", vbId, err.Error())
		go f.initiateStreamEx(vbId, false, gocbcore.VbUUID(0),
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
	if f.mgr != nil && f.mgr.meh != nil {
		go f.mgr.meh.OnFeedError(SOURCE_GOCBCORE, f,
			fmt.Errorf("DeleteCollection, collection uid: %d",
				collectionId))
		return
	}

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
	// FIXME Looks like this callback isn't working.
	if f.mgr != nil && f.mgr.meh != nil {
		go f.mgr.meh.OnFeedError(SOURCE_GOCBCORE, f,
			fmt.Errorf("DeleteScope, scope uid: %d",
				scopeId))
		return
	}
}

func (f *GocbcoreDCPFeed) ModifyCollection(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, collectionId uint32, ttl uint32, streamId uint16) {
	// FIXME
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) OSOSnapshot(vbID uint16, snapshotType uint32,
	streamID uint16) {
	// FIXME
}

func (f *GocbcoreDCPFeed) SeqNoAdvanced(vbID uint16, bySeqno uint64,
	streamID uint16) {
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

func (f *GocbcoreDCPFeed) rollback(vbId uint16, entries []gocbcore.FailoverEntry) error {
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
	} else {
		atomic.AddUint64(&f.dcpStats.TotDCPRollbacks, 1)
	}

	return err
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
			f.Close()
		}
	}
}

// ----------------------------------------------------------------

// VerifySourceNotExists returns true if it's sure the bucket
// does not exist anymore (including if UUID's no longer match).
// It is however possible that the bucket is around but the index's
// source scope/collections are dropped, in which case the index
// needs to be dropped, the index UUID is passed on in this
// scenario.
func (f *GocbcoreDCPFeed) VerifySourceNotExists() (bool, string, error) {
	config := setupAgentConfig(f.name, f.bucketName, f.config.Auth)
	err := config.FromConnStr(strings.Split(f.url, ";")[0])
	if err != nil {
		return false, "", err
	}

	agent, err := setupGocbcoreAgent(config)
	if err != nil {
		if errors.Is(err, gocbcore.ErrBucketNotFound) ||
			errors.Is(err, gocbcore.ErrAuthenticationFailure) {
			// bucket not found
			return true, "", err
		}
		return false, "", err
	}
	defer agent.Close()

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return false, "", err
	}

	if snapshot.BucketUUID() != f.bucketUUID {
		// bucket UUID mismatched, so the bucket being looked
		// up must've been deleted
		return true, "", err
	}

	signal := make(chan error, 1)
	var manifest gocbcore.Manifest
	op, err := agent.GetCollectionManifest(
		gocbcore.GetCollectionManifestOptions{},
		func(res *gocbcore.GetCollectionManifestResult, er error) {
			if er == nil && res == nil {
				er = fmt.Errorf("manifest not retrieved")
			}

			if er == nil {
				er = manifest.UnmarshalJSON(res.Manifest)
			}

			signal <- er
		})

	if err != nil {
		return false, "", err
	}

	err = waitForResponse(signal, f.closeCh, op, GocbcoreStatsTimeout)
	if err != nil {
		return false, "", err
	}

	if manifest.UID == f.streamOptions.ManifestOptions.ManifestUID {
		// no manifest update => safe to assume that no scope/collection
		// have been added/dropped
		return false, "", nil
	}

	// as any collection lifecycle events affects the scope UUID, skipping
	// that for the comparisons here.
	var scopeFound bool
	for i := range manifest.Scopes {
		if manifest.Scopes[i].Name == f.scope {
			scopeFound = true
			// check if any of the source collections got deleted.
		OUTER:
			for j := range f.collectionIDs {
				for _, coll := range manifest.Scopes[i].Collections {
					if f.collections[j] == coll.Name &&
						f.collectionIDs[j] == coll.UID {
						continue OUTER
					}
				}
				return true, f.indexUUID, nil
			}
			break
		}
	}

	return !scopeFound, f.indexUUID, nil
}

func (f *GocbcoreDCPFeed) GetBucketDetails() (string, string) {
	return f.bucketName, f.bucketUUID
}

// ----------------------------------------------------------------

// The following API checks and waits for all the requested vbuckets to
// be ready to be streamed from, within the GocbcoreKVConnectTimeout.
func (f *GocbcoreDCPFeed) waitForVBsToBecomeReady() error {
	config := setupAgentConfig(f.name, f.bucketName, f.config.Auth)
	err := config.FromConnStr(strings.Split(f.url, ";")[0])
	if err != nil {
		return err
	}

	agent, err := setupGocbcoreAgent(config)
	if err != nil {
		return err
	}

	signal := make(chan error, 1)
	currentVbStates := func() (map[uint16]string, error) {
		vbStates := map[uint16]string{}
		op, err := agent.Stats(gocbcore.StatsOptions{Key: "vbucket"},
			func(resp *gocbcore.StatsResult, er error) {
				if resp == nil || er != nil {
					signal <- er
					return
				}

				stats := resp.Servers
				for _, nodeStats := range stats {
					if nodeStats.Error != nil || len(nodeStats.Stats) <= 0 {
						continue
					}

					for i, vbid := range vbucketIdStrings {
						stateVal, ok := nodeStats.Stats["vb_"+vbid]
						if !ok {
							continue
						}

						vbStates[uint16(i)] = stateVal
					}
				}

				signal <- nil
			})

		if err != nil {
			return nil, err
		}

		err = waitForResponse(signal, f.closeCh, op, GocbcoreKVConnectTimeout)
		return vbStates, err
	}

	timeoutTick := time.Tick(GocbcoreKVConnectTimeout)
OUTER:
	for {
		vbStates, err := currentVbStates()
		if err != nil {
			return err
		}

		vbsAvailable := true
		for _, i := range f.vbucketIds {
			state, exists := vbStates[i]
			if !exists || (state == "dead" || state == "pending") {
				vbsAvailable = false
				break
			}
		}

		if vbsAvailable {
			return nil
		}

		select {
		case <-f.closeCh:
			break OUTER

		case <-timeoutTick:
			break OUTER

		default:
			// continue
		}
	}

	return fmt.Errorf("couldn't determine vbs states within time")
}
