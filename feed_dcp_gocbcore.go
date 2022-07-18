//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"sort"
	"strconv"
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

var maxEndSeqno = gocbcore.SeqNo(0xffffffffffffffff)

// GocbcoreConnectTimeout and GocbcoreKVConnectTimeout are timeouts used
// by gocbcore to connect to the cluster manager and KV
var GocbcoreConnectTimeout = time.Duration(60 * time.Second)
var GocbcoreKVConnectTimeout = time.Duration(7 * time.Second)

// GocbcoreAgentSetupTimeout is the time alloted for completing setup of
// a gocbcore.Agent or a gocbcore.DCPAgent, two factors ..
//   - cluster state to be online
//   - memcached service to be ready
var GocbcoreAgentSetupTimeout = time.Duration(60 * time.Second)

// GocbcoreStatsTimeout is the time alloted to obtain a response from
// the server for a stats request.
var GocbcoreStatsTimeout = time.Duration(60 * time.Second)

// ----------------------------------------------------------------

var streamID uint64

func newStreamID() uint16 {
	// OpenStreamOptions needs streamID to be of type uint16.
	// Also, KV requires streamID to fall within a range of: 1 to 65535.
	// Here we do a mod operation, to circle around in case of an overflow.
	for {
		ret := uint16(atomic.AddUint64(&streamID, 1) % (math.MaxUint16 + 1))
		if ret != 0 {
			return ret
		}
	}
}

// ----------------------------------------------------------------

// Function overrride to set up a gocbcore.DCPAgent
// servers: single URL or multiple URLs delimited by ';'
var FetchDCPAgent func(bucketName, bucketUUID, paramsStr, servers string,
	options map[string]string) (*gocbcore.DCPAgent, error)

// Function overrride to close a gocbcore.DCPAgent
var CloseDCPAgent func(bucketName, bucketUUID string, agent *gocbcore.DCPAgent) error

// ----------------------------------------------------------------

// Map to hold a pool of gocbcore.DCPAgents for every bucket, each
// gocbcore.DCPAgent will be allowed a maximum reference count controlled
// by maxFeedsPerDCPAgent.
type gocbcoreDCPAgentMap struct {
	// mutex to serialize access to entries/refCount
	m sync.Mutex
	// map of gocbcore.DCPAgents with ref counts for bucket <name>:<uuid>
	entries map[string]map[*gocbcore.DCPAgent]int
	// stat to track number of live DCP agents (connections)
	numDCPAgents uint64
}

// Max references for a gocbcore.DCPAgent
// NOTE: Increasing this value to > 1 will cause agents to be reused for
//       multiple feeds, provided they're up against the same source.
const defaultMaxFeedsPerDCPAgent = int(1)

var dcpAgentMap *gocbcoreDCPAgentMap

func init() {
	dcpAgentMap = &gocbcoreDCPAgentMap{
		entries: make(map[string]map[*gocbcore.DCPAgent]int),
	}

	FetchDCPAgent = dcpAgentMap.fetchAgent
	CloseDCPAgent = dcpAgentMap.releaseAgent
}

func NumDCPAgents() uint64 {
	if dcpAgentMap != nil {
		return atomic.LoadUint64(&dcpAgentMap.numDCPAgents)
	}

	return 0
}

// Fetches a gocbcore.DCPAgent instance for the bucket (name:uuid),
// after increasing it's reference count.
// If no instance is available or reference count for existing instances
// is at limit, creates a new instance and stashes it with reference
// count of 1, before returning it.
func (dm *gocbcoreDCPAgentMap) fetchAgent(bucketName, bucketUUID, paramsStr,
	servers string, options map[string]string) (*gocbcore.DCPAgent, error) {
	var maxFeedsPerDCPAgent int
	if v, exists := options["maxFeedsPerDCPAgent"]; exists {
		if i, err := strconv.Atoi(v); err == nil {
			maxFeedsPerDCPAgent = i
		}
	}
	if maxFeedsPerDCPAgent <= 0 {
		maxFeedsPerDCPAgent = defaultMaxFeedsPerDCPAgent
	}

	dm.m.Lock()
	defer dm.m.Unlock()

	key := bucketName + ":" + bucketUUID
	if _, exists := dm.entries[key]; exists {
		for agent, refs := range dm.entries[key] {
			if refs < maxFeedsPerDCPAgent {
				dm.entries[key][agent]++
				log.Printf("feed_dcp_gocbcore: fetchAgent, re-using existing DCP agent"+
					" (key: %v, agent: %p, ref count: %v, number of agents for key: %v)",
					key, agent, dm.entries[key][agent], len(dm.entries[key]))
				return agent, nil
			}
		}
	} else {
		dm.entries[key] = map[*gocbcore.DCPAgent]int{}
	}

	auth, err := gocbAuth(paramsStr, options["authType"])
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocbcore: fetchAgent, gocbAuth,"+
			" bucketName: %s, err: %v", bucketName, err)
	}

	dcpConnName := fmt.Sprintf("%s%s-%x", DCPFeedPrefix, key, rand.Int31())
	config := setupDCPAgentConfig(dcpConnName, bucketName, auth,
		gocbcore.DcpAgentPriorityMed, options)

	svrs := strings.Split(servers, ";")
	if len(svrs) == 0 {
		return nil, fmt.Errorf("feed_dcp_gocbcore: fetchAgent, no servers provided")
	}

	connStr, useTLS, caProvider := setupConfigParams(bucketName, bucketUUID, svrs[0], options)
	err = config.FromConnStr(connStr)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocbcore: fetchAgent,"+
			" unable to build config from connStr: %s, err: %v", connStr, err)
	}
	config.UseTLS = useTLS
	config.TLSRootCAProvider = caProvider

	params := NewDCPFeedParams()
	err = json.Unmarshal([]byte(paramsStr), params)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocbcore: fetchAgent, params err: %v", err)
	}

	flags := memd.DcpOpenFlagProducer
	if params.IncludeXAttrs {
		flags |= memd.DcpOpenFlagIncludeXattrs
	}

	if params.NoValue {
		flags |= memd.DcpOpenFlagNoValue
	}

	agent, err := setupGocbcoreDCPAgent(config, dcpConnName, flags)
	if err != nil {
		return nil, fmt.Errorf("feed_dcp_gocbcore: fetchAgent, setup err: %w", err)
	}

	dm.entries[key][agent] = 1
	log.Printf("feed_dcp_gocbcore: fetchAgent, set up new DCP agent: `%v`"+
		" (key: %v, agent: %p, number of agents for key: %v)", dcpConnName,
		key, agent, len(dm.entries[key]))

	atomic.AddUint64(&dm.numDCPAgents, 1)

	return agent, nil
}

// Releases reference for the gocbcore.DCPAgent instance key'ed by name:uuid.
// Also, closes and removes the gocbcore DCPAgent if reference count is down
// to zero.
func (dm *gocbcoreDCPAgentMap) releaseAgent(bucketName, bucketUUID string,
	agent *gocbcore.DCPAgent) error {
	key := bucketName + ":" + bucketUUID

	dm.m.Lock()
	defer dm.m.Unlock()

	if _, exists := dm.entries[key]; !exists {
		log.Warnf("feed_dcp_gocbcore: releaseAgent, no entry for key %v", key)
		return nil
	}

	if _, exists := dm.entries[key][agent]; !exists {
		log.Warnf("feed_dcp_gocbcore: releaseAgent, DCPAgent doesn't exist"+
			" (key: %v, agent: %p)", key, agent)
	} else {
		dm.entries[key][agent]--
		if dm.entries[key][agent] > 0 {
			log.Printf("feed_dcp_gocbcore: releaseAgent, ref count decremented for"+
				" DCPagent (key: %v, agent: %p, ref count: %v, number of agents"+
				" for key: %v)",
				key, agent, dm.entries[key][agent], len(dm.entries[key]))
			return nil
		}
		// ref count of agent down to 0
		delete(dm.entries[key], agent)
		atomic.AddUint64(&dm.numDCPAgents, ^uint64(0))

		log.Printf("feed_dcp_gocbcore: releaseAgent, closing DCPAgent"+
			" (key: %v, agent: %p, number of agents for key: %v)",
			key, agent, len(dm.entries[key]))

		// close the agent only once
		go agent.Close()
	}

	if len(dm.entries[key]) == 0 {
		// no agents listed for bucket
		delete(dm.entries, key)
	}

	return nil
}

func (dm *gocbcoreDCPAgentMap) forceReconnectAgents() {
	dm.m.Lock()
	for _, agents := range dm.entries {
		for agent := range agents {
			go agent.ForceReconnect()
		}
	}
	dm.m.Unlock()
}

// ----------------------------------------------------------------

const defaultOSOBackfillMode = false
const defaultInitialBootstrapNonTLS = true

func setupDCPAgentConfig(
	name, bucketName string,
	auth gocbcore.AuthProvider,
	agentPriority gocbcore.DcpAgentPriority,
	options map[string]string) *gocbcore.DCPAgentConfig {
	useOSOBackfill := defaultOSOBackfillMode
	if options["useOSOBackfill"] == "true" {
		useOSOBackfill = true
	} else if options["useOSOBackfill"] == "false" {
		useOSOBackfill = false
	}

	initialBootstrapNonTLS := defaultInitialBootstrapNonTLS
	if options["feedInitialBootstrapNonTLS"] == "true" {
		initialBootstrapNonTLS = true
	} else if options["feedInitialBootstrapNonTLS"] == "false" {
		initialBootstrapNonTLS = false
	}

	useCollections := true
	if options["disableCollectionsSupport"] == "true" {
		useCollections = false
	}

	useStreamID := true
	if options["disableStreamIDs"] == "true" {
		useStreamID = false
	}

	return &gocbcore.DCPAgentConfig{
		UserAgent:              name,
		BucketName:             bucketName,
		Auth:                   auth,
		ConnectTimeout:         GocbcoreConnectTimeout,
		KVConnectTimeout:       GocbcoreKVConnectTimeout,
		NetworkType:            "default",
		InitialBootstrapNonTLS: initialBootstrapNonTLS,
		UseCollections:         useCollections,
		UseOSOBackfill:         useOSOBackfill,
		UseStreamID:            useStreamID,
		BackfillOrder:          gocbcore.DCPBackfillOrderRoundRobin,
		AgentPriority:          agentPriority,
		DCPBufferSize:          int(DCPFeedBufferSizeBytes),
	}
}

func setupGocbcoreDCPAgent(config *gocbcore.DCPAgentConfig,
	connName string, flags memd.DcpOpenFlag) (
	*gocbcore.DCPAgent, error) {
	agent, err := gocbcore.CreateDcpAgent(config, connName, flags)
	if err != nil {
		return nil, fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState: gocbcore.ClusterStateOnline,
		ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService},
	}

	signal := make(chan error, 1)
	_, err = agent.WaitUntilReady(time.Now().Add(GocbcoreAgentSetupTimeout),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		})

	if err == nil {
		err = <-signal
	}

	if err != nil {
		log.Warnf("feed_dcp_gocbcore: CreateDcpAgent, err: %v (close DCPAgent: %p)",
			err, agent)
		go agent.Close()
		return nil, fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	log.Printf("feed_dcp_gocbcore: CreateDcpAgent succeeded"+
		" (agent: %p, bucketName: %s, name: %s)",
		agent, config.BucketName, config.UserAgent)

	return agent, nil
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
		log.Warnf("feed_dcp_gocbcore: Request has timed out, canceling op")
		if op != nil {
			op.Cancel()
			// wait for confirmation after canceling the PendingOp
			<-signal
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
	if mgr == nil {
		return fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed," +
			" mgr is nil")
	}

	servers, _, bucketName :=
		CouchbaseParseSourceName(mgr.server, "default", sourceName)

	feed, err := newGocbcoreDCPFeed(feedName, indexName, indexUUID,
		servers, bucketName, bucketUUID, params, BasicPartitionFunc,
		dests, mgr.tagsMap != nil && !mgr.tagsMap["feed"], mgr)
	if err != nil {
		if errors.Is(err, errAgentSetupFailed) {
			// In the event of a connection error (agent setup error,
			// likely because KV wasn't ready), notify the manager
			// (asynchronously) that the feed setup has failed, so
			// the janitor can reattempt this operation.
			//
			// This needs to be asynchronous, as "kick"ing the Janitor
			// from within the JanitorLoop (this API is invoked from
			// within JanitorOnce) is prohibited - deadlock!
			go mgr.Kick(fmt.Sprintf("gocbcore-feed-start, feed: %v", feedName))
		} else if errors.Is(err, errBucketUUIDMismatched) {
			// In the event the bucket UUID changed between index
			// creation and feed setup - and if the request was for
			// the older bucket UUID, then due to the feed error drop
			// the index (asynchronously).
			log.Warnf("feed_dcp_gocbcore: DeleteIndex, indexName: %s,"+
				" indexUUID: %s, err: %v", indexName, indexUUID, err)
			go mgr.DeleteIndexEx(indexName, indexUUID)
		}

		return fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed,"+
			" could not prepare DCP feed, name: %s, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			feedName, mgr.server, bucketName, indexName, err)
	}

	err = mgr.registerFeed(feed)
	if err != nil {
		// A feed for this pindex already exists, no need to notify
		// manager on this closure
		return feed.onError(false, err)
	}

	err = feed.Start()
	if err != nil {
		return feed.onError(true,
			fmt.Errorf("feed_dcp_gocbcore: StartGocbcoreDCPFeed,"+
				" could not start feed: %s, server: %s, err: %v",
				feed.Name(), mgr.server, err))
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
// servers: single URL or multiple URLs delimited by ';'
type GocbcoreDCPFeed struct {
	name       string
	indexName  string
	indexUUID  string
	servers    string
	bucketName string
	bucketUUID string
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	stopAfter  map[string]UUIDSeq
	mgr        *Manager

	agent *gocbcore.DCPAgent

	scope       string
	collections []string

	manifestUID   uint64
	scopeID       uint32
	collectionIDs []uint32

	streamOptions gocbcore.OpenStreamOptions

	vbucketIds        []uint16
	lastReceivedSeqno []uint64
	currVBs           []*vbucketState

	dcpStats *gocbcoreDCPFeedStats

	m                 sync.Mutex
	remaining         sync.WaitGroup
	closed            bool
	shutdownInitiated bool
	active            map[uint16]bool
	stats             *DestStats
	stopAfterReached  map[string]bool // May be nil.

	closeCh chan struct{}
}

type gocbcoreDCPFeedStats struct {
	// TODO: Add more stats
	TotDCPStreamReqs uint64
	TotDCPStreamEnds uint64
	TotDCPRollbacks  uint64

	TotDCPSnapshotMarkers   uint64
	TotDCPMutations         uint64
	TotDCPDeletions         uint64
	TotDCPSeqNoAdvanceds    uint64
	TotDCPCreateCollections uint64
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

func newGocbcoreDCPFeed(name, indexName, indexUUID, servers,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*GocbcoreDCPFeed, error) {
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
		servers:    servers,
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

	if err = feed.setupStreamOptions(paramsStr, mgr.Options()); err != nil {
		return nil, fmt.Errorf("newGocbcoreDCPFeed:"+
			" error in setting up feed's stream options, err: %w", err)
	}

	feed.agent, err = FetchDCPAgent(feed.bucketName, feed.bucketUUID,
		paramsStr, servers, mgr.Options())
	if err != nil {
		return nil, fmt.Errorf("newGocbcoreDCPFeed DCPAgent, err: %w", err)
	}

	log.Printf("feed_dcp_gocbcore: newGocbcoreDCPFeed, name: %s, indexName: %s,"+
		" server: %v, bucketName: %s, bucketUUID: %s",
		name, indexName, feed.servers, feed.bucketName, feed.bucketUUID)

	return feed, nil
}

func (f *GocbcoreDCPFeed) setupStreamOptions(paramsStr string,
	options map[string]string) error {
	svrs := strings.Split(f.servers, ";")
	if len(svrs) == 0 {
		return fmt.Errorf("no servers provided")
	}

	agent, _, err := statsAgentsMap.obtainAgents(f.bucketName, f.bucketUUID,
		paramsStr, svrs[0], options)
	if err != nil {
		return fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	// the sourceUUID setting in the index definition is optional,
	// so make sure the feed's bucketUUID is set in case it wasn't
	// provided, and validated otherwise
	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return err
	}

	bucketUUID := snapshot.BucketUUID()
	if len(f.bucketUUID) == 0 {
		f.bucketUUID = bucketUUID
	} else if f.bucketUUID != bucketUUID {
		return fmt.Errorf("%w, bucket: [%s, %s], request: %s",
			errBucketUUIDMismatched, f.bucketName, bucketUUID, f.bucketUUID)
	}

	f.streamOptions = gocbcore.OpenStreamOptions{}
	if !agent.HasCollectionsSupport() {
		// No support for collections
		return nil
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

	f.manifestUID = manifest.UID

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

	f.streamOptions.StreamOptions = &gocbcore.OpenStreamStreamOptions{
		StreamID: newStreamID(),
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
						if res.ManifestID != f.manifestUID {
							er = fmt.Errorf("manifestID mismatch, %v != %v",
								res.ManifestID, f.manifestUID)
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
		" manifestUID: %v, streamOptions: {FilterOptions: %+v, StreamOptions: %+v},"+
		" vbuckets: %v", f.Name(), len(f.vbucketIds), f.manifestUID,
		f.streamOptions.FilterOptions, f.streamOptions.StreamOptions, f.vbucketIds)

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
	if f.close() {
		log.Printf("feed_dcp_gocbcore: Close, name: %s", f.Name())
	}
	return nil
}

func (f *GocbcoreDCPFeed) NotifyMgrOnClose() {
	if f.close() {
		log.Printf("feed_dcp_gocbcore: Close, name: %s, notify manager",
			f.Name())

		go f.mgr.Kick(fmt.Sprintf("gocbcore-feed, feed: %v", f.Name()))
	}
}

func (f *GocbcoreDCPFeed) close() bool {
	f.m.Lock()
	if f.closed {
		f.m.Unlock()
		return false
	}
	f.closed = true
	f.closeAllStreamsLOCKED()
	CloseDCPAgent(f.bucketName, f.bucketUUID, f.agent)
	f.m.Unlock()

	f.mgr.unregisterFeed(f.Name())

	close(f.closeCh)
	f.wait()

	return true
}

func (f *GocbcoreDCPFeed) getCloseStreamOptions() gocbcore.CloseStreamOptions {
	rv := gocbcore.CloseStreamOptions{}
	if f.agent.HasCollectionsSupport() {
		rv.StreamOptions = &gocbcore.CloseStreamStreamOptions{
			StreamID: f.streamOptions.StreamOptions.StreamID,
		}
	}

	return rv
}

// This will call close on all streams on feed closure. Note that
// streams would then see an END message with the reason: "closed by
// consumer".
func (f *GocbcoreDCPFeed) closeAllStreamsLOCKED() {
	closeStreamOptions := f.getCloseStreamOptions()

	log.Printf("feed_dcp_gocbcore: name: %s, streamOptions: %+v,"+
		" close any open streams over vbuckets: %v",
		f.Name(), closeStreamOptions.StreamOptions, f.vbucketIds)

	for _, vbId := range f.vbucketIds {
		if f.active[vbId] {
			f.agent.CloseStream(vbId, closeStreamOptions, func(err error) {})
			f.active[vbId] = false
			f.remaining.Done()
		}
	}
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

func (f *GocbcoreDCPFeed) lastVbUUIDSeqFromFailOverLog(vbId uint16) (
	uint64, uint64, error) {
	vbMetaData, lastSeq, err := f.getMetaData(vbId)
	if err != nil {
		return 0, 0, err
	}

	var vbuuid uint64
	if len(vbMetaData.FailOverLog) > 0 {
		vbuuid = vbMetaData.FailOverLog[0][0]
	}

	return vbuuid, lastSeq, nil
}

func (f *GocbcoreDCPFeed) initiateStream(vbId uint16) error {
	vbuuid, lastSeq, err := f.lastVbUUIDSeqFromFailOverLog(vbId)
	if err != nil {
		return err
	}

	go f.initiateStreamEx(vbId, true, gocbcore.VbUUID(vbuuid),
		gocbcore.SeqNo(lastSeq), maxEndSeqno)

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
	log.Debugf("feed_dcp_gocbcore: [%s] Initiating DCP stream request for vb: %v,"+
		" vbUUID: %v, seqStart: %v, seqEnd: %v, manifestUID: %v,"+
		" streamOptions: {%+v, %+v}", f.Name(), vbId, vbuuid, seqStart, seqEnd,
		f.manifestUID, f.streamOptions.FilterOptions, f.streamOptions.StreamOptions)
	op, err := f.agent.OpenStream(vbId, memd.DcpStreamAddFlagStrictVBUUID,
		vbuuid, seqStart, seqEnd, snapStart, snapStart, f, f.streamOptions,
		func(entries []gocbcore.FailoverEntry, er error) {
			if errors.Is(er, gocbcore.ErrShutdown) ||
				errors.Is(er, gocbcore.ErrSocketClosed) ||
				errors.Is(er, gocbcore.ErrScopeNotFound) ||
				errors.Is(er, gocbcore.ErrCollectionNotFound) {
				f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s] OpenStream,"+
					" vb: %v, streamOptions: %+v, err: %v", f.Name(),
					vbId, f.streamOptions.StreamOptions, er))
				er = nil
			} else if errors.Is(er, gocbcore.ErrMemdRollback) {
				log.Printf("feed_dcp_gocbcore: [%s] OpenStream received rollback,"+
					" for vb: %v, streamOptions: %+v, seqno requested: %v", f.Name(),
					vbId, f.streamOptions.StreamOptions, seqStart)
				f.complete(vbId)
				go f.rollback(vbId, entries)
				// rollback will handle this feed closure and setting up of a new feed
				er = nil
			} else if errors.Is(er, gocbcore.ErrRequestCanceled) {
				// request was canceled by FTS, catch error and re-initiate stream request
				log.Warnf("feed_dcp_gocbcore: [%s] OpenStream for vb: %v, streamOptions: %+v"+
					" was canceled, (timeout) will re-initiate the stream request",
					f.Name(), vbId, f.streamOptions.StreamOptions)
			} else if errors.Is(er, gocbcore.ErrForcedReconnect) {
				// request was canceled by GOCBCORE, catch error and re-initate stream request
				log.Warnf("feed_dcp_gocbcore: [%s] OpenStream for vb: %v, streamOptions: %+v"+
					"failed with err: %v, reconnecting ...", f.Name(),
					vbId, f.streamOptions.StreamOptions, er)
			} else if er != nil {
				// unidentified error
				log.Errorf("feed_dcp_gocbcore: [%s] OpenStream received error for vb: %v, "+
					" streamOptions: %+v, err: %v", f.Name(), vbId,
					f.streamOptions.StreamOptions, er)
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

	if err != nil {
		f.onError(true, fmt.Errorf("OpenStream error for vb: %v, err: %v", vbId, err))
		return
	}

	err = waitForResponse(signal, f.closeCh, op, GocbcoreConnectTimeout)
	if err != nil {
		if errors.Is(err, gocbcore.ErrTimeout) || errors.Is(err, gocbcore.ErrForcedReconnect) {
			// Verify source exists before closing and re-initiating stream request(s).
			if gone, _, _ := f.VerifySourceNotExists(); gone {
				f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s], OpenStream,"+
					" source is gone, vb: %v, err: %v", f.Name(), vbId, err))
				return
			}

			// Send a close-stream request for the vbucket to KV to make
			// certain that the earlier stream isn't still lingering around,
			// before re-initiating a new stream request.
			wait := make(chan error, 1)
			op, err = f.agent.CloseStream(vbId, f.getCloseStreamOptions(), func(er error) {
				wait <- er
			})

			if err == nil {
				err = waitForResponse(wait, f.closeCh, op, GocbcoreConnectTimeout)
				if err == nil || errors.Is(err, gocbcore.ErrDocumentNotFound) ||
					errors.Is(err, gocbcore.ErrDCPStreamIDInvalid) {
					// Send a new open-stream request for the vbucket only if the
					// error returned by the close-stream request on the earlier
					// stream is nil or key-not-found - vbucket/streamId - in which
					// case it's safe to proceed with a repeat stream request.
					go f.initiateStreamEx(vbId, false, vbuuid, seqStart, seqEnd)
					return
				}

				log.Warnf("feed_dcp_gocbcore: [%s] CloseStream for vb: %v,"+
					" streamOptions: %+v, err: %v", f.Name(), vbId,
					f.streamOptions.StreamOptions, err)
			}
		}

		// Error cannot be handled without shutting down feed, notify manager
		f.onError(true,
			fmt.Errorf("OpenStream, error waiting for vb: %v, err: %v", vbId, err))
	}
}

// ----------------------------------------------------------------

// initiateShutdown is to be invoked when the error received on
// the feed is either ErrShutdown or ErrSocketClosed.
func (f *GocbcoreDCPFeed) initiateShutdown(err error) {
	f.m.Lock()
	if f.shutdownInitiated {
		f.m.Unlock()
		return
	}
	f.shutdownInitiated = true
	f.m.Unlock()

	if f.mgr.meh != nil {
		f.mgr.meh.OnFeedError(SOURCE_GOCBCORE, f, err)
	}
}

// onError is to be invoked in case of errors encountered while
// processing DCP messages.
func (f *GocbcoreDCPFeed) onError(notifyMgr bool, err error) error {
	log.Debugf("feed_dcp_gocbcore: onError, name: %s,"+
		" bucketName: %s, bucketUUID: %s, err: %v",
		f.Name(), f.bucketName, f.bucketUUID, err)

	if notifyMgr {
		f.NotifyMgrOnClose()
	} else {
		f.Close()
	}

	return err
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, streamId uint16, snapshotType gocbcore.SnapshotState) {
	if f.currVBs[vbId] == nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] SnapshotMarker, invalid vb",
			vbId, streamId))
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
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] SnapshotMarker, err: %v",
			vbId, streamId, err))
		return
	}

	atomic.AddUint64(&f.dcpStats.TotDCPSnapshotMarkers, 1)
}

func (f *GocbcoreDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	collectionId uint32, streamId uint16, key, value []byte) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] Mutation, %v",
			vbId, streamId, err))
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
				Expiry:   expiry,
				Flags:    flags,
				Datatype: datatype,
			}
			if f.agent.HasCollectionsSupport() {
				extras.ScopeId = f.streamOptions.FilterOptions.ScopeID
				extras.CollectionId = collectionId
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
			return fmt.Errorf("name: %s, partition: %s, key: %v, seq: %d, err: %v",
				f.Name(), partition, log.Tag(log.UserData, key), seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerDataUpdate)

	if err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] Mutation, err: %v",
			vbId, streamId, err))
		return
	}

	f.lastReceivedSeqno[vbId] = seqNo

	atomic.AddUint64(&f.dcpStats.TotDCPMutations, 1)
}

func (f *GocbcoreDCPFeed) Deletion(seqNo, revNo uint64, deleteTime uint32,
	cas uint64, datatype uint8, vbId uint16, collectionId uint32, streamId uint16,
	key, value []byte) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] Deletion, %v",
			vbId, streamId, err))
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
				Datatype: datatype,
				Value:    value,
			}
			if f.agent.HasCollectionsSupport() {
				extras.ScopeId = f.streamOptions.FilterOptions.ScopeID
				extras.CollectionId = collectionId
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
			return fmt.Errorf("name: %s, partition: %s, key: %v, seq: %d, err: %v",
				f.Name(), partition, log.Tag(log.UserData, key), seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerDataDelete)

	if err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] Deletion, err: %v",
			vbId, streamId, err))
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
		f.complete(vbId)
		log.Printf("feed_dcp_gocbcore: [%s] DCP stream [%v] ended for vb: %v,"+
			" last seq: %v", f.Name(), streamId, vbId, lastReceivedSeqno)
	} else if errors.Is(err, gocbcore.ErrShutdown) ||
		errors.Is(err, gocbcore.ErrSocketClosed) ||
		errors.Is(err, gocbcore.ErrDCPStreamFilterEmpty) {
		f.complete(vbId)
		f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s], End, vb: %v, err: %v",
			f.Name(), vbId, err))
	} else if errors.Is(err, gocbcore.ErrDCPStreamStateChanged) ||
		errors.Is(err, gocbcore.ErrDCPStreamTooSlow) ||
		errors.Is(err, gocbcore.ErrDCPStreamDisconnected) ||
		errors.Is(err, gocbcore.ErrForcedReconnect) {
		log.Printf("feed_dcp_gocbcore: [%s] DCP stream [%v] for vb: %v, closed due to"+
			" `%s`, last seq: %v, reconnecting ...",
			f.Name(), streamId, vbId, err.Error(), lastReceivedSeqno)
		go func(vb uint16) {
			vbuuid, lastSeq, _ := f.lastVbUUIDSeqFromFailOverLog(vb)
			f.initiateStreamEx(vb, false, gocbcore.VbUUID(vbuuid),
				gocbcore.SeqNo(lastSeq), maxEndSeqno)
		}(vbId)
	} else if errors.Is(err, gocbcore.ErrDCPStreamClosed) {
		f.complete(vbId)
		log.Debugf("feed_dcp_gocbcore: [%s] DCP stream [%v] for vb: %v,"+
			" closed by consumer", f.Name(), streamId, vbId)
	} else {
		f.complete(vbId)
		log.Warnf("feed_dcp_gocbcore: [%s] DCP stream [%v] closed for vb: %v,"+
			" last seq: %v, err: `%s`",
			f.Name(), streamId, vbId, lastReceivedSeqno, err.Error())
	}
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) CreateCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32,
	ttl uint32, streamId uint16, key []byte) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] CreateCollection, %v",
			vbId, streamId, err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destColl, ok := dest.(DestCollection); ok {
			// A CreateCollection message for a collection is received only
			// if the feed has subscribed to the collection, so update seqno
			// received for the feed.
			err = destColl.CreateCollection(partition, manifestUid, scopeId,
				collectionId, seqNo)
		}

		if err != nil {
			return fmt.Errorf("name: %s, partition: %s,"+
				" seq: %d, err: %v", f.Name(), partition, seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerCreateCollection)

	if err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] CreateCollection, err: %v",
			vbId, streamId, err))
		return
	}

	f.lastReceivedSeqno[vbId] = seqNo

	atomic.AddUint64(&f.dcpStats.TotDCPCreateCollections, 1)
}

func (f *GocbcoreDCPFeed) DeleteCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32,
	streamId uint16) {
	// initiate a feed closure on collection delete
	f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s] DeleteCollection,"+
		" vb: %v, stream: %v, collection uid: %d",
		f.Name(), vbId, streamId, collectionId))
}

func (f *GocbcoreDCPFeed) FlushCollection(seqNo uint64, version uint8,
	vbId uint16, manifestUid uint64, collectionId uint32) {
	// FIXME: not supported for CC
}

func (f *GocbcoreDCPFeed) CreateScope(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, scopeId uint32, streamId uint16, key []byte) {
	// Don't expect to see a CreateScope message as indexes cannot span scopes
}

func (f *GocbcoreDCPFeed) DeleteScope(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, scopeId uint32, streamId uint16) {
	// initiate a feed closure on scope delete
	f.initiateShutdown(fmt.Errorf("feed_dcp_gocbcore: [%s] DeleteScope,"+
		" vb: %v, stream: %v, scope uid: %d",
		f.Name(), vbId, streamId, scopeId))
}

func (f *GocbcoreDCPFeed) ModifyCollection(seqNo uint64, version uint8, vbId uint16,
	manifestUid uint64, collectionId uint32, ttl uint32, streamId uint16) {
	// FIXME: not supported for CC
}

// ----------------------------------------------------------------

func (f *GocbcoreDCPFeed) OSOSnapshot(vbId uint16, snapshotType uint32,
	streamId uint16) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] OSOSnapshot, %v",
			vbId, streamId, err))
		return
	}

	partition, dest, err :=
		VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
	if err == nil && !f.checkStopAfter(partition) {
		if destColl, ok := dest.(DestCollection); ok {
			err = destColl.OSOSnapshot(partition, snapshotType)
		}
	}

	if err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] OSOSnapshot, err: %v",
			vbId, streamId, err))
		return
	}
}

func (f *GocbcoreDCPFeed) SeqNoAdvanced(vbId uint16, seqNo uint64,
	streamId uint16) {
	if err := f.checkAndUpdateVBucketState(vbId); err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] SeqNoAdvanced, %v",
			vbId, streamId, err))
		return
	}

	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destColl, ok := dest.(DestCollection); ok {
			// A SeqNoAdvanced message is received when the feed has subscribed
			// to collection(s), and it is to be interpreted as a SnapshotEnd
			// message, indicating that the feed should not expect any more
			// sequence numbers up until this.
			err = destColl.SeqNoAdvanced(partition, seqNo)
		}

		if err != nil {
			return fmt.Errorf("name: %s, partition: %s,"+
				" seq: %d, err: %v", f.Name(), partition, seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerSeqNoAdvanced)

	if err != nil {
		f.onError(true, fmt.Errorf("[vb:%v stream:%v] SeqNoAdvanced, err: %v",
			vbId, streamId, err))
		return
	}

	f.lastReceivedSeqno[vbId] = seqNo

	atomic.AddUint64(&f.dcpStats.TotDCPSeqNoAdvanceds, 1)
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
		log.Warnf("feed_dcp_gocbcore: [%s] Rollback to seqno: %v, vbuuid: %v for"+
			" vb: %v, failed with err: %v",
			f.Name(), rollbackSeqno, rollbackVbuuid, vbId, err)
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

// This implementation of GetPoolsDefaultForBucket works with CBAUTH only;
// For all other authtypes, the application will have to override this function.
var GetPoolsDefaultForBucket = func(server, bucket string, scopes bool) ([]byte, error) {
	if len(bucket) == 0 {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket: bucket not provided")
	}

	url := server + "/pools/default/buckets/" + bucket
	if scopes {
		url += "/scopes"
	}

	u, err := CBAuthURL(url)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}

	resp, err := HttpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}

	if len(respBuf) == 0 {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, empty response for url: %v", url)
	}

	return respBuf, nil
}

// VerifySourceNotExists returns true if it's sure the bucket
// does not exist anymore (including if UUID's no longer match).
// It is however possible that the bucket is around but the index's
// source scope/collections are dropped, in which case the index
// needs to be dropped, the index UUID is passed on in this
// scenario.
func (f *GocbcoreDCPFeed) VerifySourceNotExists() (bool, string, error) {
	resp, err := GetPoolsDefaultForBucket(f.mgr.Server(), f.bucketName, false)
	if err != nil {
		return false, "", err
	}

	rv := struct {
		Name string `json:"name"`
		UUID string `json:"uuid"`
	}{}

	err = json.Unmarshal(resp, &rv)
	if err != nil || f.bucketUUID != rv.UUID {
		// safe to assume that bucket is deleted
		// - respBuf carries: `Requested resource not found.`
		// - bucketUUID didn't match, so the bucket being looked up
		//   must've been deleted
		return true, "", err
	}

	resp, err = GetPoolsDefaultForBucket(f.mgr.Server(), f.bucketName, true)
	if err != nil {
		return false, "", err
	}

	var manifest gocbcore.Manifest
	if err = manifest.UnmarshalJSON(resp); err != nil {
		return false, "", err
	}

	if manifest.UID == f.manifestUID {
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
