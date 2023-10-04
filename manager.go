//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
	"github.com/couchbase/tools-common/cloud/v5/objstore/objcli"
)

// A Manager represents a runtime node in a cluster.
//
// Although often used like a singleton, multiple Manager instances
// can be instantiated in a process to simulate a cluster of nodes.
//
// A Manager has two related child, actor-like goroutines:
// - planner
// - janitor
//
// A planner splits index definitions into index partitions (pindexes)
// and assigns those pindexes to nodes.  A planner wakes up and runs
// whenever the index definitions change or the set of nodes changes
// (which are both read from the Cfg system).  A planner stores the
// latest plans into the Cfg system.
//
// A janitor running on each node maintains runtime PIndex and Feed
// instances, creating, deleting & hooking them up as necessary to try
// to match to latest plans from the planner.  A janitor wakes up and
// runs whenever it sees that latest plans in the Cfg have changed.
//
// As part of server: multiple urls permitted with ';' delimiter.
type Manager struct {
	startTime time.Time
	version   string // See VERSION.
	cfg       Cfg
	uuid      string          // Unique to every Manager instance.
	tags      []string        // The tags at Manager start.
	tagsMap   map[string]bool // The tags at Manager start, performance opt.
	container string          // '/' separated containment path (optional).
	weight    int
	extras    string
	bindHttp  string
	dataDir   string
	server    string // The default datasource that will be indexed.
	stopCh    chan struct{}
	plannerCh chan *workReq // Kicks planner that there's more work.
	janitorCh chan *workReq // Kicks janitor that there's more work.
	meh       ManagerEventHandlers

	stats ManagerStats

	m                      sync.RWMutex       // Protects the fields that follow.
	pindexes               map[string]*PIndex // Key is PIndex.Name().
	bootingPIndexes        map[string]bool    // booting flag
	lastNodeDefs           map[string]*NodeDefs
	lastIndexDefs          *IndexDefs
	lastIndexDefsByName    map[string]*IndexDef
	lastPlanPIndexes       *PlanPIndexes
	lastPlanPIndexesByName map[string][]*PlanPIndex
	coveringCache          map[CoveringPIndexesSpec]*CoveringPIndexes

	feedsMutex sync.RWMutex
	feeds      map[string]Feed // Key is Feed.Name().

	optionsMutex sync.RWMutex
	options      map[string]string

	eventsMutex sync.RWMutex
	events      *list.List

	peh PlannerEventHandlerCallback

	stablePlanPIndexesMutex sync.RWMutex // Protects the local stable plan access.

	// The below fields are related to hibernationa and optional.
	objStoreClient           objcli.Client
	hibernationCtx           context.Context
	hibernationCancel        context.CancelFunc
	bucketInHibernationMutex sync.RWMutex
	bucketInHibernation      string
	bucketScopeInfoTracker   *BucketScopeInfoTracker
}

func (mgr *Manager) GetHibernationContext() (context.Context, context.CancelFunc) {
	return mgr.hibernationCtx, mgr.hibernationCancel
}

func (mgr *Manager) setHibernationContext(rateLimit uint64) {
	mgr.hibernationCtx = context.Background()
	mgr.hibernationCtx, mgr.hibernationCancel = context.WithCancel(mgr.hibernationCtx)
	mgr.hibernationCtx = context.WithValue(mgr.hibernationCtx, "rateLimit", rateLimit)
}

func (mgr *Manager) GetObjStoreClient() objcli.Client {
	return mgr.objStoreClient
}

func (mgr *Manager) setObjStoreClient(client objcli.Client) {
	mgr.objStoreClient = client
}

// This is the key for the manager option to track which bucket
// is currently being tracked for hibernation.
// If set to "$", it indicates that currently, no bucket is
// being hibernated/tracked for hibernation.
const bucketInHibernationKey = "bucketInHibernation"
const NoBucketInHibernation = "$"

func (mgr *Manager) MarkBucketForHibernation(bucketTaskKey string) error {
	if mgr.GetOption(bucketInHibernationKey) == bucketTaskKey {
		return nil
	}

	return mgr.SetOption(bucketInHibernationKey, bucketTaskKey, true)
}

func (mgr *Manager) ResetBucketTrackedForHibernation() error {
	if mgr.GetOption(bucketInHibernationKey) == NoBucketInHibernation {
		return nil
	}

	return mgr.SetOption(bucketInHibernationKey, NoBucketInHibernation, true)
}

func (mgr *Manager) IsBucketBeingHibernated(bucket string) bool {
	if bucket == "" {
		return false
	}

	mgr.bucketInHibernationMutex.RLock()
	bucketInHibernation := mgr.bucketInHibernation
	mgr.bucketInHibernationMutex.RUnlock()

	return bucketInHibernation == bucket
}

func (mgr *Manager) RegisterHibernationBucketTracker(bucket string) {
	mgr.bucketInHibernationMutex.Lock()
	defer mgr.bucketInHibernationMutex.Unlock()

	if mgr.bucketInHibernation == bucket {
		return
	}

	mgr.bucketInHibernation = bucket

	atomic.AddUint64(&mgr.stats.TotRegisterHibernationBucketTracker, 1)
}

func (mgr *Manager) UnregisterBucketTracker() {
	mgr.bucketInHibernationMutex.Lock()
	defer mgr.bucketInHibernationMutex.Unlock()

	if mgr.bucketInHibernation == NoBucketInHibernation {
		return
	}

	mgr.bucketInHibernation = NoBucketInHibernation

	atomic.AddUint64(&mgr.stats.TotUnregisterHibernationBucketTracker, 1)
}

// Sets options in manager and optionally persists them as cluster options
// if cfgSet is true
func (mgr *Manager) SetOption(key, value string, cfgSet bool) error {
	mgr.optionsMutex.Lock()
	defer mgr.optionsMutex.Unlock()

	mgr.options[key] = value

	if !cfgSet {
		return nil
	}

	mo := ClusterOptions{}
	oval := reflect.ValueOf(&mo)
	for k, v := range mgr.options {
		fName := strings.ToUpper(string(k[0])) + k[1:]
		f := oval.Elem().FieldByName(fName)
		if f.IsValid() {
			f.SetString(v)
		}
	}
	_, err := CfgSetClusterOptions(mgr.cfg, &mo, 0)
	if err != nil {
		return err
	}
	atomic.AddUint64(&mgr.stats.TotSetOptions, 1)
	return nil
}

// PlannerEventHandlerCallback is an optional event
// callback for an external planner that wish to receive
// direct notifications of custom cfg events (bypassing
// the metakv/cfg event subscription model) directly
// from the local manager instance.
// Currently  the manager only provides index definition
// related events alone in this callbacks.
type PlannerEventHandlerCallback func(*CfgEvent)

// RegisterPlannerEventHandlerCallback lets an external
// planner register for any custom index definition change
// notifications from the manager instance.
func (mgr *Manager) RegisterPlannerEventHandlerCallback(
	ev PlannerEventHandlerCallback) {
	mgr.peh = ev
}

// ManagerStats represents the stats/metrics tracked by a Manager
// instance.
type ManagerStats struct {
	TotKick uint64

	TotSetOptions uint64

	TotRegisterFeed     uint64
	TotUnregisterFeed   uint64
	TotRegisterPIndex   uint64
	TotUnregisterPIndex uint64

	TotLoadDataDir uint64

	TotSaveNodeDef       uint64
	TotSaveNodeDefNil    uint64
	TotSaveNodeDefGetErr uint64
	TotSaveNodeDefSetErr uint64
	TotSaveNodeDefRetry  uint64
	TotSaveNodeDefSame   uint64
	TotSaveNodeDefOk     uint64

	TotCreateIndex    uint64
	TotCreateIndexOk  uint64
	TotDeleteIndex    uint64
	TotDeleteIndexOk  uint64
	TotIndexControl   uint64
	TotIndexControlOk uint64

	TotDeleteIndexBySource    uint64
	TotDeleteIndexBySourceErr uint64
	TotDeleteIndexBySourceOk  uint64

	TotPlannerOpStart           uint64
	TotPlannerOpRes             uint64
	TotPlannerOpErr             uint64
	TotPlannerOpDone            uint64
	TotPlannerNOOP              uint64
	TotPlannerNOOPOk            uint64
	TotPlannerKick              uint64
	TotPlannerKickStart         uint64
	TotPlannerKickChanged       uint64
	TotPlannerKickErr           uint64
	TotPlannerKickOk            uint64
	TotPlannerUnknownErr        uint64
	TotPlannerSubscriptionEvent uint64
	TotPlannerStop              uint64

	TotJanitorOpStart           uint64
	TotJanitorOpRes             uint64
	TotJanitorOpErr             uint64
	TotJanitorOpDone            uint64
	TotJanitorNOOP              uint64
	TotJanitorNOOPOk            uint64
	TotJanitorKick              uint64
	TotJanitorKickStart         uint64
	TotJanitorKickErr           uint64
	TotJanitorKickOk            uint64
	TotJanitorClosePIndex       uint64
	TotJanitorRemovePIndex      uint64
	TotJanitorRestartPIndex     uint64
	TotJanitorUnknownErr        uint64
	TotJanitorSubscriptionEvent uint64
	TotJanitorStop              uint64

	TotRefreshLastNodeDefs     uint64
	TotRefreshLastIndexDefs    uint64
	TotRefreshLastPlanPIndexes uint64

	TotSlowConfigAccess uint64

	TotRegisterHibernationBucketTracker   uint64
	TotUnregisterHibernationBucketTracker uint64
}

// ClusterOptions stores the configurable cluster-level
// manager options.
// Follow strict naming guideline for any option additions.
// Every field in ClusterOptions should have the same exact
// name as is in the original manager options cache map with
// the exception of being exported field names.
type ClusterOptions struct {
	BleveMaxResultWindow               string `json:"bleveMaxResultWindow"`
	BleveMaxClauseCount                string `json:"bleveMaxClauseCount"`
	FeedAllotment                      string `json:"feedAllotment"`
	FtsMemoryQuota                     string `json:"ftsMemoryQuota"`
	ConcurrentMergeLimit               string `json:"concurrentMergeLimit"`
	MaxReplicasAllowed                 string `json:"maxReplicasAllowed"`
	SlowQueryLogTimeout                string `json:"slowQueryLogTimeout"`
	EnableVerboseLogging               string `json:"enableVerboseLogging"`
	MaxFeedsPerDCPAgent                string `json:"maxFeedsPerDCPAgent"`
	KVConnectionBufferSize             string `json:"kvConnectionBufferSize"`
	MaxConcurrentPartitionMovesPerNode string `json:"maxConcurrentPartitionMovesPerNode"`
	UseOSOBackfill                     string `json:"useOSOBackfill"`
	SeqChecksTimeoutInSec              string `json:"seqChecksTimeoutInSec"`
	EnableReplicaCatchupOnRebalance    string `json:"enableReplicaCatchupOnRebalance"`
	NumSeqCheckRetriesDuringRebalance  string `json:"numSeqCheckRetriesDuringRebalance"`
	DisableFileTransferRebalance       string `json:"disableFileTransferRebalance"`
	EnablePartitionNodeStickiness      string `json:"enablePartitionNodeStickiness"`
	DisableGeoPointSpatialPlugin       string `json:"disableGeoPointSpatialPlugin"`
	MaxIndexCountPerSource             string `json:"maxIndexCountPerSource"`
	MinBackoffTimeForBatchLimitingMS   string `json:"minBackoffTimeForBatchLimitingMS"`
	MaxBackoffTimeForBatchLimitingMS   string `json:"maxBackoffTimeForBatchLimitingMS"`
	DisableRegulatorControl            string `json:"disableRegulatorControl"`
	ResourceUtilizationHighWaterMark   string `json:"resourceUtilizationHighWaterMark"`
	ResourceUtilizationLowWaterMark    string `json:"resourceUtilizationLowWaterMark"`
	ResourceUnderUtilizationWaterMark  string `json:"resourceUnderUtilizationWaterMark"`
	BucketInHibernation                string `json:"bucketInHibernation"`
	HibernationSourcePartitions        string `json:"hibernationSourcePartitions"`
}

var ErrNoIndexDefs = errors.New("no index definitions found")

// MANAGER_MAX_EVENTS limits the number of events tracked by a Manager
// for diagnosis/debugging.
const MANAGER_MAX_EVENTS = 10

const MANAGER_CLUSTER_OPTIONS_KEY = "manager_cluster_options_key"

// ManagerEventHandlers represents the callback interface where an
// application can receive important event callbacks from a Manager.
type ManagerEventHandlers interface {
	OnRegisterPIndex(pindex *PIndex)
	OnUnregisterPIndex(pindex *PIndex)
	OnFeedError(srcType string, r Feed, err error)
	OnRefreshManagerOptions(options map[string]string)
}

// NewManager returns a new, ready-to-be-started Manager instance.
func NewManager(version string, cfg Cfg, uuid string, tags []string,
	container string, weight int, extras, bindHttp, dataDir, server string,
	meh ManagerEventHandlers) *Manager {
	return NewManagerEx(version, cfg, uuid, tags, container, weight, extras,
		bindHttp, dataDir, server, meh, nil)
}

// NewManagerEx returns a new, ready-to-be-started Manager instance,
// with additional options.
func NewManagerEx(version string, cfg Cfg, uuid string, tags []string,
	container string, weight int, extras, bindHttp, dataDir, server string,
	meh ManagerEventHandlers, options map[string]string) *Manager {
	if options == nil {
		options = map[string]string{}
	}

	return &Manager{
		startTime:              time.Now(),
		version:                version,
		cfg:                    cfg,
		uuid:                   uuid,
		tags:                   tags,
		tagsMap:                StringsToMap(tags),
		container:              container,
		weight:                 weight,
		extras:                 extras,
		bindHttp:               bindHttp, // TODO: Need FQDN:port instead of ":8095".
		dataDir:                dataDir,
		server:                 server,
		stopCh:                 make(chan struct{}),
		options:                options,
		feeds:                  make(map[string]Feed),
		pindexes:               make(map[string]*PIndex),
		bootingPIndexes:        make(map[string]bool),
		plannerCh:              make(chan *workReq),
		janitorCh:              make(chan *workReq),
		meh:                    meh,
		events:                 list.New(),
		bucketScopeInfoTracker: initBucketScopeInfoTracker(server),

		lastNodeDefs: make(map[string]*NodeDefs),
	}
}

func (mgr *Manager) Stop() {
	close(mgr.stopCh)
}

// Start will start and register a Manager instance with its
// configured Cfg system, based on the register parameter.  See
// Manager.Register().
func (mgr *Manager) Start(register string) error {
	err := mgr.Register(register)
	if err != nil {
		return err
	}

	if mgr.tagsMap == nil || mgr.tagsMap["pindex"] {
		mldd := mgr.options["managerLoadDataDir"]
		if mldd == "" || mldd == "true" {
			err := mgr.LoadDataDir()
			if err != nil {
				return err
			}
		}
	}

	if mgr.tagsMap == nil || mgr.tagsMap["planner"] {
		go mgr.PlannerLoop()
		go mgr.PlannerKick("start")
	}

	if mgr.tagsMap == nil ||
		(mgr.tagsMap["pindex"] && mgr.tagsMap["janitor"]) {
		go mgr.JanitorLoop()
		go mgr.JanitorKick("start")
	}

	return mgr.StartCfg()
}

// StartCfg will start Cfg subscriptions.
func (mgr *Manager) StartCfg() error {
	if mgr.cfg != nil { // TODO: Need err handling for Cfg subscriptions.

		// refresh the cluster options.
		mgr.RefreshOptions()

		go func() {
			ei := make(chan CfgEvent)
			mgr.cfg.Subscribe(INDEX_DEFS_KEY, ei)
			mgr.cfg.Subscribe(MANAGER_CLUSTER_OPTIONS_KEY, ei)
			for {
				select {
				case <-mgr.stopCh:
					return
				case e := <-ei:
					if e.Key == INDEX_DEFS_KEY {
						mgr.GetIndexDefs(true)
						continue
					}

					mgr.RefreshOptions()
				}
			}
		}()

		go func() {
			ep := make(chan CfgEvent)
			mgr.cfg.Subscribe(PLAN_PINDEXES_DIRECTORY_STAMP, ep)
			for {
				select {
				case <-mgr.stopCh:
					return
				case <-ep:
					mgr.GetPlanPIndexes(true)
				}
			}
		}()

		kinds := []string{NODE_DEFS_KNOWN, NODE_DEFS_WANTED}
		for _, kind := range kinds {
			go func(kind string) {
				ep := make(chan CfgEvent)
				mgr.cfg.Subscribe(CfgNodeDefsKey(kind), ep)
				for {
					select {
					case <-mgr.stopCh:
						return
					case <-ep:
						mgr.GetNodeDefs(kind, true)
					}
				}
			}(kind)
		}
	}

	return nil
}

// StartRegister is deprecated and has been renamed to Register().
func (mgr *Manager) StartRegister(register string) error {
	return mgr.Register(register)
}

// Register will register or unregister a Manager with its configured
// Cfg system, based on the register parameter, which can have these
// values:
// * wanted - register this node as wanted
// * wantedForce - same as wanted, but force a Cfg update
// * known - register this node as known
// * knownForce - same as unknown, but force a Cfg update
// * unwanted - unregister this node no longer wanted
// * unknown - unregister this node no longer wanted and no longer known
// * unchanged - don't change any Cfg registrations for this node
func (mgr *Manager) Register(register string) error {
	if register == "unchanged" {
		return nil
	}
	if register == "unwanted" || register == "unknown" {
		err := mgr.RemoveNodeDef(NODE_DEFS_WANTED)
		if err != nil {
			return err
		}
		if register == "unknown" {
			err := mgr.RemoveNodeDef(NODE_DEFS_KNOWN)
			if err != nil {
				return err
			}
		}
	}

	log.Printf("manager: container: %s", mgr.container)

	if register == "known" || register == "knownForce" ||
		register == "wanted" || register == "wantedForce" {
		// Save our nodeDef (with our UUID) into the Cfg as a known node.
		err := mgr.SaveNodeDef(NODE_DEFS_KNOWN, register == "knownForce")
		if err != nil {
			return err
		}
		if register == "wanted" || register == "wantedForce" {
			// Save our nodeDef (with our UUID) into the Cfg as a wanted node.
			err := mgr.SaveNodeDef(NODE_DEFS_WANTED, register == "wantedForce")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------

// SaveNodeDef updates the NodeDef registrations in the Cfg system for
// this Manager node instance.
func (mgr *Manager) SaveNodeDef(kind string, force bool) error {
	atomic.AddUint64(&mgr.stats.TotSaveNodeDef, 1)

	if mgr.cfg == nil {
		atomic.AddUint64(&mgr.stats.TotSaveNodeDefNil, 1)
		return nil // Occurs during testing.
	}

	nodeDef := &NodeDef{
		HostPort:    mgr.bindHttp,
		UUID:        mgr.uuid,
		ImplVersion: mgr.version,
		Tags:        mgr.tags,
		Container:   mgr.container,
		Weight:      mgr.weight,
		Extras:      mgr.extras,
	}

	for {
		nodeDefs, cas, err := CfgGetNodeDefs(mgr.cfg, kind)
		if err != nil {
			atomic.AddUint64(&mgr.stats.TotSaveNodeDefGetErr, 1)
			return err
		}
		if nodeDefs == nil {
			nodeDefs = NewNodeDefs(mgr.version)
		}
		nodeDefPrev, exists := nodeDefs.NodeDefs[mgr.uuid]
		if exists && !force {
			if reflect.DeepEqual(nodeDefPrev, nodeDef) {
				atomic.AddUint64(&mgr.stats.TotSaveNodeDefSame, 1)
				atomic.AddUint64(&mgr.stats.TotSaveNodeDefOk, 1)
				return nil // No changes, so leave the existing nodeDef.
			}
		}

		nodeDefs.UUID = NewUUID()
		nodeDefs.NodeDefs[mgr.uuid] = nodeDef
		nodeDefs.ImplVersion = CfgGetVersion(mgr.cfg)
		log.Printf("manager: setting the nodeDefs implVersion "+
			"to %s", nodeDefs.ImplVersion)

		_, err = CfgSetNodeDefs(mgr.cfg, kind, nodeDefs, cas)
		if err != nil {
			if _, ok := err.(*CfgCASError); ok {
				// Retry if it was a CAS mismatch, as perhaps
				// multiple nodes are all racing to register themselves,
				// such as in a full datacenter power restart.
				atomic.AddUint64(&mgr.stats.TotSaveNodeDefRetry, 1)
				continue
			}
			atomic.AddUint64(&mgr.stats.TotSaveNodeDefSetErr, 1)
			return err
		}
		break
	}
	atomic.AddUint64(&mgr.stats.TotSaveNodeDefOk, 1)
	return nil
}

// ---------------------------------------------------------------

// RemoveNodeDef removes the NodeDef registrations in the Cfg system for
// this Manager node instance.
func (mgr *Manager) RemoveNodeDef(kind string) error {
	if mgr.cfg == nil {
		return nil // Occurs during testing.
	}

	for {
		err := CfgRemoveNodeDef(mgr.cfg, kind, mgr.uuid, CfgGetVersion(mgr.cfg))
		if err != nil {
			if _, ok := err.(*CfgCASError); ok {
				// Retry if it was a CAS mismatch, as perhaps multiple
				// nodes are racing to register/unregister themselves,
				// such as in a full cluster power restart.
				continue
			}
			return err
		}
		break
	}

	return nil
}

// bootingPIndexes maintains the loading status of pindexes
// during the loadDataDir operation. An entry in bootingPIndexes
// indicates that the pindex is booting.
// bootingPIndex returns true if the pindex loading is in progress
func (mgr *Manager) bootingPIndex(pindex string) bool {
	mgr.m.RLock()
	rv := mgr.bootingPIndexes[pindex]
	mgr.m.RUnlock()
	return rv
}

// update the booting status and returns whether the update was success or not
func (mgr *Manager) updateBootingStatus(pindex string, status bool) bool {
	if pindex != "" {
		mgr.m.Lock()
		defer mgr.m.Unlock()
		if !status {
			// booting completed
			delete(mgr.bootingPIndexes, pindex)
			return true
		}
		if _, exists := mgr.pindexes[pindex]; exists {
			// already booted by Janitor, no status updates
			return false
		}

		if mgr.bootingPIndexes[pindex] {
			// if booting already in progress
			return false
		}

		mgr.bootingPIndexes[pindex] = true
	}
	return true
}

type pindexLoadReq struct {
	path, pindexName string
}

// ---------------------------------------------------------------

// TempPathPrefix indicates the prefix string applied to
// name a temp directory.
var TempPathPrefix = "temp$$"

// Walk the data dir and register pindexes for a Manager instance.
func (mgr *Manager) LoadDataDir() error {
	log.Printf("manager: loading dataDir...")
	dirEntries, err := os.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("manager: could not read dataDir: %s, err: %v",
			mgr.dataDir, err)
	}

	// clean up any left over temp download directories.
	for i := len(dirEntries) - 1; i >= 0; i-- {
		path := filepath.Join(mgr.dataDir, dirEntries[i].Name())
		if strings.HasPrefix(dirEntries[i].Name(), TempPathPrefix) {
			log.Printf("manager: purging temp directory: %s", path)
			os.RemoveAll(path)
			dirEntries = append(dirEntries[:i], dirEntries[i+1:]...)
		}
	}

	size := len(dirEntries)
	openReqs := make(chan *pindexLoadReq, size)
	nWorkers := getWorkerCount(size)
	var wg sync.WaitGroup
	// spawn the openPIndex workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for req := range openReqs {
				// check whether the path still exists and if not then skip.
				if _, err := os.Stat(req.path); os.IsNotExist(err) {
					continue
				}
				// check whether pindex already loaded by the Janitor
				// its possible after the first kick from a worker.
				// if not loaded yet, then mark the pindex booting inprogress status
				if !mgr.updateBootingStatus(req.pindexName, true) {
					// 'p' already loaded
					continue
				}
				// we have already validated the pindex paths, hence feeding directly
				pindex, err := OpenPIndex(mgr, req.path)
				if err != nil {
					if strings.Contains(err.Error(), panicCallStack) {
						log.Printf("manager: OpenPIndex error,"+
							" cleaning up and trying NewPIndex,"+
							" path: %s, err: %v", req.path, err)
						os.RemoveAll(req.path)
					} else {
						log.Errorf("manager: could not open pindex path: %s, err: %v",
							req.path, err)
					}
				} else {
					mgr.registerPIndex(pindex)
					// kick the janitor only in case of successful pindex load
					// to complete the boot up ceremony like feed hook ups.
					// but for a failure, we would like to depend on the
					// usual healing power of JanitorOnce loop.
					// Note: The moment first work kick happens, then its the Janitor
					// who handles the further loading of pindexes.
					mgr.janitorCh <- &workReq{op: WORK_KICK}
				}
				// mark the pindex booting complete status
				mgr.updateBootingStatus(req.pindexName, false)
			}
			wg.Done()
		}()
	}
	// feed the openPIndex workers with pindex paths
	for _, dirInfo := range dirEntries {
		path := mgr.dataDir + string(os.PathSeparator) + dirInfo.Name()
		// validate the pindex path here, if valid then
		// send to workers for further processing
		name, ok := mgr.ParsePIndexPath(path)
		if !ok {
			// Skip the entry that doesn't match the naming pattern.
			continue
		}
		openReqs <- &pindexLoadReq{path: path, pindexName: name}
	}
	close(openReqs)

	// log this message only after all workers have completed
	go func() {
		wg.Wait()
		atomic.AddUint64(&mgr.stats.TotLoadDataDir, 1)
		log.Printf("manager: loading dataDir... done")
	}()

	// leave the pindex loading task to the async workers and return here
	return nil
}

// ---------------------------------------------------------------

// Schedule kicks of the planner and janitor of a Manager.
func (mgr *Manager) Kick(msg string) {
	atomic.AddUint64(&mgr.stats.TotKick, 1)

	mgr.PlannerKick(msg)
	mgr.JanitorKick(msg)
}

// ---------------------------------------------------------------

// ClosePIndex synchronously has the janitor close a pindex.
func (mgr *Manager) ClosePIndex(pindex *PIndex) error {
	return syncWorkReq(mgr.janitorCh, JANITOR_CLOSE_PINDEX,
		"api-ClosePIndex:"+pindex.Name, pindex)
}

// RemovePIndex synchronously has the janitor remove a pindex.
func (mgr *Manager) RemovePIndex(pindex *PIndex) error {
	return syncWorkReq(mgr.janitorCh, JANITOR_REMOVE_PINDEX,
		"api-RemovePIndex:"+pindex.Name, pindex)
}

// GetPIndex retrieves a named pindex instance.
func (mgr *Manager) GetPIndex(pindexName string) *PIndex {
	mgr.m.RLock()
	rv := mgr.pindexes[pindexName]
	mgr.m.RUnlock()
	return rv
}

func (mgr *Manager) registerPIndex(pindex *PIndex) error {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if _, exists := mgr.pindexes[pindex.Name]; exists {
		return fmt.Errorf("manager: registered pindex exists, name: %s",
			pindex.Name)
	}

	pindexes := mgr.copyPIndexesLOCKED()
	pindexes[pindex.Name] = pindex
	mgr.pindexes = pindexes
	atomic.AddUint64(&mgr.stats.TotRegisterPIndex, 1)
	mgr.coveringCache = nil

	if mgr.meh != nil {
		mgr.meh.OnRegisterPIndex(pindex)
	}

	if RegisteredPIndexCallbacks.OnCreate != nil {
		RegisteredPIndexCallbacks.OnCreate(pindex.Name)
	}

	return nil
}

// unregisterPIndex takes an optional pindexToMatch, which the caller
// can use for an exact pindex unregistration.
func (mgr *Manager) unregisterPIndex(name string, pindexToMatch *PIndex) *PIndex {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	pindex, ok := mgr.pindexes[name]
	if ok {
		if pindexToMatch != nil &&
			pindexToMatch != pindex {
			return nil
		}

		pindexes := mgr.copyPIndexesLOCKED()
		delete(pindexes, name)
		mgr.pindexes = pindexes
		atomic.AddUint64(&mgr.stats.TotUnregisterPIndex, 1)
		mgr.coveringCache = nil

		if mgr.meh != nil {
			mgr.meh.OnUnregisterPIndex(pindex)
		}

		if RegisteredPIndexCallbacks.OnDelete != nil {
			RegisteredPIndexCallbacks.OnDelete(pindex.Name)
		}
	}

	return pindex
}

// ---------------------------------------------------------------

func (mgr *Manager) registerFeed(feed Feed) error {
	mgr.feedsMutex.Lock()
	defer mgr.feedsMutex.Unlock()

	if _, exists := mgr.feeds[feed.Name()]; exists {
		return fmt.Errorf("manager: registered feed already exists, name: %s",
			feed.Name())
	}

	feeds := mgr.copyFeedsLOCKED()
	feeds[feed.Name()] = feed
	mgr.feeds = feeds
	atomic.AddUint64(&mgr.stats.TotRegisterFeed, 1)

	return nil
}

func (mgr *Manager) unregisterFeed(name string) Feed {
	mgr.feedsMutex.Lock()
	defer mgr.feedsMutex.Unlock()

	rv, ok := mgr.feeds[name]
	if ok {
		feeds := mgr.copyFeedsLOCKED()
		delete(feeds, name)
		mgr.feeds = feeds
		atomic.AddUint64(&mgr.stats.TotUnregisterFeed, 1)
	}

	return rv
}

// ---------------------------------------------------------------

// Returns a snapshot copy of the current feeds and pindexes.
func (mgr *Manager) CurrentMaps() (map[string]Feed, map[string]*PIndex) {
	mgr.feedsMutex.RLock()
	feeds := mgr.feeds
	mgr.feedsMutex.RUnlock()
	mgr.m.RLock()
	pindexes := mgr.pindexes
	mgr.m.RUnlock()
	return feeds, pindexes
}

// ---------------------------------------------------------------

func (mgr *Manager) copyFeedsLOCKED() map[string]Feed {
	feeds := make(map[string]Feed)
	for k, v := range mgr.feeds {
		feeds[k] = v
	}
	return feeds
}

func (mgr *Manager) copyPIndexesLOCKED() map[string]*PIndex {
	pindexes := make(map[string]*PIndex)
	for k, v := range mgr.pindexes {
		pindexes[k] = v
	}
	return pindexes
}

// ---------------------------------------------------------------

// Returns read-only snapshot of NodeDefs of a given kind (i.e.,
// NODE_DEFS_WANTED).  Use refresh of true to force a read from Cfg.
func (mgr *Manager) GetNodeDefs(kind string, refresh bool) (
	nodeDefs *NodeDefs, err error) {
	mgr.m.RLock()
	nodeDefs = mgr.lastNodeDefs[kind]
	mgr.m.RUnlock()

	if nodeDefs == nil || refresh {
		mgr.m.Lock()
		defer mgr.m.Unlock()
		nodeDefs, _, err = CfgGetNodeDefs(mgr.Cfg(), kind)
		if err != nil {
			return nil, err
		}
		mgr.lastNodeDefs[kind] = nodeDefs
		atomic.AddUint64(&mgr.stats.TotRefreshLastNodeDefs, 1)
		mgr.coveringCache = nil
		// update the container cache if required.
		if nodeDef, ok := nodeDefs.NodeDefs[mgr.uuid]; ok {
			if nodeDef.Container != mgr.container &&
				nodeDef.Container != "" {
				mgr.container = nodeDef.Container
				log.Printf("manager: refreshed container: %s", mgr.container)
			}
		}

		if RegisteredPIndexCallbacks.OnRefresh != nil {
			RegisteredPIndexCallbacks.OnRefresh()
		}
	}

	return nodeDefs, nil
}

// Returns read-only snapshot of the IndexDefs, also with IndexDef's
// organized by name.  Use refresh of true to force a read from Cfg.
func (mgr *Manager) GetIndexDefs(refresh bool) (lastIndexDefs *IndexDefs,
	lastIndexDefsByName map[string]*IndexDef, err error) {
	if !refresh {
		mgr.m.RLock()
		lastIndexDefs = mgr.lastIndexDefs
		lastIndexDefsByName = mgr.lastIndexDefsByName
		mgr.m.RUnlock()
	}

	if lastIndexDefs == nil || refresh {
		mgr.m.Lock()
		lastIndexDefs, _, err = CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			mgr.m.Unlock()
			return nil, nil, err
		}
		mgr.lastIndexDefs = lastIndexDefs
		atomic.AddUint64(&mgr.stats.TotRefreshLastIndexDefs, 1)

		lastIndexDefsByName = make(map[string]*IndexDef)
		if lastIndexDefs != nil {
			for _, indexDef := range lastIndexDefs.IndexDefs {
				lastIndexDefsByName[indexDef.Name] = indexDef
			}
		}
		mgr.lastIndexDefsByName = lastIndexDefsByName

		mgr.coveringCache = nil

		mgr.m.Unlock()

		if RegisteredPIndexCallbacks.OnRefresh != nil {
			RegisteredPIndexCallbacks.OnRefresh()
		}
	}

	return lastIndexDefs, lastIndexDefsByName, nil
}

func (mgr *Manager) CheckAndGetIndexDef(indexName string,
	refresh bool) (*IndexDef, error) {
	indexDefs, _, err := mgr.GetIndexDefs(refresh)
	if err != nil {
		return nil, err
	}

	if indexDefs == nil {
		return nil, ErrNoIndexDefs
	}

	indexDef := indexDefs.IndexDefs[indexName]
	if indexDef == nil {
		return nil, nil
	}

	return indexDef, nil
}

// GetIndexDef retrieves the IndexDef and PIndexImplType for an index.
// Use refresh of true to force a read from Cfg.
func (mgr *Manager) GetIndexDef(indexName string, refresh bool) (
	*IndexDef, *PIndexImplType, error) {
	var indexDef *IndexDef
	var err error
	indexDef, err = mgr.CheckAndGetIndexDef(indexName, refresh)
	if err != nil {
		return nil, nil, fmt.Errorf("manager: could not get indexDefs,"+
			" indexName: %s, err: %v",
			indexName, err)
	}

	if indexDef == nil {
		return nil, nil, fmt.Errorf("manager: no indexDef,"+
			" indexName: %s", indexName)
	}

	pindexImplType := PIndexImplTypes[indexDef.Type]
	if pindexImplType == nil {
		return nil, nil, fmt.Errorf("manager: no pindexImplType,"+
			" indexName: %s, indexDef.Type: %s",
			indexName, indexDef.Type)
	}

	return indexDef, pindexImplType, nil
}

var LimitIndexDefHook func(mgr *Manager, indexDef *IndexDef) (*IndexDef, error)

var HibernationClientHook = func(string) (objcli.Client, error) {
	return nil, fmt.Errorf("not-implemented")
}

// This function does the groundwork/preparation for hibernation tasks.
func (mgr *Manager) HibernationPrepareUtil(task, bucket, remoteStorageRegion string,
	rateLimit uint64, dryRun bool) error {
	mgr.setHibernationContext(rateLimit)
	objStoreClient, err := HibernationClientHook(remoteStorageRegion)
	if err != nil {
		return fmt.Errorf("manager: unable to get object store client: %v", err)
	}
	mgr.setObjStoreClient(objStoreClient)

	// Does not require bucket/task to be tracked during dry run
	if !dryRun {
		mgr.SetOption(task, "true", false)
		mgr.MarkBucketForHibernation(task + ":" + bucket)
	}

	return nil
}

func (mgr *Manager) CheckIfIndexesCanBeAdded(indexDefs *IndexDefs) error {
	if LimitIndexDefHook != nil {
		for _, index := range indexDefs.IndexDefs {
			_, err := LimitIndexDefHook(mgr, index)
			if err != nil {
				// Returning an error if even one index cannot be accommodated.
				return err
			}
		}
	}
	return nil
}

func (mgr *Manager) GetIndexNameForPIndex(pindexName string) (
	string, error) {
	_, indexPlansMap, err := mgr.GetPlanPIndexes(true)
	if err != nil {
		return "", fmt.Errorf("manager: error getting plan pindexes: %v",
			err)
	}
	for index, plans := range indexPlansMap {
		for _, plan := range plans {
			if plan.Name == pindexName {
				return index, nil
			}
		}
	}
	return "", nil
}

// Returns read-only snapshot of the PlanPIndexes, also with PlanPIndex's
// organized by IndexName.  Use refresh of true to force a read from Cfg.
func (mgr *Manager) GetPlanPIndexes(refresh bool) (
	*PlanPIndexes, map[string][]*PlanPIndex, error) {

	mgr.m.RLock()
	lastPlanPIndexes := mgr.lastPlanPIndexes
	lastPlanPIndexesByName := mgr.lastPlanPIndexesByName
	mgr.m.RUnlock()

	if lastPlanPIndexes == nil || refresh {
		mgr.m.Lock()
		defer mgr.m.Unlock()

		lastPlanPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
		if err != nil {
			return nil, nil, err
		}
		// skip disk writes on repeated Cfg callbacks.
		if !reflect.DeepEqual(mgr.lastPlanPIndexes, lastPlanPIndexes) {
			// make a local copy of the updated plan,
			mgr.checkAndStoreStablePlanPIndexes(lastPlanPIndexes)
		}

		mgr.lastPlanPIndexes = lastPlanPIndexes
		atomic.AddUint64(&mgr.stats.TotRefreshLastPlanPIndexes, 1)

		lastPlanPIndexesByName = make(map[string][]*PlanPIndex)
		if lastPlanPIndexes != nil {
			for _, planPIndex := range lastPlanPIndexes.PlanPIndexes {
				a := lastPlanPIndexesByName[planPIndex.IndexName]
				if a == nil {
					a = make([]*PlanPIndex, 0)
				}
				lastPlanPIndexesByName[planPIndex.IndexName] =
					append(a, planPIndex)
			}
		}
		mgr.lastPlanPIndexesByName = lastPlanPIndexesByName

		mgr.coveringCache = nil

		if RegisteredPIndexCallbacks.OnRefresh != nil {
			RegisteredPIndexCallbacks.OnRefresh()
		}
	}

	return lastPlanPIndexes, lastPlanPIndexesByName, nil
}

// GetStableLocalPlanPIndexes retrieves the recovery plan for
// a failover-recovery.
func (mgr *Manager) GetStableLocalPlanPIndexes() *PlanPIndexes {
	dirPath := filepath.Join(mgr.dataDir, "planPIndexes")
	mgr.stablePlanPIndexesMutex.RLock()
	defer mgr.stablePlanPIndexesMutex.RUnlock()
	// read the files from the planPIndexes directory.
	files, err := os.ReadDir(dirPath)
	if err != nil {
		log.Errorf("manager: GetStableLocalPlanPIndexes, readDir err: %v", err)
		return nil
	}

	rv := &PlanPIndexes{}
	// There will only be a single file in the directory, and if the processing
	// fails then fall back to the usual flow of recovery by returning nil,
	// As the files are in the ascending order of their names, let's read the
	// latest first. This helps the situation if there was a kill -9/node crash
	// on the writer side to end up having multiple files on disk.
	for i := len(files) - 1; i >= 0; i-- {
		path := filepath.Join(dirPath, files[i].Name())
		val, err := os.ReadFile(path)
		if err != nil {
			log.Errorf("manager: GetStableLocalPlanPIndexes, readFile, err: %v", err)
			// in case of a file read error, check for any subsequent plan files.
			continue
		}

		contentMD5, err := computeMD5(val)
		if err != nil {
			log.Errorf("manager: GetStableLocalPlanPIndexes, computeMD5, err: %v", err)
			// in case of a hash compute error, check for any subsequent plan files.
			continue
		}

		// Get the hashMD5 from the file name
		fname := files[i].Name()
		nameMD5 := fname[strings.LastIndex(fname, "-")+1:]
		if contentMD5 != nameMD5 {
			log.Errorf("manager: GetStableLocalPlanPIndexes failed, hash mismatch "+
				"contentMD5: %s, contents: %s, path: %s", contentMD5, val, path)
			// in case of a hash mis match, check for any subsequent plan files.
			continue
		}

		err = UnmarshalJSON(val, rv)
		if err != nil {
			// if the file is read successfully and hash digest matched then json
			// parsing should have passed too. So return here.
			log.Errorf("manager: GetStableLocalPlanPIndexes, json, err: %v", err)
			return nil
		}
		log.Printf("manager: GetStableLocalPlanPIndexes, recovery plan: %s", val)
		return rv
	}

	return nil
}

// IsStablePlan checks whether the given plan is a stable or evolving plan
// by checking the partition-node assignments of partitions belonging to
// each of the indexes. If all the partitions belonging to an index is having
// same exact node assignments count, then the partition assignment is considered
// stable for that index. If all the indexes in a plan is having stable node,
// assignments then that plan is considered stable and can be stored for recovery.
func (mgr *Manager) IsStablePlan(planPIndexes *PlanPIndexes) bool {
	if planPIndexes == nil || planPIndexes.PlanPIndexes == nil {
		return false
	}
	// group planPIndexes per index.
	planPIndexesPerIndex := make(map[string][]*PlanPIndex)
	for _, pi := range planPIndexes.PlanPIndexes {
		planPIndexesPerIndex[pi.IndexName] = append(
			planPIndexesPerIndex[pi.IndexName], pi)
	}

	// consider partitions per index.
	for _, planPIndexes := range planPIndexesPerIndex {
		// check whether all the index partitions are having same number of
		// node assignments.
		nodeCount := -1
		for _, p := range planPIndexes {
			if len(p.Nodes) == 0 && len(planPIndexes) == 1 {
				return false
			}

			// get the partition count from the index definition.
			if mgr.lastIndexDefsByName != nil {
				indexDef := mgr.lastIndexDefsByName[p.IndexName]
				if indexDef != nil &&
					indexDef.PlanParams.NumReplicas+1 != len(p.Nodes) {
					// plan doesn't contain the full partition-node assignments
					// as per the index definitions then its not a stable/final plan.
					return false
				}
			}

			if nodeCount == -1 {
				nodeCount = len(p.Nodes)
				continue
			}
			if len(p.Nodes) != nodeCount {
				return false
			}
		}
	}
	return true
}

func (mgr *Manager) checkAndStoreStablePlanPIndexes(planPIndexes *PlanPIndexes) {
	if !mgr.IsStablePlan(planPIndexes) {
		return
	}
	val, err := MarshalJSON(planPIndexes)
	if err != nil {
		log.Errorf("manager: persistPlanPIndexes, json err: %v", err)
		return
	}
	// Decorate the file name with the hash of the plan contents so that
	// the content can be verified during the read paths.
	hashMD5, err := computeMD5(val)
	if err != nil {
		return
	}
	timeStr := strconv.FormatInt(time.Now().UnixNano()/1000000, 10)
	fname := "recoveryPlan-" + timeStr + "-" + hashMD5
	dirPath := filepath.Join(mgr.dataDir, "planPIndexes")
	newPath := filepath.Join(dirPath, fname)

	log.Printf("manager: persistPlanPIndexes, new plan path: %s", newPath)

	mgr.stablePlanPIndexesMutex.Lock()
	defer mgr.stablePlanPIndexesMutex.Unlock()

	err = os.MkdirAll(dirPath, 0700)
	if err != nil {
		log.Errorf("manager: persistPlanPIndexes,  MkdirAll failed, err: %v", err)
		return
	}
	err = os.WriteFile(newPath, val, 0600)
	if err != nil {
		log.Errorf("manager: persistPlanPIndexes writeFile failed, err: %v", err)
		return
	}

	// After successful write to disk for the latest plan,
	// purge all older plans except the most recent one.
	// The plan right before a failover ought to be a stable, usable
	// plan for a failover-recovery operation.
	// ReadDir returns files in the sorted order of their timestamped names.
	files, err := os.ReadDir(dirPath)
	if err != nil {
		log.Errorf("manager: persistPlanPIndexes, readDir failed, err: %v", err)
		return
	}
	// No purging needs to be done for a single file on disk.
	if len(files) <= 1 {
		return
	}
	// As the files are in the sorted order of their timestamped names,
	// purge all older plan files from disk.
	files = files[:len(files)-1]
	for _, f := range files {
		fname := f.Name()
		// extra check with the timestamp for the most recent one.
		if strings.Contains(fname, timeStr) {
			continue
		}
		err := os.Remove(filepath.Join(dirPath, fname))
		if err != nil {
			log.Errorf("manager: persistPlanPIndexes, remove failed, err %v", err)
			continue
		}
	}
}

// ---------------------------------------------------------------

// PIndexPath returns the filesystem path for a given named pindex.
// See also ParsePIndexPath().
func (mgr *Manager) PIndexPath(pindexName string) string {
	return PIndexPath(mgr.dataDir, pindexName)
}

// ParsePIndexPath returns the name for a pindex given a filesystem
// path.  See also PIndexPath().
func (mgr *Manager) ParsePIndexPath(pindexPath string) (string, bool) {
	return ParsePIndexPath(mgr.dataDir, pindexPath)
}

// ---------------------------------------------------------------

// Returns the start time of a Manager.
func (mgr *Manager) StartTime() time.Time {
	return mgr.startTime
}

// Returns the version of a Manager.
func (mgr *Manager) Version() string {
	return mgr.version
}

// Returns the configured Cfg of a Manager.
func (mgr *Manager) Cfg() Cfg {
	return mgr.cfg
}

// Returns the UUID (the "node UUID") of a Manager.
func (mgr *Manager) UUID() string {
	return mgr.uuid
}

// Returns the configured tags of a Manager, which should be
// treated as immutable / read-only.
func (mgr *Manager) Tags() []string {
	return mgr.tags
}

// Returns the configured tags map of a Manager, which should be
// treated as immutable / read-only.
func (mgr *Manager) TagsMap() map[string]bool {
	return mgr.tagsMap
}

// Returns the configured container of a Manager.
func (mgr *Manager) Container() string {
	return mgr.container
}

// Returns the configured weight of a Manager.
func (mgr *Manager) Weight() int {
	return mgr.weight
}

// Returns the configured extras of a Manager.
func (mgr *Manager) Extras() string {
	return mgr.extras
}

// Returns the configured bindHttp of a Manager.
func (mgr *Manager) BindHttp() string {
	return mgr.bindHttp
}

// Returns the configured data dir of a Manager.
func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

// Returns the configured server(s) of a Manager.
func (mgr *Manager) Server() string {
	return mgr.server
}

// Same as GetOptions(), for backwards compatibility.
func (mgr *Manager) Options() map[string]string {
	return mgr.GetOptions()
}

// GetOptions returns the (read-only) options of a Manager.  Callers
// must not modify the returned map.
func (mgr *Manager) GetOptions() map[string]string {
	mgr.optionsMutex.RLock()
	options := mgr.options
	mgr.optionsMutex.RUnlock()
	return options
}

func (mgr *Manager) GetOption(key string) string {
	mgr.optionsMutex.RLock()
	val := mgr.options[key]
	mgr.optionsMutex.RUnlock()
	return val
}

// RefreshOptions updates the local managerOptions cache
func (mgr *Manager) RefreshOptions() error {
	mo, _, err := CfgGetClusterOptions(mgr.cfg)
	if err != nil || mo == nil {
		return err
	}
	// apply the newer values from the cluster level options
	// into the managerOptions cache
	mgr.optionsMutex.Lock()
	opts := mgr.options
	newOptions := map[string]string{}
	for k, v := range opts {
		newOptions[k] = v
	}
	oval := reflect.ValueOf(*mo)
	for i := 0; i < oval.NumField(); i++ {
		if v, ok := oval.Field(i).Interface().(string); ok && v != "" {
			optionName := strings.ToLower(string(oval.Type().Field(i).Name[0])) +
				oval.Type().Field(i).Name[1:]
			newOptions[optionName] = v
		}
	}
	mgr.options = newOptions
	log.Printf("manager: RefreshOptions: %+v finished", mgr.options)
	mgr.optionsMutex.Unlock()
	// invoke any manager option refresh callbacks.
	if mgr.meh != nil {
		mgr.meh.OnRefreshManagerOptions(newOptions)
	}

	return err
}

// SetOptions replaces the options map with the provided map, which
// should be considered immutable after this call.
func (mgr *Manager) SetOptions(options map[string]string) error {
	// extract the values to be stored as the cluster options
	// in metakv from the option map
	mo := ClusterOptions{}
	oval := reflect.ValueOf(&mo)
	for k, v := range options {
		fName := strings.ToUpper(string(k[0])) + k[1:]
		f := oval.Elem().FieldByName(fName)
		if f.IsValid() {
			f.SetString(v)
		}
	}
	mgr.optionsMutex.Lock()
	_, err := CfgSetClusterOptions(mgr.cfg, &mo, 0)
	if err != nil {
		mgr.optionsMutex.Unlock()
		return err
	}
	mgr.options = options
	atomic.AddUint64(&mgr.stats.TotSetOptions, 1)
	mgr.optionsMutex.Unlock()
	return nil
}

// Copies the current manager stats to the dst manager stats.
func (mgr *Manager) StatsCopyTo(dst *ManagerStats) {
	mgr.stats.AtomicCopyTo(dst)
}

// --------------------------------------------------------

func (mgr *Manager) VisitEvents(callback func(event []byte)) {
	mgr.eventsMutex.RLock()
	defer mgr.eventsMutex.RUnlock()

	p := mgr.events.Front()
	for p != nil {
		callback(p.Value.([]byte))
		p = p.Next()
	}
}

// --------------------------------------------------------

func (mgr *Manager) AddEvent(jsonBytes []byte) {
	mgr.eventsMutex.Lock()
	for mgr.events.Len() >= MANAGER_MAX_EVENTS {
		mgr.events.Remove(mgr.events.Front())
	}
	mgr.events.PushBack(jsonBytes)
	mgr.eventsMutex.Unlock()
}

// --------------------------------------------------------

// AtomicCopyTo copies metrics from s to r (from source to result).
func (s *ManagerStats) AtomicCopyTo(r *ManagerStats) {
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			atomic.StoreUint64(rvefp.(*uint64),
				atomic.LoadUint64(svefp.(*uint64)))
		}
	}
}
