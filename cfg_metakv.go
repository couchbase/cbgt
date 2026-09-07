//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbauth/metakv"
)

// The values stored in a Cfg are normally atomic, but CfgMetaKv has
// an exception for values related to NodeDefs and PlanPIndexes.
// In CfgMetaKv, NodeDefs and PlanPIndexes are handled in "composite"
// fashion, where CfgMetaKv will split a NodeDefs/PlanPIndexes value
// into zero or more child values, each stored into metakv using a
// shared path prefix (so, similar to having a file-per-NodeDef in a
// shared directory).
//
// This maneuver helps mainly on two aspects.
// First, it allows concurrent, racing nodes to register their own entries
// into a composite NodeDefs "directory".  This can happen when
// multiple nodes are launched at nearly the same time, faster than
// metakv can replicate in eventually consistent fashion.
//
// Secondly, it helps in optimising the storage requirements of
// the PlanPIndexes at the config level by de duplicating the information.
//
// Eventually, metakv replication will occur, and a reader will see a
// union of NodeDef values that CfgMetaKv reconstructs on the fly for
// the entire "directory".
//
// The CAS values that CfgMetaKv hands out for its non-composite
// entries come from the opaque revisions ("rev"s) that metakv keeps
// per key, as the Cfg API has no room for a rev.  A Get() pairs the
// entry's current rev with a CAS, and a Set()/Del() carrying that CAS
// resolves it back to the rev for metakv to compare against.
//
// A CAS from a Set() cannot be CAS'ed on, as metakv does not report
// the rev that it assigned; mutating again means Get()'ing first.

// Prefix of paths stored in metakv, and should be immutable after
// process init()'ialization.
var CfgMetaKvPrefix = "/cbgt/cfg/"

var LeanPlanVersion = "5.5.0"

var AdvMetaEncodingFeatureVersion = "5.6.0"

// NodeFeatureAdvMetaEncoding represents the feature flag for the
// advanced metadata encoding that comprises of two format changes.
//   - compress the data before writing to metakv.
//   - split the plan contents on to multiple keys upon reaching
//     a per key size threshold of 100KB.
var NodeFeatureAdvMetaEncoding = "advMetaEncoding"

// CfgAppVersion is a global Cfg variable supposed to be overridden by
// applications to indicate the current application version.
// This version information is used to figure out cluster level compatibility
// issues.
var CfgAppVersion = "7.5.0"

type cfgMetaKvAdvancedHandler interface {
	get(c *CfgMetaKv, key string, cas uint64) ([]byte, uint64, error)
	set(c *CfgMetaKv, key string, val []byte, cas uint64) (uint64, error)
	del(c *CfgMetaKv, key string, cas uint64) error
}

const CFG_METAKV = "metakv"

// PLAN_PINDEXES_KEY refers to the PlanPIndex handler which basically
// invokes the different optimised representations of the PlanPIndex
// (shared/lean) depending on the feature availability in the cluster.
//
// Lean plan re arranges the PlanPIndexes information grouped by index
// level to optimise the de duplication of information.
// So all the common fields of all the PlanPIndexes for a given index are
// stored only once per index level as given below. This boosts the savings
// with higher number of partitions per index.
// And the plans for each index are stored in sibling folders at metakv.
// Introduced since 5.1
//
// LeanPlanPIndexes -> map[indexName]
//                                  => LeanIndexPlanPIndexes
//                                     { indexDef,
//                                       map[PlanPIndexName]=> LeanPlanPIndex{}
//                                     }
//
// Shared plan refers to another approach for saving space while
// storing the PlanPIndexes. Here common fields like IndexParams and
// SourceParams across all PlanPIndexes of an index are deduplicated.
// And the entire PlanPIndexes are stored under the same folder at metakv.

var cfgMetaKvAdvancedKeys map[string]cfgMetaKvAdvancedHandler = map[string]cfgMetaKvAdvancedHandler{
	CfgNodeDefsKey(NODE_DEFS_WANTED): &cfgMetaKvNodeDefsSplitHandler{},
	CfgNodeDefsKey(NODE_DEFS_KNOWN):  &cfgMetaKvNodeDefsSplitHandler{},
	PLAN_PINDEXES_KEY:                &cfgMetaKvPlanPIndexesHandler{},
	INDEX_DEFS_KEY:                   &cfgMetaKvIndexDefsHandler{},
}

type CfgMetaKv struct {
	prefix   string // Prefix for paths stores in metakv.
	nodeUUID string // The uuid for this node.

	m            sync.Mutex // Protects the fields that follow.
	cfgMem       *CfgMem
	cancelCh     chan struct{}
	lastSplitCAS uint64
	splitEntries map[string]CfgMetaKvEntry
	nsServerUrl  string

	// Applicable to non-split metaKVEntries only: lastCAS is the
	// source of their CAS values, and revEntries maps each current
	// CAS back to the metakv rev it came from.
	lastCAS    uint64
	revEntries map[string]cfgMetaKvRev

	advMetaEncodingSupported int32

	readersPool *sync.Pool
	writersPool *sync.Pool
}

type CfgMetaKvEntry struct {
	cas  uint64
	data []byte
}

// cfgMetaKvRev pairs a non-composite key's metakv rev with the CAS
// handed out for it.  A fresh CAS is created only when a Get() sees a
// rev differing from the recorded one, so repeated Get()'s of an
// unchanged entry keep returning the same CAS.
type cfgMetaKvRev struct {
	cas uint64
	rev interface{}
}

// sameRev compares two opaque metakv revs.
func sameRev(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	aBytes, aOk := a.([]byte)
	bBytes, bOk := b.([]byte)

	return aOk && bOk && bytes.Equal(aBytes, bBytes)
}

// VersionReader is an interface to be implemented by the
// configuration providers who supports the verification of
// homogeneousness of the cluster before performing certain
// Key/Values updates related to the cluster status
type VersionReader interface {
	// ClusterVersion retrieves the cluster
	// compatibility information from the ns_server
	ClusterVersion() (string, error)
}

// NewCfgMetaKv returns a CfgMetaKv that reads and stores its single
// configuration file in the metakv.
func NewCfgMetaKv(nodeUUID string, options map[string]string) (*CfgMetaKv, error) {
	nsServerURL, _ := options["nsServerURL"]

	cfg := &CfgMetaKv{
		prefix:       CfgMetaKvPrefix,
		nodeUUID:     nodeUUID,
		cfgMem:       NewCfgMem(),
		cancelCh:     make(chan struct{}),
		splitEntries: map[string]CfgMetaKvEntry{},
		revEntries:   map[string]cfgMetaKvRev{},

		readersPool: &sync.Pool{New: func() interface{} { return new(gzip.Reader) }},
		writersPool: &sync.Pool{New: func() interface{} { return new(gzip.Writer) }},
	}

	backoffStartSleepMS := 200
	backoffFactor := float32(1.5)
	backoffMaxSleepMS := 5000
	leanPlanKeyPrefix = CfgMetaKvPrefix + leanPlanKeyPrefix

	cfg.nsServerUrl = nsServerURL + "/pools/default"

	go ExponentialBackoffLoop("cfg_metakv.RunObserveChildren",
		func() int {
			err := metakv.RunObserveChildren(cfg.prefix, cfg.metaKVCallback,
				cfg.cancelCh)
			if err == nil {
				return -1 // Success, so stop the loop.
			}

			log.Warnf("cfg_metakv: RunObserveChildren, err: %v", err)

			return 0 // No progress, so exponential backoff.
		},
		backoffStartSleepMS, backoffFactor, backoffMaxSleepMS)

	return cfg, nil
}

func (c *CfgMetaKv) Get(key string, cas uint64) ([]byte, uint64, error) {
	c.m.Lock()
	data, cas, err := c.getLOCKED(key, cas)
	c.m.Unlock()

	return data, cas, err
}

func (c *CfgMetaKv) getLOCKED(key string, cas uint64) ([]byte, uint64, error) {
	handler := cfgMetaKvAdvancedKeys[key]
	if handler != nil {
		return handler.get(c, key, cas)
	}

	return c.getRawLOCKED(key, cas)
}

// getRawLOCKED reads a non-composite entry from metakv and returns a
// CAS standing for the rev it was read at, so that a later
// setRawLOCKED()/delRawLOCKED() can compare-and-swap against exactly
// that version.  An incoming non-zero cas is not matched, as before.
// Must be invoked with c.m.Lock()'ed.
func (c *CfgMetaKv) getRawLOCKED(key string, cas uint64) ([]byte, uint64, error) {
	path := c.keyToPath(key)

	v, rev, err := metakv.Get(path)
	if err != nil {
		return nil, 0, err
	}

	v, err = c.uncompressLocked(v)
	if err != nil {
		return nil, 0, err
	}

	if rev == nil {
		// metakv reports a missing path as a nil value and nil rev.
		delete(c.revEntries, key)

		return v, 0, nil
	}

	return v, c.recordRevLOCKED(key, rev), nil
}

// recordRevLOCKED associates rev with key, reusing the CAS already
// handed out if the rev has not moved on, and creating a fresh one
// otherwise.  Must be invoked with c.m.Lock()'ed.
func (c *CfgMetaKv) recordRevLOCKED(key string, rev interface{}) uint64 {
	if entry, exists := c.revEntries[key]; exists && sameRev(entry.rev, rev) {
		return entry.cas
	}

	c.lastCAS++

	c.revEntries[key] = cfgMetaKvRev{cas: c.lastCAS, rev: rev}

	return c.lastCAS
}

// forgetRevLOCKED creates a CAS for key with no rev behind it, which
// is what a successful mutation leaves, as metakv does not report the
// rev it assigned.  A later mutation carrying such a CAS gets a
// CfgCASError, forcing a re-read rather than a blind overwrite.
// Must be invoked with c.m.Lock()'ed.
// This entire thing is techincally unnecessary because none of the
// callers of Set actually do anything with the returned cas.
// Leaving here anyways in case someone tries to use the returned cas.
func (c *CfgMetaKv) forgetRevLOCKED(key string) uint64 {
	c.lastCAS++

	c.revEntries[key] = cfgMetaKvRev{cas: c.lastCAS}

	return c.lastCAS
}

// revForCASLOCKED maps a CAS back to the metakv rev it came from, for
// metakv to compare-and-swap on.  A zero CAS ("don't care") and
// CFG_CAS_FORCE map to a nil rev, an unconditional mutation; a CAS
// that is no longer the key's current one is a conflict.
// Must be invoked with c.m.Lock()'ed.
func (c *CfgMetaKv) revForCASLOCKED(key string, cas uint64) (
	interface{}, error) {
	if cas == 0 || cas == CFG_CAS_FORCE {
		return nil, nil
	}

	entry, exists := c.revEntries[key]
	if !exists || entry.cas != cas {
		log.Warnf("cfg_metakv: key: %v, stale cas: %x, cur cas: %x",
			key, cas, entry.cas)

		return nil, &CfgCASError{}
	}

	if entry.rev == nil {
		// See forgetRevLOCKED(): no rev, so re-read before mutating.
		log.Warnf("cfg_metakv: key: %v, cas: %x has no rev to CAS on",
			key, cas)

		return nil, &CfgCASError{}
	}

	return entry.rev, nil
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("cfg_metakv: Set, key: %v, cas: %x, split: %t, nodeUUID: %s",
		key, cas, cfgMetaKvAdvancedKeys[key] != nil, c.nodeUUID)

	c.m.Lock()
	casResult, err := c.setLOCKED(key, val, cas)
	c.m.Unlock()

	return casResult, err
}

func (c *CfgMetaKv) setLOCKED(key string, val []byte, cas uint64) (
	uint64, error) {
	handler := cfgMetaKvAdvancedKeys[key]
	if handler != nil {
		return handler.set(c, key, val, cas)
	}

	return c.setRawLOCKED(key, val, cas)
}

// setRawLOCKED writes a non-composite entry, letting metakv
// compare-and-swap against the rev the incoming cas came from.  The
// cas returned is not resolvable to a rev; see forgetRevLOCKED().
// Must be invoked with c.m.Lock()'ed.
func (c *CfgMetaKv) setRawLOCKED(key string, val []byte, cas uint64) (
	uint64, error) {
	path := c.keyToPath(key)

	// Resolve the rev first: compressLocked() below reads from the Cfg
	// and so can move this key's CAS bookkeeping along.
	rev, err := c.revForCASLOCKED(key, cas)
	if err != nil {
		return 0, err
	}

	val, err = c.compressLocked(val)
	if err != nil {
		return 0, err
	}

	log.Printf("cfg_metakv: Set path: %v", path)

	err = metakv.Set(path, val, rev)
	if err != nil {
		if errors.Is(err, metakv.ErrRevMismatch) {
			// The entry moved on since its CAS was handed out.
			log.Warnf("cfg_metakv: Set, key: %v, cas: %x, rev mismatch",
				key, cas)

			return 0, &CfgCASError{}
		}

		return 0, err
	}

	return c.forgetRevLOCKED(key), nil
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	err := c.delLOCKED(key, cas)
	c.m.Unlock()

	return err
}

func (c *CfgMetaKv) delLOCKED(key string, cas uint64) error {
	handler := cfgMetaKvAdvancedKeys[key]
	if handler != nil {
		return handler.del(c, key, cas)
	}

	return c.delRawLOCKED(key, cas)
}

// delRawLOCKED removes a non-composite entry, letting metakv
// compare-and-swap against the rev the incoming cas came from.
// Must be invoked with c.m.Lock()'ed.
func (c *CfgMetaKv) delRawLOCKED(key string, cas uint64) error {
	path := c.keyToPath(key)

	rev, err := c.revForCASLOCKED(key, cas)
	if err != nil {
		return err
	}

	err = metakv.Delete(path, rev)
	if err != nil {
		if errors.Is(err, metakv.ErrRevMismatch) {
			log.Warnf("cfg_metakv: Del, key: %v, cas: %x, rev mismatch",
				key, cas)

			return &CfgCASError{}
		}

		return err
	}

	delete(c.revEntries, key)

	return nil
}

func (c *CfgMetaKv) Load() error {
	metakv.IterateChildren(c.prefix, c.metaKVCallback)

	return nil
}

func (c *CfgMetaKv) metaKVCallback(kve metakv.KVEntry) error {
	key := c.pathToKey(kve.Path)

	log.Printf("cfg_metakv: metaKVCallback, path: %v, key: %v,"+
		" deletion: %t", kve.Path, key, kve.Value == nil)

	for splitKeyPrefix := range cfgMetaKvAdvancedKeys {
		// Handle the case when the key from metakv looks like
		// "nodeDefs-known/63f8a79660", but the subscription was for a
		// key prefix like "nodeDefs-known".
		if strings.HasPrefix(key, splitKeyPrefix) {
			key = splitKeyPrefix
		}
	}

	c.m.Lock()
	c.cfgMem.FireEvent(key, 1, nil)
	c.m.Unlock()

	return nil
}

func (c *CfgMetaKv) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	err := c.cfgMem.Subscribe(key, ch)
	c.m.Unlock()

	return err
}

func (c *CfgMetaKv) Refresh() error {
	return c.Load()
}

// RemoveAllKeys removes all cfg entries from metakv, where the caller
// should no longer use this CfgMetaKv instance, but instead create a
// new instance.
func (c *CfgMetaKv) RemoveAllKeys() {
	metakv.RecursiveDelete(c.prefix)
}

func (c *CfgMetaKv) keyToPath(key string) string {
	return c.prefix + key
}

func (c *CfgMetaKv) pathToKey(k string) string {
	return k[len(c.prefix):]
}

// for testing
func (c *CfgMetaKv) listChildPaths(key string) ([]string, error) {
	g := []string{}

	if cfgMetaKvAdvancedKeys[key] != nil {
		m, err := metakv.ListAllChildren(c.keyToPath(key) + "/")
		if err != nil {
			return nil, err
		}
		for _, v := range m {
			g = append(g, v.Path)
		}
	}

	return g, nil
}

func checkSumUUIDs(uuids []string) string {
	sort.Strings(uuids)
	d, _ := MarshalJSON(uuids)
	return fmt.Sprint(crc32.ChecksumIEEE(d))
}

// ----------------------------------------------------------------

type cfgMetaKvNodeDefsSplitHandler struct{}

// get() retrieves multiple child entries from the metakv and weaves
// the results back into a composite nodeDefs.  get() must be invoked
// with c.m.Lock()'ed.
func (a *cfgMetaKvNodeDefsSplitHandler) get(
	c *CfgMetaKv, key string, cas uint64) ([]byte, uint64, error) {
	m, err := metakv.ListAllChildren(c.keyToPath(key) + "/")
	if err != nil {
		return nil, 0, err
	}

	rv := &NodeDefs{NodeDefs: make(map[string]*NodeDef)}

	uuids := []string{}
	for _, v := range m {
		var childNodeDefs NodeDefs

		value, err := c.uncompressLocked(v.Value)
		if err != nil {
			return nil, 0, err
		}

		err = UnmarshalJSON(value, &childNodeDefs)
		if err != nil {
			return nil, 0, err
		}

		for k1, v1 := range childNodeDefs.NodeDefs {
			rv.NodeDefs[k1] = v1
		}

		// use the lowest version among nodeDefs
		if rv.ImplVersion == "" ||
			!VersionGTE(childNodeDefs.ImplVersion, rv.ImplVersion) {
			rv.ImplVersion = childNodeDefs.ImplVersion
		}

		uuids = append(uuids, childNodeDefs.UUID)
	}
	rv.UUID = checkSumUUIDs(uuids)

	if rv.ImplVersion == "" {
		version := VERSION
		v, _, err := c.getRawLOCKED(VERSION_KEY, 0)
		if err == nil {
			version = string(v)
		}
		rv.ImplVersion = version
	}

	data, err := MarshalJSON(rv)
	if err != nil {
		return nil, 0, err
	}

	casResult := c.lastSplitCAS + 1
	c.lastSplitCAS = casResult

	c.splitEntries[key] = CfgMetaKvEntry{
		cas:  casResult,
		data: data,
	}

	return data, casResult, nil
}

// set() splits a nodeDefs into multiple child metakv entries and must
// be invoked with c.m.Lock()'ed.
func (a *cfgMetaKvNodeDefsSplitHandler) set(
	c *CfgMetaKv, key string, val []byte, cas uint64) (uint64, error) {
	path := c.keyToPath(key)

	curEntry := c.splitEntries[key]

	if cas != CFG_CAS_FORCE && cas != 0 && cas != curEntry.cas {
		log.Warnf("cfg_metakv: Set split, key: %v, cas mismatch: %x != %x",
			key, cas, curEntry.cas)

		return 0, &CfgCASError{}
	}

	var curNodeDefs NodeDefs

	if curEntry.data != nil && len(curEntry.data) > 0 {
		err := UnmarshalJSON(curEntry.data, &curNodeDefs)
		if err != nil {
			return 0, err
		}
	}

	var nd NodeDefs

	err := UnmarshalJSON(val, &nd)
	if err != nil {
		return 0, err
	}

	// Analyze which children were added, removed, updated.
	//
	added := map[string]bool{}
	removed := map[string]bool{}
	updated := map[string]bool{}

	for k, v := range nd.NodeDefs {
		if curNodeDefs.NodeDefs == nil ||
			curNodeDefs.NodeDefs[k] == nil {
			added[k] = true
		} else {
			if !reflect.DeepEqual(curNodeDefs.NodeDefs[k], v) {
				updated[k] = true
			}
		}
	}

	if curNodeDefs.NodeDefs != nil {
		for k := range curNodeDefs.NodeDefs {
			if nd.NodeDefs[k] == nil {
				removed[k] = true
			}
		}
	}

	log.Printf("cfg_metakv: Set split, key: %v,"+
		" added: %v, removed: %v, updated: %v",
		key, added, removed, updated)

LOOP:
	for k, v := range nd.NodeDefs {
		if cas != CFG_CAS_FORCE && c.nodeUUID != "" && c.nodeUUID != v.UUID {
			// If we have a nodeUUID, only add/update our
			// nodeDef, where other nodes will each add/update
			// only their own nodeDef's.
			log.Printf("cfg_metakv: Set split, key: %v,"+
				" skipping other node UUID: %v, self nodeUUID: %s",
				key, v.UUID, c.nodeUUID)

			continue LOOP
		}

		childNodeDefs := NodeDefs{
			UUID:        nd.UUID,
			NodeDefs:    map[string]*NodeDef{},
			ImplVersion: nd.ImplVersion,
		}
		childNodeDefs.NodeDefs[k] = v

		val, err = MarshalJSON(childNodeDefs)
		if err != nil {
			return 0, err
		}

		childPath := path + "/" + k

		log.Printf("cfg_metakv: Set split, key: %v, childPath: %v",
			key, childPath)

		val, err := c.compressLocked(val)
		if err != nil {
			return 0, err
		}

		err = metakv.Set(childPath, val, nil)
		if err != nil {
			return 0, err
		}
		if cas != CFG_CAS_FORCE {
			break LOOP
		}
	}

	// Remove composite children entries from metakv only if
	// caller was attempting removals only.  This should work as
	// the caller usually has read-compute-write logic that only
	// removes node defs and does not add/update node defs in the
	// same read-compute-write code path.
	//
	if len(added) <= 0 && len(updated) <= 0 && len(removed) > 0 {
		for nodeDefUUID := range removed {
			childPath := path + "/" + nodeDefUUID

			log.Printf("cfg_metakv: Set delete, childPath: %v", childPath)

			err = metakv.Delete(childPath, nil)
			if err != nil {
				return 0, err
			}
		}
	}

	casResult := c.lastSplitCAS + 1
	c.lastSplitCAS = casResult
	return casResult, err
}

func (a *cfgMetaKvNodeDefsSplitHandler) del(
	c *CfgMetaKv, key string, cas uint64) error {
	delete(c.splitEntries, key)

	path := c.keyToPath(key)

	return metakv.RecursiveDelete(path + "/")
}

// ----------------------------------------------------------------

type cfgMetaKvIndexDefsHandler struct{}

// set() stores the indexDefs as a single metakv entry; the CAS check
// is the rev based one that setRawLOCKED() runs against metakv.
// set() must be invoked with c.m.Lock()'ed.
func (a *cfgMetaKvIndexDefsHandler) set(
	c *CfgMetaKv, key string, val []byte, cas uint64) (uint64, error) {
	return c.setRawLOCKED(key, val, cas)
}

// get(..) retrieves entry from metakv for composite indexDefs;
// must be invoked with c.m.Lock()'ed.
func (a *cfgMetaKvIndexDefsHandler) get(
	c *CfgMetaKv, key string, cas uint64) ([]byte, uint64, error) {
	return c.getRawLOCKED(key, cas)
}

func (a *cfgMetaKvIndexDefsHandler) del(
	c *CfgMetaKv, key string, cas uint64) error {
	delete(c.splitEntries, key)

	path := c.keyToPath(key)

	return metakv.RecursiveDelete(path + "/")
}

// ----------------------------------------------------------------

// cfgMetaKvPlanPIndexesHandler invokes the appropriate plan handler
// depending on the feature availability
type cfgMetaKvPlanPIndexesHandler struct{}

func (a *cfgMetaKvPlanPIndexesHandler) get(c *CfgMetaKv,
	key string, cas uint64) ([]byte, uint64, error) {
	// if the cluster contains any nodes which dont support
	// the Lean planPIndex feature then fallback to older
	// shared PlanPIndex format
	if c.isFeatureSupported(LeanPlanVersion, NodeFeatureLeanPlan) {
		return getLeanPlan(c, key, cas)
	}
	return getSharedPlan(c, key, cas)
}

func (a *cfgMetaKvPlanPIndexesHandler) set(c *CfgMetaKv,
	key string, val []byte, cas uint64) (uint64, error) {
	// if the cluster contains any nodes which dont support
	// the feature Lean planPIndex feature then fallback to older
	// shared PlanPIndex format
	if c.isFeatureSupported(LeanPlanVersion, NodeFeatureLeanPlan) {
		return setLeanPlan(c, key, val, cas)
	}
	return setSharedPlan(c, key, val, cas)
}

func (a *cfgMetaKvPlanPIndexesHandler) del(c *CfgMetaKv,
	key string, cas uint64) error {
	// if the cluster contains any nodes which dont support
	// the feature Lean planPIndex feature then fallback to older
	// shared PlanPIndex format
	if c.isFeatureSupported(LeanPlanVersion, NodeFeatureLeanPlan) {
		return delLeanPlan(c, key, cas)
	}
	return delSharedPlan(c, key, cas)
}

// PlanPIndexesShared represents a PlanPIndexes that has been
// deduplicated into shared parts.
type PlanPIndexesShared struct {
	PlanPIndexes

	// Key is "indexType/indexName/indexUUID".
	SharedIndexDefs map[string]*PlanPIndexIndexDef `json:"sharedIndexDefs"`

	// Key is "sourceType/sourceName/sourceUUID".
	SharedSourceDefs map[string]*PlanPIndexSourceDef `json:"sharedSourceDefs"`
}

// PlanPIndexIndexDef represents the shared, repeated index definition
// part of a PlanPIndex.
type PlanPIndexIndexDef struct {
	IndexParams string `json:"indexParams"`
}

// PlanPIndexSourceDef represents the shared, repeated source
// definition part of a PlanPIndex.
type PlanPIndexSourceDef struct {
	SourceParams string `json:"sourceParams"`
}

func getSharedPlan(c *CfgMetaKv,
	key string, cas uint64) ([]byte, uint64, error) {
	buf, casResult, err := c.getRawLOCKED(key, cas)
	if err != nil || len(buf) <= 0 {
		return buf, casResult, err
	}

	var shared PlanPIndexesShared

	err = UnmarshalJSON(buf, &shared)
	if err != nil {
		return nil, 0, err
	}

	planPIndexes := &shared.PlanPIndexes

	// Expand/reconstitute the planPIndex's name.
	for ppiName, ppi := range planPIndexes.PlanPIndexes {
		if ppi.Name == "" {
			ppi.Name = ppiName
		}
	}

	// Expand/reconstitute the index def part of each planPIndex.
	if shared.SharedIndexDefs != nil {
		for _, ppi := range planPIndexes.PlanPIndexes {
			if ppi.IndexParams == "" {
				k := ppi.IndexType + "/" + ppi.IndexName + "/" + ppi.IndexUUID
				sharedPart, ok := shared.SharedIndexDefs[k]
				if ok && sharedPart != nil {
					ppi.IndexParams = sharedPart.IndexParams
				}
			}
		}
	}

	// Expand/reconstitute the source def part of each planPIndex.
	if shared.SharedSourceDefs != nil {
		for _, ppi := range planPIndexes.PlanPIndexes {
			if ppi.SourceParams == "" {
				k := ppi.SourceType + "/" + ppi.SourceName + "/" + ppi.SourceUUID +
					"/" + ppi.IndexName + "/" + ppi.IndexUUID
				sharedPart, ok := shared.SharedSourceDefs[k]
				if ok && sharedPart != nil {
					ppi.SourceParams = sharedPart.SourceParams
				}
			}
		}
	}

	bufResult, err := MarshalJSON(planPIndexes)
	if err != nil {
		return nil, 0, err
	}

	return bufResult, casResult, nil
}

// setSharedPlan rewrites the set of a planPIndex
// document by deduplicating repeated index definitions and source
// definitions.
func setSharedPlan(c *CfgMetaKv,
	key string, val []byte, cas uint64) (uint64, error) {
	var shared PlanPIndexesShared

	err := UnmarshalJSON(val, &shared.PlanPIndexes)
	if err != nil {
		return 0, err
	}

	shared.SharedIndexDefs = map[string]*PlanPIndexIndexDef{}
	shared.SharedSourceDefs = map[string]*PlanPIndexSourceDef{}

	// Reduce the planPIndexes by not repeating the shared parts.
	for _, ppi := range shared.PlanPIndexes.PlanPIndexes {
		ppi.Name = ""

		if ppi.IndexParams != "" {
			k := ppi.IndexType + "/" + ppi.IndexName + "/" + ppi.IndexUUID
			shared.SharedIndexDefs[k] = &PlanPIndexIndexDef{
				IndexParams: ppi.IndexParams,
			}
			ppi.IndexParams = ""
		}

		if ppi.SourceParams != "" {
			k := ppi.SourceType + "/" + ppi.SourceName + "/" + ppi.SourceUUID +
				"/" + ppi.IndexName + "/" + ppi.IndexUUID
			shared.SharedSourceDefs[k] = &PlanPIndexSourceDef{
				SourceParams: ppi.SourceParams,
			}
			ppi.SourceParams = ""
		}
	}

	valShared, err := MarshalJSON(&shared)
	if err != nil {
		return 0, err
	}

	return c.setRawLOCKED(key, valShared, cas)
}

func delSharedPlan(c *CfgMetaKv, key string, cas uint64) error {
	return c.delRawLOCKED(key, cas)
}

type Compatibility struct {
	// For cluster version tracker
	ClusterCompatibility int    `json:"clusterCompatibility"`
	Version              string `json:"version"`
}

type NsServerResponse struct {
	Nodes []Compatibility `json:"nodes"`
}

func (c *CfgMetaKv) ClusterVersion() (string, error) {
	if len(c.nsServerUrl) < 2 {
		return "", fmt.Errorf("cfg_metakv: no ns_server URL configured")
	}

	u, err := CBAuthURL(c.nsServerUrl)
	if err != nil {
		return "", fmt.Errorf("cfg_metakv: auth for ns_server,"+
			" nsServerURL: %s, authType: %s, err: %v",
			c.nsServerUrl, "cbauth", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return "", err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := HttpClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("cfg_metakv: error reading resp.Body,"+
			" nsServerURL: %s, resp: %#v, err: %v", c.nsServerUrl, resp, err)
	}

	rv := &NsServerResponse{}
	err = UnmarshalJSON(respBuf, rv)
	if err != nil {
		return "", fmt.Errorf("cfg_metakv: error parsing respBuf: %s,"+
			" nsServerURL: %s, err: %v", respBuf, c.nsServerUrl, err)

	}

	// Truncate version to be able to compare it with the CfgAppVersion
	// eg. 7.6.2-0000-enterprise becomes 7.6.2
	firstDashIndex := strings.Index(rv.Nodes[0].Version, "-")

	if firstDashIndex < 0 {
		// "dash" not found
		return "", fmt.Errorf("cfg_metakv: unknown version `%v`", rv.Nodes[0].Version)
	}

	return rv.Nodes[0].Version[:firstDashIndex], nil
}

func CompatibilityVersion(version string) (uint64, error) {
	eVersion := uint64(1)
	xa := strings.Split(version, ".")
	if len(xa) < 2 {
		return eVersion, fmt.Errorf("invalid version")
	}

	majVersion, err := strconv.Atoi(xa[0])
	if err != nil {
		return eVersion, err
	}

	minVersion, err := strconv.Atoi(xa[1])
	if err != nil {
		return eVersion, err
	}

	return uint64(65536*majVersion + minVersion), nil
}
