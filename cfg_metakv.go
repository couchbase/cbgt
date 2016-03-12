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
	"hash/crc32"
	"reflect"
	"sort"
	"sync"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbauth/metakv"
)

// The values stored in a Cfg are normally atomic, but CfgMetaKv has
// an exception for values related to NodeDefs.  In CfgMetaKv,
// NodeDefs are handled in "composite" fashion, where CfgMetaKv will
// split a NodeDefs value into zero or more child NodeDef values, each
// stored into metakv using a shared path prefix (so, similar to
// having a file-per-NodeDef in a shared directory).
//
// This allows concurrent, racing nodes to register their own entries
// into a composite NodeDefs "directory".  This can happen when
// multiple nodes are launched at nearly the same time, faster than
// metakv can replicate in eventually consistent fashion.
//
// Eventually, metakv replication will occur, and a reader will see a
// union of NodeDef values that CfgMetaKv reconstructs on the fly for
// the entire "directory".

// Prefix of paths stored in metakv, and should be immutable after
// process init()'ialization.
var CfgMetaKvPrefix = "/cbgt/cfg/"

type cfgMetaKvSplitHandler interface {
	get(c *CfgMetaKv, key string, cas uint64) ([]byte, uint64, error)
	set(c *CfgMetaKv, key string, val []byte, cas uint64) (uint64, error)
}

var cfgMetaKvSplitKeys map[string]cfgMetaKvSplitHandler = map[string]cfgMetaKvSplitHandler{
	CfgNodeDefsKey(NODE_DEFS_WANTED): &cfgMetaKvNodeDefsSplitHandler{},
	CfgNodeDefsKey(NODE_DEFS_KNOWN):  &cfgMetaKvNodeDefsSplitHandler{},
}

type CfgMetaKv struct {
	prefix   string // Prefix for paths stores in metakv.
	nodeUUID string // The uuid for this node.

	m            sync.Mutex // Protects the fields that follow.
	cfgMem       *CfgMem
	cancelCh     chan struct{}
	lastSplitCAS uint64
	splitEntries map[string]CfgMetaKvEntry
}

type CfgMetaKvEntry struct {
	cas  uint64
	data []byte
}

// NewCfgMetaKv returns a CfgMetaKv that reads and stores its single
// configuration file in the metakv.
func NewCfgMetaKv(nodeUUID string) (*CfgMetaKv, error) {
	cfg := &CfgMetaKv{
		prefix:       CfgMetaKvPrefix,
		nodeUUID:     nodeUUID,
		cfgMem:       NewCfgMem(),
		cancelCh:     make(chan struct{}),
		splitEntries: map[string]CfgMetaKvEntry{},
	}

	backoffStartSleepMS := 200
	backoffFactor := float32(1.5)
	backoffMaxSleepMS := 5000

	go ExponentialBackoffLoop("cfg_metakv.RunObserveChildren",
		func() int {
			err := metakv.RunObserveChildren(cfg.prefix, cfg.metaKVCallback,
				cfg.cancelCh)
			if err == nil {
				return -1 // Success, so stop the loop.
			}

			log.Printf("cfg_metakv: RunObserveChildren, err: %v", err)

			return 0 // No progress, so exponential backoff.
		},
		backoffStartSleepMS, backoffFactor, backoffMaxSleepMS)

	return cfg, nil
}

func (c *CfgMetaKv) Get(key string, cas uint64) ([]byte, uint64, error) {
	c.m.Lock()
	data, cas, err := c.getUnlocked(key, cas)
	c.m.Unlock()
	return data, cas, err
}

func (c *CfgMetaKv) getUnlocked(key string, cas uint64) ([]byte, uint64, error) {
	handler := cfgMetaKvSplitKeys[key]
	if handler != nil {
		return handler.get(c, key, cas)
	}

	path := c.keyToPath(key)

	v, _, err := metakv.Get(path) // TODO: Handle rev.
	if err != nil {
		return nil, 0, err
	}

	return v, 1, nil
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("cfg_metakv: Set, key: %v, cas: %x, split: %t, nodeUUID: %s",
		key, cas, cfgMetaKvSplitKeys[key] != nil, c.nodeUUID)

	c.m.Lock()
	defer c.m.Unlock()

	handler := cfgMetaKvSplitKeys[key]
	if handler != nil {
		return handler.set(c, key, val, cas)
	}

	path := c.keyToPath(key)

	log.Printf("cfg_metakv: Set path: %v", path)

	err := metakv.Set(path, val, nil) // TODO: Handle rev better.
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	err := c.delUnlocked(key, cas)
	c.m.Unlock()
	return err
}

func (c *CfgMetaKv) delUnlocked(key string, cas uint64) error {
	path := c.keyToPath(key)

	if cfgMetaKvSplitKeys[key] != nil {
		delete(c.splitEntries, key)

		return metakv.RecursiveDelete(path + "/")
	}

	return metakv.Delete(path, nil) // TODO: Handle rev better.
}

func (c *CfgMetaKv) Load() error {
	metakv.IterateChildren(c.prefix, c.metaKVCallback)
	return nil
}

func (c *CfgMetaKv) metaKVCallback(path string,
	value []byte, rev interface{}) error {
	key := c.pathToKey(path)

	log.Printf("cfg_metakv: metaKVCallback, path: %v, key: %v,"+
		" deletion: %t", path, key, value == nil)

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

	if cfgMetaKvSplitKeys[key] != nil {
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
	d, _ := json.Marshal(uuids)
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

		err = json.Unmarshal(v.Value, &childNodeDefs)
		if err != nil {
			return nil, 0, err
		}

		for k1, v1 := range childNodeDefs.NodeDefs {
			rv.NodeDefs[k1] = v1
		}

		if rv.ImplVersion == "" ||
			VersionGTE(childNodeDefs.ImplVersion, rv.ImplVersion) {
			rv.ImplVersion = childNodeDefs.ImplVersion
		}

		uuids = append(uuids, childNodeDefs.UUID)
	}
	rv.UUID = checkSumUUIDs(uuids)

	if rv.ImplVersion == "" {
		rv.ImplVersion = VERSION
	}

	data, err := json.Marshal(rv)
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

	if cas != 0 && cas != curEntry.cas {
		log.Printf("cfg_metakv: Set split, key: %v, cas mismatch: %x != %x",
			key, cas, curEntry.cas)

		return 0, &CfgCASError{}
	}

	var curNodeDefs NodeDefs

	if curEntry.data != nil && len(curEntry.data) > 0 {
		err := json.Unmarshal(curEntry.data, &curNodeDefs)
		if err != nil {
			return 0, err
		}
	}

	var nd NodeDefs

	err := json.Unmarshal(val, &nd)
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
		for k, _ := range curNodeDefs.NodeDefs {
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
		if c.nodeUUID != "" && c.nodeUUID != v.UUID {
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

		val, err = json.Marshal(childNodeDefs)
		if err != nil {
			return 0, err
		}

		childPath := path + "/" + k

		log.Printf("cfg_metakv: Set split, key: %v, childPath: %v",
			key, childPath)

		err = metakv.Set(childPath, val, nil)
		if err != nil {
			return 0, err
		}

		break LOOP
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
