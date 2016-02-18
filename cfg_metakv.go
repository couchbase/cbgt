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

const (
	CFG_METAKV_PREFIX = "/cbgt/cfg/" // Prefix of paths stored in metakv.
)

var cfgMetaKvSplitKeys map[string]bool = map[string]bool{
	CfgNodeDefsKey(NODE_DEFS_WANTED): true,
	CfgNodeDefsKey(NODE_DEFS_KNOWN):  true,
}

type CfgMetaKv struct {
	prefix string // Prefix for paths stores in metakv.

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
func NewCfgMetaKv() (*CfgMetaKv, error) {
	cfg := &CfgMetaKv{
		prefix:       CFG_METAKV_PREFIX,
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
	if cfgMetaKvSplitKeys[key] {
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

	path := c.keyToPath(key)

	v, _, err := metakv.Get(path) // TODO: Handle rev.
	if err != nil {
		return nil, 0, err
	}

	return v, 1, nil
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("cfg_metakv: Set, key: %v, cas: %x", key, cas)

	path := c.keyToPath(key)

	c.m.Lock()
	defer c.m.Unlock()

	if cfgMetaKvSplitKeys[key] {
		curEntry := c.splitEntries[key]

		if cas != 0 && cas != curEntry.cas {
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
				if curNodeDefs.NodeDefs[k].UUID != v.UUID {
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

		// Update metakv with child entries.
		//
		if len(added) > 0 || len(updated) > 0 {
			for k, v := range nd.NodeDefs {
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

				log.Printf("cfg_metakv: Set split key: %v, childPath: %v",
					key, childPath)

				err = metakv.Set(childPath, val, nil)
				if err != nil {
					return 0, err
				}
			}
		}

		// Remove composite children entries from metakv only if
		// caller was attempting removals only.  This should work as
		// the caller usually has read-modify-write logic that only
		// removes node defs and does not add/update node defs in the
		// same read-modify-write code path.
		//
		if len(added) <= 0 && len(updated) <= 0 && len(removed) > 0 {
			for nodeDefUUID := range removed {
				err = metakv.Delete(path+"/"+nodeDefUUID, nil)
				if err != nil {
					return 0, err
				}
			}
		}

		casResult := c.lastSplitCAS + 1
		c.lastSplitCAS = casResult
		return casResult, err
	}

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

	if cfgMetaKvSplitKeys[key] {
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
	c.m.Lock()
	defer c.m.Unlock()

	key := c.pathToKey(path)

	log.Printf("cfg_metakv: metaKVCallback, path: %v, key: %v,"+
		" deletion: %t", path, key, value == nil)

	return c.cfgMem.Refresh()
}

func (c *CfgMetaKv) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	err := c.cfgMem.Subscribe(key, ch)
	c.m.Unlock()
	return err
}

func (c *CfgMetaKv) Refresh() error {
	return nil
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

	if cfgMetaKvSplitKeys[key] {
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
