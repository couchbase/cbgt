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

const (
	CFG_METAKV_BASE_PATH            = "/cbgt/cfg/"
	CFG_METAKV_NODEDEFS_WANTED_PATH = "/cbgt/nodeDefs/wanted/"
	CFG_METAKV_NODEDEFS_KNOWN_PATH  = "/cbgt/nodeDefs/known/"
)

var cfgMetaKvSplitKeys map[string]string = map[string]string{
	CfgNodeDefsKey(NODE_DEFS_WANTED): CFG_METAKV_NODEDEFS_WANTED_PATH,
	CfgNodeDefsKey(NODE_DEFS_KNOWN):  CFG_METAKV_NODEDEFS_KNOWN_PATH,
}

type CfgMetaKv struct {
	uuid     string
	path     string
	cancelCh chan struct{}

	m      sync.Mutex // Protects the fields that follow.
	cfgMem *CfgMem
}

// NewCfgMetaKv returns a CfgMetaKv that reads and stores its single
// configuration file in the metakv.
func NewCfgMetaKv() (*CfgMetaKv, error) {
	cfg := &CfgMetaKv{
		uuid:     NewUUID(),
		path:     CFG_METAKV_BASE_PATH,
		cancelCh: make(chan struct{}),
		cfgMem:   NewCfgMem(),
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
	defer c.m.Unlock()

	pathPrefix, needSplit := cfgMetaKvSplitKeys[key]
	if needSplit {
		m, err := metakv.ListAllChildren(pathPrefix)
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
				!VersionGTE(rv.ImplVersion, childNodeDefs.ImplVersion) {
				rv.ImplVersion = childNodeDefs.ImplVersion
			}

			uuids = append(uuids, childNodeDefs.UUID)
		}
		rv.UUID = checkSumUUIDs(uuids)

		data, _ := json.Marshal(rv)
		return data, 0, nil
	}

	return c.cfgMem.Get(key, cas)
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("cfg_metakv: Set, key: %v, cas: %x", key, cas)

	c.m.Lock()
	defer c.m.Unlock()

	pathPrefix, needSplit := cfgMetaKvSplitKeys[key]
	if needSplit {
		var nd NodeDefs

		err := json.Unmarshal(val, &nd)
		if err != nil {
			return 0, err
		}

		for k, v := range nd.NodeDefs {
			childNodeDefs := NodeDefs{
				UUID:     nd.UUID,
				NodeDefs: map[string]*NodeDef{
					k: v,
				},
				ImplVersion: nd.ImplVersion,
			}

			childPath := pathPrefix + k

			log.Printf("cfg_metakv: Set split key: %v, childPath: %v",
				key, childPath)

			val, _ = json.Marshal(childNodeDefs)

			err = metakv.Set(childPath, val, nil)
			if err != nil {
				return 0, err
			}
		}

		return cas, err
	}

	rev, err := c.cfgMem.GetRev(key, cas)
	if err != nil {
		return 0, err
	}

	if rev == nil {
		err = metakv.Add(c.keyToPath(key), val)
	} else {
		err = metakv.Set(c.keyToPath(key), val, rev)
	}
	if err != nil {
		return 0, err
	}

	return c.cfgMem.Set(key, val, CFG_CAS_FORCE)
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	err := c.delUnlocked(key, cas)
	c.m.Unlock()
	return err
}

func (c *CfgMetaKv) delUnlocked(key string, cas uint64) error {
	pathPrefix, needSplit := cfgMetaKvSplitKeys[key]
	if needSplit {
		return metakv.RecursiveDelete(pathPrefix)
	}

	rev, err := c.cfgMem.GetRev(key, cas)
	if err != nil {
		return err
	}

	err = metakv.Delete(c.keyToPath(key), rev)
	if err != nil {
		return err
	}

	return c.cfgMem.Del(key, 0)
}

func (c *CfgMetaKv) Load() error {
	metakv.IterateChildren(c.path, c.metaKVCallback)
	return nil
}

func (c *CfgMetaKv) metaKVCallback(path string,
	value []byte, rev interface{}) error {
	c.m.Lock()
	defer c.m.Unlock()

	key := c.pathToKey(path)

	if value == nil { // Deletion.
		return c.delUnlocked(key, 0)
	}

	log.Printf("cfg_metakv: metaKVCallback, path: %v, key: %v", path, key)

	cas, err := c.cfgMem.Set(key, value, CFG_CAS_FORCE)
	if err == nil {
		c.cfgMem.SetRev(key, cas, rev)
	}

	return err
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

func (c *CfgMetaKv) OnError(err error) {
	log.Printf("cfg_metakv: OnError, err: %v", err)
}

func (c *CfgMetaKv) RemoveAllKeys() {
	metakv.RecursiveDelete(c.path)
}

func (c *CfgMetaKv) keyToPath(key string) string {
	return c.path + key
}

func (c *CfgMetaKv) pathToKey(k string) string {
	return k[len(c.path):]
}

// for testing
func (c *CfgMetaKv) getChildrenKeys(key string) ([]string, error) {
	g := []string{}

	pathPrefix, needSplit := cfgMetaKvSplitKeys[key]
	if needSplit {
		m, err := metakv.ListAllChildren(pathPrefix)
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
