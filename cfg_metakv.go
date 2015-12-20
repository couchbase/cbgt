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
	"strings"
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
		path:     CFG_METAKV_BASE_PATH,
		cancelCh: make(chan struct{}),
		cfgMem:   NewCfgMem(),
		uuid:     NewUUID(),
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

func (c *CfgMetaKv) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()
	if cfgMetaKvSplitKeyRequired(key) {
		rv := &NodeDefs{NodeDefs: make(map[string]*NodeDef)}
		tmp := &NodeDefs{}
		m, err := metakv.ListAllChildren(cfgMetaKvSplitKeys[key])
		if err != nil {
			return nil, 0, err
		}
		uuids := []string{}
		for _, v := range m {
			err = json.Unmarshal(v.Value, tmp)
			if err != nil {
				return nil, 0, err
			}
			for k1, v1 := range tmp.NodeDefs {
				rv.NodeDefs[k1] = v1
			}
			uuids = append(uuids, tmp.UUID)
			rv.ImplVersion = tmp.ImplVersion
		}
		rv.UUID = checkSumUUIDs(uuids)
		data, _ := json.Marshal(rv)
		return data, 0, nil
	}
	return c.cfgMem.Get(key, cas)
}

func (c *CfgMetaKv) SetKey(key string, val []byte, cas uint64, cache bool) (
	uint64, error) {
	if cache {
		rev, err := c.cfgMem.GetRev(key, cas)
		if err != nil {
			return 0, err
		}

		if rev == nil {
			err = metakv.Add(c.makeKey(key), val)
		} else {
			err = metakv.Set(c.makeKey(key), val, rev)
		}
		if err != nil {
			return 0, err
		}

		return c.cfgMem.Set(key, val, CFG_CAS_FORCE)
	}

	return 0, metakv.Set(c.makeKey(key), val, nil)
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("cfg_metakv: Set, key: %v, cas: %x", key, cas)

	c.m.Lock()
	defer c.m.Unlock()

	var err error
	if cfgMetaKvSplitKeyRequired(key) {
		// split the keys
		nd := &NodeDefs{}
		err = json.Unmarshal(val, nd)
		if err != nil {
			return 0, err
		}
		for k, v := range nd.NodeDefs {
			n := &NodeDefs{
				UUID:        nd.UUID,
				NodeDefs:    make(map[string]*NodeDef),
				ImplVersion: nd.ImplVersion,
			}
			n.NodeDefs[k] = v
			k = key + "_" + k
			val, _ = json.Marshal(n)
			log.Printf("cfg_metakv: Set split key: %v, k: %v", key, k)
			cas, err = c.SetKey(k, val, cas, false)
		}
	} else {
		cas, err = c.SetKey(key, val, cas, true)
	}
	return cas, err
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	err := c.delUnlocked(key, cas)
	c.m.Unlock()
	return err
}

func (c *CfgMetaKv) delUnlocked(key string, cas uint64) error {
	var err error
	if cfgMetaKvSplitKeyRequired(key) {
		return metakv.RecursiveDelete(cfgMetaKvSplitKeys[key])
	}
	rev, err := c.cfgMem.GetRev(key, cas)
	if err != nil {
		return err
	}
	err = metakv.Delete(c.makeKey(key), rev)
	if err == nil {
		return c.cfgMem.Del(key, 0)
	}
	return err
}

func (c *CfgMetaKv) Load() error {
	metakv.IterateChildren(c.path, c.metaKVCallback)
	return nil
}

func (c *CfgMetaKv) metaKVCallback(path string, value []byte, rev interface{}) error {
	c.m.Lock()
	defer c.m.Unlock()

	key := c.getMetaKey(path)
	if value == nil {
		// key got deleted
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
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgMetaKv) Refresh() error {
	return nil
}

func (c *CfgMetaKv) OnError(err error) {
	log.Printf("cfg_metakv: OnError, err: %v", err)
}

func (c *CfgMetaKv) DelConf() {
	metakv.RecursiveDelete(c.path)
}

func (c *CfgMetaKv) makeKey(key string) string {
	for k, v := range cfgMetaKvSplitKeys {
		if strings.HasPrefix(key, k) {
			return v + key
		}
	}
	return c.path + key
}

func (c *CfgMetaKv) getMetaKey(k string) string {
	return k[len(c.path):]
}

// for testing
func (c *CfgMetaKv) getAllKeys(k string) ([]string, error) {
	g := []string{}
	if cfgMetaKvSplitKeyRequired(k) {
		m, err := metakv.ListAllChildren(cfgMetaKvSplitKeys[k])
		if err != nil {
			return nil, err
		}
		for _, v := range m {
			g = append(g, v.Path)
		}
	}
	return g, nil
}

func cfgMetaKvSplitKeyRequired(key string) bool {
	for k, _ := range cfgMetaKvSplitKeys {
		if strings.HasPrefix(key, k) {
			return true
		}
	}
	return false
}

func checkSumUUIDs(uuids []string) string {
	sort.Strings(uuids)
	d, _ := json.Marshal(uuids)
	return fmt.Sprint(crc32.ChecksumIEEE(d))
}
