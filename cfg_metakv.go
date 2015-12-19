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
	BASE_CFG_PATH            = "/cbgt/cfg/"
	CFG_NODEDEFS_WANTED_PATH = "/cbgt/nodeDefs/wanted/"
	CFG_NODEDEFS_KNOWN_PATH  = "/cbgt/nodeDefs/known/"
)

var splitKeys map[string]string = map[string]string{
	CfgNodeDefsKey(NODE_DEFS_WANTED): CFG_NODEDEFS_WANTED_PATH,
	CfgNodeDefsKey(NODE_DEFS_KNOWN):  CFG_NODEDEFS_KNOWN_PATH,
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
		path:     BASE_CFG_PATH,
		cancelCh: make(chan struct{}),
		cfgMem:   NewCfgMem(),
		uuid:     NewUUID(),
	}
	go func() {
		for {
			err := metakv.RunObserveChildren(cfg.path, cfg.metaKVCallback,
				cfg.cancelCh)
			if err == nil {
				return
			} else {
				log.Printf("metakv notifier failed (%v)", err)
			}
		}
	}()
	return cfg, nil
}

func (c *CfgMetaKv) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()
	if splitRequired(key) {
		rv := &NodeDefs{NodeDefs: make(map[string]*NodeDef)}
		tmp := &NodeDefs{}
		m, err := metakv.ListAllChildren(splitKeys[key])
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
		rv.UUID = getCksum(uuids)
		data, _ := json.Marshal(rv)
		return data, 0, nil
	}
	return c.cfgMem.Get(key, cas)
}

func (c *CfgMetaKv) SetKey(key string, val []byte, cas uint64, cache bool) (
	uint64, error) {
	var err error
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
		if err == nil {
			cas, err = c.cfgMem.Set(key, val, CFG_CAS_FORCE)
			if err != nil {
				return 0, err
			}
		}
	} else {
		return 0, metakv.Set(c.makeKey(key), val, nil)
	}
	return cas, err
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	log.Printf("setting key %v", key)
	c.m.Lock()
	defer c.m.Unlock()
	var err error
	if splitRequired(key) {
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
			log.Printf("splitted key %v", k)
			cas, err = c.SetKey(k, val, cas, false)
		}
	} else {
		cas, err = c.SetKey(key, val, cas, true)
	}
	return cas, err
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()
	return c.delUnlocked(key, cas)
}

func (c *CfgMetaKv) delUnlocked(key string, cas uint64) error {
	var err error
	if splitRequired(key) {
		return metakv.RecursiveDelete(splitKeys[key])
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
	log.Printf("callback got key %v", key)
	cas, err := c.cfgMem.Set(key, value, CFG_CAS_FORCE)
	if err == nil {
		c.cfgMem.SetRev(key, cas, rev)
	}
	return err
}

func getCksum(uuids []string) string {
	sort.Strings(uuids)
	d, _ := json.Marshal(uuids)
	return fmt.Sprint(crc32.ChecksumIEEE(d))
}

func splitRequired(key string) bool {
	for k, _ := range splitKeys {
		if strings.HasPrefix(key, k) {
			return true
		}
	}
	return false
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
	for k, v := range splitKeys {
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
	if splitRequired(k) {
		m, err := metakv.ListAllChildren(splitKeys[k])
		if err != nil {
			return nil, err
		}
		for _, v := range m {
			g = append(g, v.Path)
		}
	}
	return g, nil
}
