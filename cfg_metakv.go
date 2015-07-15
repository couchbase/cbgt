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
	"github.com/couchbase/cbauth/metakv"
	log "github.com/couchbase/clog"
	"sync"
)

const (
	BASE_CFG_PATH = "/cbft/cfg"
)

type CfgMetaKv struct {
	m        sync.Mutex
	cfgMem   *CfgMem
	path     string
	cancelCh chan struct{}
}

// NewCfgMetaKv returns a CfgMetaKv that reads and stores its single
// configuration file in the metakv.
func NewCfgMetaKv() (*CfgMetaKv, error) {
	cfg := &CfgMetaKv{
		path:     BASE_CFG_PATH,
		cancelCh: make(chan struct{}),
		cfgMem:   NewCfgMem(),
	}
	go func() {
		for {
			err := metakv.RunObserveChildren("/", cfg.metaKVCallback, cfg.cancelCh)
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

	return c.cfgMem.Get(key, cas)
}

func (c *CfgMetaKv) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	cas, err := c.cfgMem.Set(key, val, cas)
	if err != nil {
		return 0, err
	}

	err = c.unlockedSave()
	if err != nil {
		return 0, err
	}
	return cas, err
}

func (c *CfgMetaKv) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	err := c.cfgMem.Del(key, cas)
	if err != nil {
		return err
	}
	return c.unlockedSave()
}

func (c *CfgMetaKv) Load() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.unlockedLoad()
}

func (c *CfgMetaKv) unlockedLoad() error {
	value, _, err := metakv.Get(c.getCfgKey())
	if err != nil {
		return err
	}
	cfgMem := NewCfgMem()
	if value != nil {
		err = json.Unmarshal(value, cfgMem)
		if err != nil {
			return err
		}
	}
	cfgMemPrev := c.cfgMem
	cfgMemPrev.m.Lock()
	defer cfgMemPrev.m.Unlock()
	cfgMem.subscriptions = cfgMemPrev.subscriptions
	c.cfgMem = cfgMem
	return nil
}

func (c *CfgMetaKv) unlockedSave() error {
	var err error
	var value []byte
	if value, err = json.Marshal(c.cfgMem); err != nil {
		return err
	}
	return metakv.Set(c.getCfgKey(), value, nil)
}

func (c *CfgMetaKv) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgMetaKv) metaKVCallback(path string, value []byte, rev interface{}) error {
	if path == c.path {
		c.Load()
		c.cfgMem.Refresh()
	}
	return nil
}

func (c *CfgMetaKv) Refresh() error {
	c.m.Lock()
	defer c.m.Unlock()

	c2, err := NewCfgMetaKv()
	if err != nil {
		return err
	}

	err = c2.Load()
	if err != nil {
		return err
	}

	c.cfgMem.CASNext = c2.cfgMem.CASNext
	c.cfgMem.Entries = c2.cfgMem.Entries

	return c.cfgMem.Refresh()
}

func (c *CfgMetaKv) OnError(err error) {
	log.Printf("cfg_metakv: OnError, err: %v", err)
}

func (c *CfgMetaKv) getCfgKey() string {
	return c.path
}

func (c *CfgMetaKv) DelConf() {
	metakv.Delete(c.path, nil)
}
