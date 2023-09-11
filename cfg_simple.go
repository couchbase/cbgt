//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"os"
	"sync"
)

// CfgSimple is a local-only, persisted (in a single file)
// implementation of the Cfg interface that's useful for
// non-clustered, single-node instances for developers.
type CfgSimple struct {
	m      sync.Mutex
	path   string
	cfgMem *CfgMem
}

// NewCfgSimple returns a CfgSimple that reads and stores its single
// configuration file in the provided file path.
func NewCfgSimple(path string) *CfgSimple {
	return &CfgSimple{
		path:   path,
		cfgMem: NewCfgMem(),
	}
}

func (c *CfgSimple) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Get(key, cas)
}

func (c *CfgSimple) Set(key string, val []byte, cas uint64) (
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

func (c *CfgSimple) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	err := c.cfgMem.Del(key, cas)
	if err != nil {
		return err
	}
	return c.unlockedSave()
}

func (c *CfgSimple) Load() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.unlockedLoad()
}

func (c *CfgSimple) unlockedLoad() error {
	buf, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}

	cfgMem := NewCfgMem()
	err = UnmarshalJSON(buf, cfgMem)
	if err != nil {
		return err
	}

	c.cfgMem = cfgMem
	return nil
}

func (c *CfgSimple) unlockedSave() error {
	buf, err := MarshalJSON(c.cfgMem)
	if err != nil {
		return err
	}
	return os.WriteFile(c.path, []byte(string(buf)+"\n"), 0600)
}

func (c *CfgSimple) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgSimple) Refresh() error {
	c.m.Lock()
	defer c.m.Unlock()

	c2 := NewCfgSimple(c.path)
	err := c2.Load()
	if err != nil {
		return err
	}

	c.cfgMem.CASNext = c2.cfgMem.CASNext
	c.cfgMem.Entries = c2.cfgMem.Entries

	return c.cfgMem.Refresh()
}
