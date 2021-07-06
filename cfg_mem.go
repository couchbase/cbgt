//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"fmt"
	"math"
	"sync"
)

const (
	CFG_CAS_FORCE = math.MaxUint64
)

// CfgMem is a local-only, memory-only implementation of Cfg
// interface that's useful for development and testing.
type CfgMem struct {
	m             sync.Mutex
	CASNext       uint64
	Entries       map[string]*CfgMemEntry
	subscriptions map[string][]chan<- CfgEvent // Keyed by key.
}

// CfgMemEntry is a CAS-Val pairing tracked by CfgMem.
type CfgMemEntry struct {
	CAS uint64
	Val []byte
	rev interface{}
}

// NewCfgMem returns an empty CfgMem instance.
func NewCfgMem() *CfgMem {
	return &CfgMem{
		CASNext:       1,
		Entries:       make(map[string]*CfgMemEntry),
		subscriptions: make(map[string][]chan<- CfgEvent),
	}
}

func (c *CfgMem) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	entry, exists := c.Entries[key]
	if !exists {
		return nil, 0, nil
	}
	if cas != 0 && cas != entry.CAS {
		return nil, 0, &CfgCASError{}
	}

	val := make([]byte, len(entry.Val))
	copy(val, entry.Val)
	return val, entry.CAS, nil
}

func (c *CfgMem) GetRev(key string, cas uint64) (
	interface{}, error) {
	c.m.Lock()
	defer c.m.Unlock()

	entry, exists := c.Entries[key]
	if exists {
		if cas == 0 || cas == entry.CAS {
			return entry.rev, nil
		}
		return nil, &CfgCASError{}
	}
	return nil, nil
}

func (c *CfgMem) SetRev(key string, cas uint64, rev interface{}) error {
	c.m.Lock()
	defer c.m.Unlock()

	entry, exists := c.Entries[key]
	if cas == 0 || (exists && cas == entry.CAS) {
		entry.rev = rev
		c.Entries[key] = entry
		return nil
	}
	return &CfgCASError{}
}

func (c *CfgMem) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	prevEntry, exists := c.Entries[key]
	switch {
	case cas == 0:
		if exists {
			return 0, fmt.Errorf("cfg_mem: entry already exists, key: %s", key)
		}
	case cas == CFG_CAS_FORCE:
		break
	default:
		if !exists || cas != prevEntry.CAS {
			return 0, &CfgCASError{}
		}
	}

	nextEntry := &CfgMemEntry{
		CAS: c.CASNext,
		Val: make([]byte, len(val)),
	}
	copy(nextEntry.Val, val)
	c.Entries[key] = nextEntry
	c.CASNext += 1
	c.fireEvent(key, nextEntry.CAS, nil)
	return nextEntry.CAS, nil
}

func (c *CfgMem) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	if cas != 0 {
		entry, exists := c.Entries[key]
		if !exists || cas != entry.CAS {
			return &CfgCASError{}
		}
	}
	delete(c.Entries, key)
	c.fireEvent(key, 0, nil)
	return nil
}

func (c *CfgMem) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	a, exists := c.subscriptions[key]
	if !exists || a == nil {
		a = make([]chan<- CfgEvent, 0)
	}
	c.subscriptions[key] = append(a, ch)
	return nil
}

func (c *CfgMem) FireEvent(key string, cas uint64, err error) {
	c.m.Lock()
	c.fireEvent(key, cas, err)
	c.m.Unlock()
}

func (c *CfgMem) fireEvent(key string, cas uint64, err error) {
	for _, c := range c.subscriptions[key] {
		go func(c chan<- CfgEvent) {
			c <- CfgEvent{
				Key: key, CAS: cas, Error: err,
			}
		}(c)
	}
}

func (c *CfgMem) Refresh() error {
	c.m.Lock()
	defer c.m.Unlock()

	for key := range c.subscriptions {
		entry, exists := c.Entries[key]
		if exists && entry != nil {
			c.fireEvent(key, entry.CAS, nil)
		} else {
			c.fireEvent(key, 0, nil)
		}
	}

	return nil
}
