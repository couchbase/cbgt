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
	"net/url"
	"sync"

	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
)

// CfgCB is an implementation of Cfg that uses a couchbase bucket,
// and uses DCP to get change notifications.
//
// TODO: This current implementation is race-y!  Instead of storing
// everything as a single uber key/value, we should instead be storing
// individual key/value's on every get/set/del operation.
type CfgCB struct {
	m      sync.Mutex
	urlStr string
	url    *url.URL
	bucket string
	b      *couchbase.Bucket
	cfgKey string

	options map[string]interface{}

	subscriptions map[string][]chan<- CfgEvent // Keyed by key.

	bds  cbdatasource.BucketDataSource
	bdsm sync.Mutex
	seqs map[uint16]uint64 // To track max seq #'s we received per vbucketId.
	meta map[uint16][]byte // To track metadata blob's per vbucketId.
}

var cfgCBOptions = &cbdatasource.BucketDataSourceOptions{
	// TODO: Make these parametrized, using the CfgCB.options.
	ClusterManagerSleepMaxMS: 20000,
	DataManagerSleepMaxMS:    20000,
}

// NewCfgCB returns a Cfg implementation that reads/writes its entries
// from/to a couchbase bucket, using DCP streams to subscribe to
// changes.
func NewCfgCB(urlStr, bucket string) (*CfgCB, error) {
	return NewCfgCBEx(urlStr, bucket, nil)
}

// NewCfgCBEx is a more advanced version of NewCfgCB(), with more
// initialization options via the options map.  Allowed options:
// "keyPrefix" - an optional string prefix that's prepended to any
// keys that are written to or read from the couchbase bucket.
func NewCfgCBEx(urlStr, bucket string,
	options map[string]interface{}) (*CfgCB, error) {
	url, err := couchbase.ParseURL(urlStr)
	if err != nil {
		return nil, err
	}

	keyPrefix := ""
	if options != nil {
		v, exists := options["keyPrefix"]
		if exists {
			keyPrefix = v.(string)
		}
	}

	c := &CfgCB{
		urlStr:  urlStr,
		url:     url,
		bucket:  bucket,
		cfgKey:  keyPrefix + "cfg",
		options: options,

		subscriptions: make(map[string][]chan<- CfgEvent),
	}

	b, err := c.getBucket() // TODO: Need to b.Close()?
	if err != nil {
		return nil, err
	}

	bucketUUID := ""
	vbucketIDs := []uint16{uint16(b.VBHash(c.cfgKey))}

	bds, err := cbdatasource.NewBucketDataSource(
		[]string{urlStr},
		"default",
		bucket, bucketUUID, vbucketIDs, c, c, cfgCBOptions)
	if err != nil {
		return nil, err
	}
	c.bds = bds

	err = bds.Start()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *CfgCB) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	bucket, err := c.getBucket()
	if err != nil {
		return nil, 0, err
	}

	cfgBuf, _, cfgCAS, err := bucket.GetsRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		return nil, 0, err
	}

	if cas != 0 && cas != cfgCAS {
		return nil, 0, &CfgCASError{}
	}

	cfgMem := NewCfgMem()
	if cfgBuf != nil {
		err = json.Unmarshal(cfgBuf, cfgMem)
		if err != nil {
			return nil, 0, err
		}
	}

	val, _, err := cfgMem.Get(key, 0)
	if err != nil {
		return nil, 0, err
	}

	return val, cfgCAS, nil
}

func (c *CfgCB) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	bucket, err := c.getBucket()
	if err != nil {
		return 0, err
	}

	cfgBuf, _, cfgCAS, err := bucket.GetsRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		return 0, err
	}

	if cas != 0 && cas != cfgCAS {
		return 0, &CfgCASError{}
	}

	cfgMem := NewCfgMem()
	if cfgBuf != nil {
		err = json.Unmarshal(cfgBuf, cfgMem)
		if err != nil {
			return 0, err
		}
	}

	_, err = cfgMem.Set(key, val, CFG_CAS_FORCE)
	if err != nil {
		return 0, err
	}

	nextCAS, err := bucket.Cas(c.cfgKey, 0, cfgCAS, cfgMem)
	if err != nil {
		if res, ok := err.(*gomemcached.MCResponse); ok {
			if res.Status == gomemcached.KEY_EEXISTS {
				return 0, &CfgCASError{}
			}
		}
		return 0, err
	}

	c.fireEvent(key, nextCAS, nil)

	return nextCAS, err
}

func (c *CfgCB) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	bucket, err := c.getBucket()
	if err != nil {
		return err
	}

	cfgBuf, _, cfgCAS, err := bucket.GetsRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		return err
	}

	if cas != 0 && cas != cfgCAS {
		return &CfgCASError{}
	}

	cfgMem := NewCfgMem()
	if cfgBuf != nil {
		err = json.Unmarshal(cfgBuf, cfgMem)
		if err != nil {
			return err
		}
	}

	err = cfgMem.Del(key, 0)
	if err != nil {
		return err
	}

	nextCAS, err := bucket.Cas(c.cfgKey, 0, cfgCAS, cfgMem)
	if err != nil {
		if res, ok := err.(*gomemcached.MCResponse); ok {
			if res.Status == gomemcached.KEY_EEXISTS {
				return &CfgCASError{}
			}
		}
		return err
	}

	c.fireEvent(key, nextCAS, nil)

	return err
}

func (c *CfgCB) getBucket() (*couchbase.Bucket, error) {
	if c.b == nil {
		b, err := couchbase.GetBucket(c.urlStr, "default", c.bucket)
		if err != nil {
			return nil, err
		}
		c.b = b
	}
	return c.b, nil
}

func (c *CfgCB) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()

	a, exists := c.subscriptions[key]
	if !exists || a == nil {
		a = make([]chan<- CfgEvent, 0)
	}
	c.subscriptions[key] = append(a, ch)

	c.m.Unlock()

	return nil
}

func (c *CfgCB) FireEvent(key string, cas uint64, err error) {
	c.m.Lock()
	c.fireEvent(key, cas, err)
	c.m.Unlock()
}

func (c *CfgCB) fireEvent(key string, cas uint64, err error) {
	for _, c := range c.subscriptions[key] {
		go func(c chan<- CfgEvent) {
			c <- CfgEvent{
				Key: key, CAS: cas, Error: err,
			}
		}(c)
	}
}

func (c *CfgCB) Refresh() error {
	c.m.Lock()
	for key, cs := range c.subscriptions {
		event := CfgEvent{Key: key}
		for _, c := range cs {
			go func(c chan<- CfgEvent, event CfgEvent) {
				c <- event
			}(c, event)
		}
	}
	c.m.Unlock()
	return nil
}

// ----------------------------------------------------------------

func (a *CfgCB) GetCredentials() (string, string, string) {
	user := a.bucket
	pswd := ""

	if a.url != nil && a.url.User != nil {
		pswd, _ = a.url.User.Password()
	}

	return user, pswd, a.bucket
}

// ----------------------------------------------------------------

func (r *CfgCB) OnError(err error) {
	log.Printf("cfg_cb: OnError, err: %v", err)

	go func() {
		r.FireEvent("", 0, err)
	}()
}

func (r *CfgCB) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if string(key) == r.cfgKey {
		go r.Refresh()
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *CfgCB) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if string(key) == r.cfgKey {
		go r.Refresh()
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *CfgCB) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	return nil
}

func (r *CfgCB) SetMetaData(vbucketId uint16, value []byte) error {
	r.bdsm.Lock()

	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value

	r.bdsm.Unlock()

	return nil
}

func (r *CfgCB) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	r.bdsm.Lock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

	r.bdsm.Unlock()

	return value, lastSeq, nil
}

func (r *CfgCB) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	err := r.Refresh()
	if err != nil {
		return err
	}

	r.bdsm.Lock()
	r.seqs = nil
	r.meta = nil
	r.bdsm.Unlock()

	return nil
}

// ----------------------------------------------------------------

func (r *CfgCB) updateSeq(vbucketId uint16, seq uint64) {
	r.bdsm.Lock()
	defer r.bdsm.Unlock()

	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if r.seqs[vbucketId] < seq {
		r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().
	}
}
