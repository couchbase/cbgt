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
	cfgMem *CfgMem
	cfgKey string

	options map[string]interface{}

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
		keyPrefix = options["keyPrefix"].(string)
	}

	c := &CfgCB{
		urlStr:  urlStr,
		url:     url,
		bucket:  bucket,
		cfgMem:  NewCfgMem(),
		cfgKey:  keyPrefix + "cfg",
		options: options,
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

// DEPRECATED: Please instead use the NewCfgCBEx() API with an options
// map entry of "keyPrefix".
//
// The old docs for existing SetKeyPrefix users: SetKeyPrefix changes
// the key prefix that the CfgCB will use as it reads/writes its
// documents to the couchbase bucket (default key prefix is "").  Use
// SetKeyPrefix with care, as you must arrange all nodes in the
// cluster to use the same key prefix.  The SetKeyPrefix should be
// used right after NewCfgCB.
func (c *CfgCB) SetKeyPrefix(keyPrefix string) {
	if c.options == nil {
		c.options = map[string]interface{}{}
	}
	c.options["keyPrefix"] = keyPrefix
	c.Refresh()
}

func (c *CfgCB) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Get(key, cas)
}

func (c *CfgCB) Set(key string, val []byte, cas uint64) (
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

func (c *CfgCB) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	err := c.cfgMem.Del(key, cas)
	if err != nil {
		return err
	}
	return c.unlockedSave()
}

func (c *CfgCB) Load() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.unlockedLoad()
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

func (c *CfgCB) unlockedLoad() error {
	bucket, err := c.getBucket()
	if err != nil {
		return err
	}

	buf, err := bucket.GetRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		return err
	}

	cfgMem := NewCfgMem()
	if buf != nil {
		err = json.Unmarshal(buf, cfgMem)
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

func (c *CfgCB) unlockedSave() error {
	bucket, err := c.getBucket()
	if err != nil {
		return err
	}

	return bucket.Set(c.cfgKey, 0, c.cfgMem)
}

func (c *CfgCB) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgCB) Refresh() error {
	c.m.Lock()
	defer c.m.Unlock()

	c2, err := NewCfgCBEx(c.urlStr, c.bucket, c.options)
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
		r.cfgMem.FireEvent("", 0, err)
	}()
}

func (r *CfgCB) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if string(key) == r.cfgKey {
		go func() {
			r.Load()
			r.cfgMem.Refresh()
		}()
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *CfgCB) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if string(key) == r.cfgKey {
		go func() {
			r.Load()
			r.cfgMem.Refresh()
		}()
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
	defer r.bdsm.Unlock()

	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value

	return nil
}

func (r *CfgCB) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	r.bdsm.Lock()
	defer r.bdsm.Unlock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

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
