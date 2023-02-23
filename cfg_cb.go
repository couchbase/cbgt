//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/json"
	"net/url"
	"strings"
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
//
// urlStr: single URL or multiple URLs delimited by ';'
type CfgCB struct {
	m      sync.Mutex
	urlStr string
	url    *url.URL
	bucket string
	b      *couchbase.Bucket
	cfgKey string

	options map[string]interface{}

	logger func(format string, args ...interface{})

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
//
// urlStr: single URL or multiple URLs delimited by ';'
// bucket: couchbase bucket name
func NewCfgCB(urlStr, bucket string) (*CfgCB, error) {
	return NewCfgCBEx(urlStr, bucket, nil)
}

// NewCfgCBEx is a more advanced version of NewCfgCB(), with more
// initialization options via the options map.
//
// urlStr:  single URL or multiple URLs delimited by ';'
// bucket:  couchbase bucket name
// options: initialization options
//
// Allowed options include:
//   - 'keyPrefix':  an optional string prefix that's prepended to
//     to all keys that are written to or read from
//     the couchbase bucket.
//   - 'loggerDebug'
//   - 'loggerFunc'
func NewCfgCBEx(urlStr, bucket string,
	options map[string]interface{}) (*CfgCB, error) {
	urls := strings.Split(urlStr, ";")

	var url *url.URL
	var err error

	for _, u := range urls {
		url, err = couchbase.ParseURL(u)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	keyPrefix := ""
	logger := func(format string, args ...interface{}) {}

	if options != nil {
		v, exists := options["keyPrefix"]
		if exists {
			keyPrefix = v.(string)
		}

		loggeDebug, exists := options["loggerDebug"]
		if exists && loggeDebug.(bool) {
			logger = func(format string, args ...interface{}) {
				log.Printf(format, args...)
			}
		}

		loggerFunc, exists := options["loggerFunc"]
		if exists && loggerFunc != nil {
			logger = loggerFunc.(func(format string, args ...interface{}))
		}
	}

	c := &CfgCB{
		urlStr:  urlStr,
		url:     url,
		bucket:  bucket,
		cfgKey:  keyPrefix + "cfg",
		options: options,
		logger:  logger,

		subscriptions: make(map[string][]chan<- CfgEvent),
	}

	b, err := c.getBucket() // TODO: Need to b.Close()?
	if err != nil {
		return nil, err
	}

	// The following Add() ensures that we don't see CAS number 0, to
	// avoid situations where CAS number 0's alternate meaning of
	// "don't care" can lead to startup race issues.
	b.Add(c.cfgKey, 0, NewCfgMem())

	bucketUUID := ""
	vbucketIDs := []uint16{uint16(b.VBHash(c.cfgKey))}

	bds, err := cbdatasource.NewBucketDataSource(
		urls,
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
	c.logger("cfg_cb: Get, key: %s, cas: %d", key, cas)

	c.m.Lock()
	defer c.m.Unlock()

	bucket, err := c.getBucket()
	if err != nil {
		c.logger("cfg_cb: Get, key: %s, cas: %d, getBucket, err: %v", key, cas, err)

		return nil, 0, err
	}

	cfgBuf, _, cfgCAS, err := bucket.GetsRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		c.logger("cfg_cb: Get, key: %s, cas: %d, GetsRaw, err: %v", key, cas, err)

		return nil, 0, err
	}

	if cas != 0 && cas != cfgCAS {
		c.logger("cfg_cb: Get, key: %s, cas: %d, CASError, cfgCAS: %d", key, cas, cfgCAS)

		return nil, 0, &CfgCASError{}
	}

	cfgMem := NewCfgMem()
	if cfgBuf != nil {
		c.logger("cfg_cb: Get, key: %s, cas: %d, len(cfgBuf): %d", key, cas, len(cfgBuf))

		err = json.Unmarshal(cfgBuf, cfgMem)
		if err != nil {
			c.logger("cfg_cb: Get, key: %s, cas: %d, JSONError, err: %v", key, cas, err)

			return nil, 0, err
		}
	} else {
		c.logger("cfg_cb: Get, key: %s, cas: %d, cfgBuf nil", key, cas)
	}

	val, _, err := cfgMem.Get(key, 0)
	if err != nil {
		c.logger("cfg_cb: Get, key: %s, cas: %d, cfgMem.Get() err: %v", key, cas, err)

		return nil, 0, err
	}

	c.logger("cfg_cb: Get, key: %s, cas: %d, cfgCAS: %d, val: %s", key, cas, cfgCAS, val)

	return val, cfgCAS, nil
}

func (c *CfgCB) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	bucket, err := c.getBucket()
	if err != nil {
		c.logger("cfg_cb: Set, key: %s, cas: %d, getBucket, err: %v", key, cas, err)

		return 0, err
	}

	cfgBuf, _, cfgCAS, err := bucket.GetsRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		c.logger("cfg_cb: Set, key: %s, cas: %d, GetsRaw, err: %v", key, cas, err)

		return 0, err
	}

	if cas != 0 && cas != cfgCAS {
		c.logger("cfg_cb: Set, key: %s, cas: %d, CASError, cfgCAS: %d", key, cas, cfgCAS)

		return 0, &CfgCASError{}
	}

	cfgMem := NewCfgMem()
	if cfgBuf != nil {
		c.logger("cfg_cb: Set, key: %s, cas: %d, len(cfgBuf): %d", key, cas, len(cfgBuf))

		err = json.Unmarshal(cfgBuf, cfgMem)
		if err != nil {
			c.logger("cfg_cb: Set, key: %s, cas: %d, JSONError, err: %v", key, cas, err)

			return 0, err
		}
	} else {
		c.logger("cfg_cb: Get, key: %s, cas: %d, cfgBuf nil", key, cas)
	}

	_, err = cfgMem.Set(key, val, CFG_CAS_FORCE)
	if err != nil {
		c.logger("cfg_cb: Set, key: %s, cas: %d, cfgMem.Set() err: %v", key, cas, err)

		return 0, err
	}

	nextCAS, err := bucket.Cas(c.cfgKey, 0, cfgCAS, cfgMem)
	if err != nil {
		c.logger("cfg_cb: Set, key: %s, cas: %d, cfgCAS: %d, Cas err: %v",
			key, cas, cfgCAS, err)

		if res, ok := err.(*gomemcached.MCResponse); ok {
			if res.Status == gomemcached.KEY_EEXISTS {
				c.logger("cfg_cb: Set, key: %s, cas: %d, cfgCAS: %d, Cas KEY_EEXISTS",
					key, cas, cfgCAS)

				return 0, &CfgCASError{}
			}
		}
		return 0, err
	}

	c.logger("cfg_cb: Set, key: %s, cas: %d, cfgCAS: %d, nextCAS: %d",
		key, cas, cfgCAS, nextCAS)

	c.fireEvent(key, nextCAS, nil)

	return nextCAS, err
}

func (c *CfgCB) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	bucket, err := c.getBucket()
	if err != nil {
		c.logger("cfg_cb: Del, key: %s, cas: %d, getBucket, err: %v", key, cas, err)

		return err
	}

	cfgBuf, _, cfgCAS, err := bucket.GetsRaw(c.cfgKey)
	if err != nil && !gomemcached.IsNotFound(err) {
		c.logger("cfg_cb: Del, key: %s, cas: %d, GetsRaw, err: %v", key, cas, err)

		return err
	}

	if cas != 0 && cas != cfgCAS {
		c.logger("cfg_cb: Del, key: %s, cas: %d, CASError, cfgCAS: %d", key, cas, cfgCAS)

		return &CfgCASError{}
	}

	cfgMem := NewCfgMem()
	if cfgBuf != nil {
		c.logger("cfg_cb: Del, key: %s, cas: %d, len(cfgBuf): %d", key, cas, len(cfgBuf))

		err = json.Unmarshal(cfgBuf, cfgMem)
		if err != nil {
			c.logger("cfg_cb: Del, key: %s, cas: %d, JSONError, err: %v", key, cas, err)

			return err
		}
	} else {
		c.logger("cfg_cb: Del, key: %s, cas: %d, cfgBuf nil", key, cas)
	}

	err = cfgMem.Del(key, 0)
	if err != nil {
		c.logger("cfg_cb: Del, key: %s, cas: %d, cfgMem.Del() err: %v", key, cas, err)

		return err
	}

	nextCAS, err := bucket.Cas(c.cfgKey, 0, cfgCAS, cfgMem)
	if err != nil {
		c.logger("cfg_cb: Del, key: %s, cas: %d, cfgCAS: %d, Cas err: %v",
			key, cas, cfgCAS, err)

		if res, ok := err.(*gomemcached.MCResponse); ok {
			if res.Status == gomemcached.KEY_EEXISTS {
				c.logger("cfg_cb: Del, key: %s, cas: %d, cfgCAS: %d, Cas KEY_EEXISTS",
					key, cas, cfgCAS)

				return &CfgCASError{}
			}
		}
		return err
	}

	c.logger("cfg_cb: Del, key: %s, cas: %d, cfgCAS: %d, nextCAS: %d",
		key, cas, cfgCAS, nextCAS)

	c.fireEvent(key, nextCAS, nil)

	return err
}

func (c *CfgCB) getBucket() (*couchbase.Bucket, error) {
	if c.b == nil {
		// Retry until a connection can be made
		urls := strings.Split(c.urlStr, ";")

		var b *couchbase.Bucket
		var err error

		for _, url := range urls {
			b, err = couchbase.ConnectWithAuthAndGetBucket(url,
				"default", c.bucket, c)
			if err == nil {
				break
			}
		}

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
		if a.url.User.Username() != "" {
			user = a.url.User.Username()
		}
	}

	return user, pswd, a.bucket
}

// ----------------------------------------------------------------

func (r *CfgCB) OnError(err error) {
	log.Warnf("cfg_cb: OnError, err: %v", err)

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
