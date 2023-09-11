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
	"io"

	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached/client"
)

const SOURCE_GOCOUCHBASE_TAP = "couchbase-tap"

// DEST_EXTRAS_TYPE_TAP represents the extras that comes from TAP
// protocol.
const DEST_EXTRAS_TYPE_TAP = DestExtrasType(0x0001)

func init() {
	RegisterFeedType(SOURCE_GOCOUCHBASE_TAP,
		&FeedType{
			Start:      StartTAPFeed,
			Partitions: CouchbasePartitions,
			Public:     false,
			Description: "general/" + SOURCE_GOCOUCHBASE_TAP +
				" - Couchbase Server data source, via TAP protocol",
			StartSample: &TAPFeedParams{},
		})
}

// StartDCPFeed starts a TAP related feed and is registered at
// init/startup time with the system via RegisterFeedType().
func StartTAPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]Dest) error {
	server, poolName, bucketName :=
		CouchbaseParseSourceName(mgr.server, "default", sourceName)

	feed, err := NewTAPFeed(feedName, indexName, server, poolName,
		bucketName, bucketUUID, params, BasicPartitionFunc, dests,
		mgr.tagsMap != nil && !mgr.tagsMap["feed"])
	if err != nil {
		return fmt.Errorf("feed_tap: could not prepare TAP to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_tap: could not start, server: %s, err: %v",
			mgr.server, err)
	}
	err = mgr.registerFeed(feed)
	if err != nil {
		feed.Close()
		return err
	}
	return nil
}

// A TAPFeed implements the Feed interface and handles the TAP
// protocol to receive data from a couchbase data source.
type TAPFeed struct {
	name       string
	indexName  string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	params     *TAPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	closeCh    chan bool
	doneCh     chan bool
	doneErr    error
	doneMsg    string
}

// TAPFeedParams represents the JSON of the sourceParams for a TAP
// feed.
type TAPFeedParams struct {
	BackoffFactor float32 `json:"backoffFactor"`
	SleepInitMS   int     `json:"sleepInitMS"`
	SleepMaxMS    int     `json:"sleepMaxMS"`
}

// NewTAPFeed creates a new, ready-to-be-started TAPFeed.
func NewTAPFeed(name, indexName, url, poolName, bucketName, bucketUUID,
	paramsStr string, pf DestPartitionFunc, dests map[string]Dest,
	disable bool) (*TAPFeed, error) {
	params := &TAPFeedParams{}
	if paramsStr != "" {
		err := UnmarshalJSON([]byte(paramsStr), params)
		if err != nil {
			return nil, err
		}
	}

	return &TAPFeed{
		name:       name,
		indexName:  indexName,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		params:     params,
		pf:         pf,
		dests:      dests,
		disable:    disable,
		closeCh:    make(chan bool),
		doneCh:     make(chan bool),
		doneErr:    nil,
		doneMsg:    "",
	}, nil
}

func (t *TAPFeed) Name() string {
	return t.name
}

func (t *TAPFeed) IndexName() string {
	return t.indexName
}

func (t *TAPFeed) Start() error {
	if t.disable {
		log.Printf("feed_tap: disable, name: %s", t.Name())
		return nil
	}

	log.Printf("feed_tap: start, name: %s", t.Name())

	backoffFactor := t.params.BackoffFactor
	if backoffFactor <= 0.0 {
		backoffFactor = FEED_BACKOFF_FACTOR
	}
	sleepInitMS := t.params.SleepInitMS
	if sleepInitMS <= 0 {
		sleepInitMS = FEED_SLEEP_INIT_MS
	}
	sleepMaxMS := t.params.SleepMaxMS
	if sleepMaxMS <= 0 {
		sleepMaxMS = FEED_SLEEP_MAX_MS
	}

	go ExponentialBackoffLoop(t.Name(),
		func() int {
			progress, err := t.feed()
			if err != nil {
				log.Warnf("feed_tap: name: %s, progress: %d, err: %v",
					t.Name(), progress, err)
			}
			return progress
		},
		sleepInitMS, backoffFactor, sleepMaxMS)

	return nil
}

func (t *TAPFeed) feed() (int, error) {
	select {
	case <-t.closeCh:
		t.doneErr = nil
		t.doneMsg = "closeCh closed"
		close(t.doneCh)
		return -1, nil
	default:
	}

	bucket, err := couchbase.GetBucket(t.url, t.poolName, t.bucketName)
	if err != nil {
		return 0, err
	}
	defer bucket.Close()

	if t.bucketUUID != "" && t.bucketUUID != bucket.UUID {
		bucket.Close()
		return -1, fmt.Errorf("feed_tap: mismatched bucket uuid,"+
			"bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
			t.bucketName, t.bucketUUID, bucket.UUID)
	}

	args := memcached.TapArguments{}

	vbuckets, err := ParsePartitionsToVBucketIds(t.dests)
	if err != nil {
		return -1, err
	}
	if len(vbuckets) > 0 {
		args.VBuckets = vbuckets
	}

	feed, err := bucket.StartTapFeed(&args)
	if err != nil {
		return 0, err
	}
	defer feed.Close()

	// TODO: Maybe TAPFeed should do a rollback to zero if it finds it
	// needs to do a full backfill.
	// TODO: This TAPFeed implementation currently only works against
	// a couchbase cluster that has just a single node.

	log.Printf("feed_tap: running, url: %s,"+
		" poolName: %s, bucketName: %s, vbuckets: %#v",
		t.url, t.poolName, t.bucketName, vbuckets)

loop:
	for {
		select {
		case <-t.closeCh:
			t.doneErr = nil
			t.doneMsg = "closeCh closed"
			close(t.doneCh)
			return -1, nil

		case req, alive := <-feed.C:
			if !alive {
				break loop
			}

			log.Printf("feed_tap: received from url: %s,"+
				" poolName: %s, bucketName: %s, opcode: %s, req: %#v",
				t.url, t.poolName, t.bucketName, req.Opcode, req)

			partition, dest, err :=
				VBucketIdToPartitionDest(t.pf, t.dests, req.VBucket, req.Key)
			if err != nil {
				return 1, err
			}

			if req.Opcode == memcached.TapMutation {
				// TODO: TAP feed, what about flags, expiration, etc?
				err = dest.DataUpdate(partition, req.Key, 0, req.Value,
					req.Cas, DEST_EXTRAS_TYPE_NIL, nil)
			} else if req.Opcode == memcached.TapDeletion {
				// TODO: TAP feed, what about flags, expiration, etc?
				err = dest.DataDelete(partition, req.Key, 0,
					req.Cas, DEST_EXTRAS_TYPE_NIL, nil)
			}
			if err != nil {
				return 1, err
			}
		}
	}

	return 1, nil
}

func (t *TAPFeed) Close() error {
	select {
	case <-t.doneCh:
		return t.doneErr
	default:
	}

	close(t.closeCh)
	<-t.doneCh
	return t.doneErr
}

func (t *TAPFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *TAPFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}
