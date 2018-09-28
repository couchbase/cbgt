//  Copyright (c) 2018 Couchbase, Inc.
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
	"fmt"
	"io"
	"sync"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocb"
	"gopkg.in/couchbase/gocbcore.v7"
)

func init() {
	RegisterFeedType("gocb", &FeedType{
		Start:           StartGocbDCPFeed,
		Partitions:      CBPartitions,
		PartitionSeqs:   CBPartitionSeqs,
		Stats:           CBStats,
		PartitionLookUp: CBVBucketLookUp,
		Public:          true,
		Description: "general/couchbase" +
			" - a Couchbase Server bucket will be the data source",
		StartSample: NewDCPFeedParams(),
	})
}

func StartGocbDCPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, sourceName, bucketUUID, params string,
	dests map[string]Dest) error {
	server, poolName, bucketName :=
		CouchbaseParseSourceName(mgr.server, "default", sourceName)

	feed, err := NewGocbDCPFeed(feedName, indexName, server, poolName,
		bucketName, bucketUUID, params, BasicPartitionFunc, dests,
		mgr.tagsMap != nil && !mgr.tagsMap["feed"], mgr)
	if err != nil {
		return fmt.Errorf("feed_dcp_gocb:"+
			" could not prepare DCP feed, server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_dcp_gocb:"+
			" could not start, server: %s, err: %v",
			mgr.server, err)
	}
	err = mgr.registerFeed(feed)
	if err != nil {
		feed.Close()
		return err
	}
	return nil
}

// A GocbDCPFeed implements both Feed and gocb.StreamObserver
// interfaces, and forwards any incoming gocb.StreamObserver
// callbacks to the relevant, hooked-up Dest instances.
//
// url: single URL or multiple URLs delimited by ';'
type GocbDCPFeed struct {
	name       string
	indexName  string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	paramsStr  string
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	stopAfter  map[string]UUIDSeq
	agent      *gocbcore.Agent
	mgr        *Manager
	auth       gocb.Authenticator

	m       sync.Mutex
	closed  bool
	lastErr error
	stats   *DestStats

	stopAfterReached map[string]bool // May be nil.
}

func NewGocbDCPFeed(name, indexName, url, poolName,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*DCPFeed, error) {
	log.Printf("feed_gocb_dcp: NewGocbDCPFeed, name: %s, indexName: %s",
		name, indexName)

	// TODO
	return nil, nil
}

func (t *GocbDCPFeed) Name() string {
	return t.name
}

func (t *GocbDCPFeed) IndexName() string {
	return t.indexName
}

func (t *GocbDCPFeed) Start() error {
	if t.disable {
		log.Printf("feed_gocb_dcp: disabled, name: %s", t.Name())
		return nil
	}

	log.Printf("feed_gocb_dcp: start, name: %s", t.Name())
	// TODO: Start the FEED
	return nil
}

func (t *GocbDCPFeed) Close() error {
	t.m.Lock()
	if t.closed {
		t.m.Unlock()
		return nil
	}
	t.closed = true
	t.m.Unlock()

	log.Printf("feed_gocb_dcp: close, name: %s", t.Name())
	//TODO: Close the Feed
	return nil
}

func (t *GocbDCPFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *GocbDCPFeed) Stats(w io.Writer) error {
	// TODO
	return nil
}

func (t *GocbDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, snapshotType gocbcore.SnapshotState) {
	// TODO
}

func (t *GocbDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	key, value []byte) {
	// TODO
}

func (t *GocbDCPFeed) Deletion(seqNo, revNo, cas uint64, datatype uint8,
	vbId uint16, key, value []byte) {
	// TODO
}

func (t *GocbDCPFeed) Expiration(seqNo, revNo, cas uint64, vbId uint16,
	key []byte) {
	// TODO
}

func (t *GocbDCPFeed) End(vbId uint16, err error) {
	// TODO
}
