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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"
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

	m       sync.Mutex
	closed  bool
	lastErr error
	stats   *DestStats

	stopAfterReached map[string]bool // May be nil.
}

func NewGocbDCPFeed(name, indexName, url, poolName,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool, mgr *Manager) (*GocbDCPFeed, error) {
	log.Printf("feed_gocb_dcp: NewGocbDCPFeed, name: %s, indexName: %s",
		name, indexName)

	var stopAfter map[string]UUIDSeq

	params := NewDCPFeedParams()

	if paramsStr != "" {
		err := json.Unmarshal([]byte(paramsStr), params)
		if err != nil {
			return nil, err
		}

		stopAfterSourceParams := StopAfterSourceParams{}
		err = json.Unmarshal([]byte(paramsStr), &stopAfterSourceParams)
		if err != nil {
			return nil, err
		}

		if stopAfterSourceParams.StopAfter == "markReached" {
			stopAfter = stopAfterSourceParams.MarkPartitionSeqs
		}
	}

	// If feedBufferSizeBytes (to enable DCP flow control) isn't
	// set, initialize it to the default.
	if !strings.Contains(paramsStr, "feedBufferSizeBytes") {
		params.FeedBufferSizeBytes = DCPFeedBufferSizeBytes
	}

	// If feedBufferAckThreshold (acking for flow control) isn't
	// set, initialize it to the default.
	if !strings.Contains(paramsStr, "feedBufferAckThreshold") {
		params.FeedBufferAckThreshold = DCPFeedBufferAckThreshold
	}

	// If noopTimeIntervalSecs (to enable DCP noops) isn't
	// set, initialize it to the default.
	if !strings.Contains(paramsStr, "noopTimeIntervalSecs") {
		params.NoopTimeIntervalSecs = DCPNoopTimeIntervalSecs
	}

	urls := strings.Split(url, ";")
	if len(urls) <= 0 {
		return nil, fmt.Errorf("feed_gocb_dcp: NewGocbDCPFeed, no urls provided")
	}

	feed := &GocbDCPFeed{
		name:       name,
		indexName:  indexName,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		paramsStr:  paramsStr,
		params:     params,
		pf:         pf,
		dests:      dests,
		disable:    disable,
		stopAfter:  stopAfter,
		mgr:        mgr,
		stats:      NewDestStats(),
	}

	// FIXME: Need to add authenticator

	config := &gocbcore.AgentConfig{
		UserString:           name,
		BucketName:           bucketName,
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
	}

	spec := urls[0]
	// FIXME: Get CA cert file from somewhere
	// spec += "?certpath=" + CACERTFILE

	err := config.FromConnStr(spec)
	if err != nil {
		return nil, err
	}

	// FIXME: Plugging in DCP options: NoopTimeInterval, BufferAckThreshold, etc.

	flags := gocbcore.DcpOpenFlagProducer | gocbcore.DcpOpenFlagIncludeXattrs

	feed.agent, err = gocbcore.CreateDcpAgent(config, name, flags)
	if err != nil {
		return nil, err
	}

	return feed, nil
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) Name() string {
	return f.name
}

func (f *GocbDCPFeed) IndexName() string {
	return f.indexName
}

func (f *GocbDCPFeed) Start() error {
	if f.disable {
		log.Printf("feed_gocb_dcp: disabled, name: %s", f.Name())
		return nil
	}

	log.Printf("feed_gocb_dcp: start, name: %s", f.Name())

	vbucketIds, err := ParsePartitionsToVBucketIds(f.dests)
	if err != nil {
		return err
	}
	if len(vbucketIds) <= 0 {
		// FIXME: handle this situation
		return nil
	}

	for vbid := range vbucketIds {
		f.startIndefiniteStream(uint16(vbid))
	}
	return nil
}

func (f *GocbDCPFeed) Close() error {
	f.m.Lock()
	if f.closed {
		f.m.Unlock()
		return nil
	}
	f.closed = true
	f.m.Unlock()

	log.Printf("feed_gocb_dcp: close, name: %s", f.Name())
	f.agent.Close()
	return nil
}

func (f *GocbDCPFeed) Dests() map[string]Dest {
	return f.dests
}

func (f *GocbDCPFeed) Stats(w io.Writer) error {
	// FIXME
	return nil
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) startIndefiniteStream(vbId uint16) {
	// FIXME: 0 to be replaced with lastReceivedSeqno for the vbid
	//lastReceviedSeqno := 0
	// FIXME
}

// ----------------------------------------------------------------

func (f *GocbDCPFeed) SnapshotMarker(startSeqNo, endSeqNo uint64,
	vbId uint16, snapshotType gocbcore.SnapshotState) {
	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, nil)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if f.stopAfter != nil {
			uuidSeq, exists := f.stopAfter[partition]
			if exists && endSeqNo > uuidSeq.Seq { // TODO: Check UUID.
				// Clamp the snapEnd so batches are executed.
				endSeqNo = uuidSeq.Seq
			}
		}

		return dest.SnapshotStart(partition, startSeqNo, endSeqNo)
	}, f.stats.TimerSnapshotStart)

	if err != nil {
		log.Warnf("feed_gocb_dcp: Error in accepting a DCP snapshot marker,"+
			" err: %v", err)
	}
}

func (f *GocbDCPFeed) Mutation(seqNo, revNo uint64,
	flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	key, value []byte) {
	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			err = destEx.DataUpdateEx(partition, key, seqNo, value, cas, 0, nil)
		} else {
			err = dest.DataUpdate(partition, key, seqNo, value, cas, 0, nil)
		}

		if err != nil {
			return fmt.Errorf("feed_gocb_dcp: Mutation,"+
				" name: %s, partition: %s, key: %v, seq: %d, err: %v",
				f.name, partition,
				log.Tag(log.UserData, key), seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerDataUpdate)

	if err != nil {
		log.Warnf("feed_gocb_dcp: Error in accepting a DCP mutation, err: %v",
			err)
	}

	// FIXME: Update VbucketMetaData if (seqNo == snapshotEndSeqno), so this
	// metadata can be used to re-establish a disconnected stream
}

func (f *GocbDCPFeed) Deletion(seqNo, revNo, cas uint64, datatype uint8,
	vbId uint16, key, value []byte) {
	err := Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(f.pf, f.dests, vbId, key)
		if err != nil || f.checkStopAfter(partition) {
			return err
		}

		if destEx, ok := dest.(DestEx); ok {
			err = destEx.DataDeleteEx(partition, key, seqNo, cas, 0, nil)
		} else {
			err = dest.DataDelete(partition, key, seqNo, cas, 0, nil)
		}

		if err != nil {
			return fmt.Errorf("feed_gocb_dcp: Deletion,"+
				" name: %s, partition: %s, key: %v, seq: %d, err: %v",
				f.name, partition,
				log.Tag(log.UserData, key), seqNo, err)
		}

		f.updateStopAfter(partition, seqNo)

		return nil
	}, f.stats.TimerDataDelete)

	if err != nil {
		log.Warnf("feed_gocb_dcp: Error in accepting a DCP deletion, err: %v",
			err)
	}

	// FIXME: Update VbucketMetaData if (seqNo == snapshotEndSeqno), so this
	// metadata can be used to re-establish a disconnected stream
}

func (f *GocbDCPFeed) Expiration(seqNo, revNo, cas uint64, vbId uint16,
	key []byte) {
	f.Deletion(seqNo, revNo, cas, 0, vbId, key, nil)
}

func (f *GocbDCPFeed) End(vbId uint16, err error) {
	log.Printf("feed_gocb_dcp: DCP stream terminated for vbId: %v, err: %v",
		vbId, err)
}

// ----------------------------------------------------------------

// checkStopAfter checks to see if we've already reached the
// stopAfterReached state for a partition.
func (f *GocbDCPFeed) checkStopAfter(partition string) bool {
	f.m.Lock()
	reached := f.stopAfterReached != nil && f.stopAfterReached[partition]
	f.m.Unlock()

	return reached
}

// updateStopAfter checks and maintains the stopAfterReached tracking
// maps, which are used for so-called "one-time indexing". Once we've
// reached the stopping point, we close the feed (after all partitions
// have reached their stopAfter sequence numbers).
func (f *GocbDCPFeed) updateStopAfter(partition string, seqNo uint64) {
	if f.stopAfter == nil {
		return
	}

	uuidSeq, exists := f.stopAfter[partition]
	if !exists {
		return
	}

	// TODO: check UUID matches?
	if seqNo >= uuidSeq.Seq {
		f.m.Lock()

		if f.stopAfterReached == nil {
			f.stopAfterReached = map[string]bool{}
		}
		f.stopAfterReached[partition] = true

		allDone := len(f.stopAfterReached) >= len(f.stopAfter)

		f.m.Unlock()

		if allDone {
			go f.Close()
		}
	}
}
