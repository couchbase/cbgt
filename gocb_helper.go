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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocb"
	"gopkg.in/couchbase/gocbcore.v7"
)

// ----------------------------------------------------------------

type bucketInfo struct {
	// TODO: Can we do with a gocb.Agent instead?
	bkt         *gocb.Bucket
	lastChecked time.Time
}

type bucketMap struct {
	// Mutex to serialize access to entries
	m sync.Mutex
	// Map of gocb.Bucket instances by bucket <name>:<uuid>
	entries map[string]*bucketInfo
}

var bktMap *bucketMap

func init() {
	bktMap = &bucketMap{
		entries: make(map[string]*bucketInfo),
	}
}

// Fetches a gocb bucket instance with the requested uuid,
// if not found creates a new instance and stashes it in the map.
func (bm *bucketMap) fetchGoCBBucket(name, uuid, params, server string,
	options map[string]string) (*gocb.Bucket, error) {
	bm.m.Lock()
	defer bm.m.Unlock()

	key := name + ":" + uuid

	createNewInstance := false
	_, exists := bm.entries[key]
	if !exists {
		// if not found, create new bucket instance
		createNewInstance = true
	} else {
		timeSinceLastCheck := time.Since(bm.entries[key].lastChecked)
		if timeSinceLastCheck >= CouchbaseNodesRecheckInterval {
			// if time elapsed since last check is greater than the set
			// CouchbaseNodesRecheckInterval, re-check to see the state
			// of the couchbase cluster.
			// FIXME: Check if cluster configuration has changed?

		}
	}

	if createNewInstance {
		bucket, err := cbBucket(name, uuid, params, server, options)
		if err != nil {
			return nil, err
		}
		// FIXME: Check if the vbucket map is ready or not? maybe?

		bm.entries[key] = &bucketInfo{bkt: bucket, lastChecked: time.Now()}
	}

	return bm.entries[key].bkt, nil
}

// Closes and removes the gocb bucket instance with the uuid.
func (bm *bucketMap) closeGoCBBucket(name, uuid string) {
	bm.m.Lock()
	defer bm.m.Unlock()

	key := name + ":" + uuid

	bktInfo, exists := bm.entries[key]
	if exists {
		bktInfo.bkt.Close()
		delete(bm.entries, key)
	}
}

// ----------------------------------------------------------------

func cbBucket(sourceName, sourceUUID, sourceParams, serverIn string,
	options map[string]string) (*gocb.Bucket, error) {
	/*
		server, poolName, bucketName :=
			CouchbaseParseSourceName(serverIn, "default", sourceName)
	*/

	// FIXME: Implement gocbcore.Authenticator
	// FIXME: Setup connection via a gocb.Bucket or a gocbcore.Agent?

	return nil, nil
}

// ----------------------------------------------------------------

// CBPartitions parses a sourceParams for a couchbase
// data-sourcec/feed.
// FIXME: How to fetch the vbucket server map for the bucket??
func CBPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (
	partitions []string, err error) {

	return []string{}, nil
}

// ----------------------------------------------------------------

// CBPartitionSeqs returns a map keyed by partition/vbucket ID
// with values of each vbucket's UUID / high_seqno. It implements the
// FeedPartitionsFunc func signature.
func CBPartitionSeqs(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string) (
	map[string]UUIDSeq, error) {

	bucket, err := bktMap.fetchGoCBBucket(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	rv := map[string]UUIDSeq{}

	stats, err := bucket.Stats("vbucket-details")
	if err != nil {
		return nil, err
	}

	for _, nodeStats := range stats {
		if len(nodeStats) <= 0 {
			continue
		}

		for _, vbid := range vbucketIdStrings {
			stateVal, ok := nodeStats["vb_"+vbid]
			if !ok || stateVal != "active" {
				continue
			}

			uuid, ok := nodeStats["vb_"+vbid+":uuid"]
			if !ok {
				continue
			}

			seqStr, ok := nodeStats["vb_"+vbid+":high_seqno"]
			if !ok {
				continue
			}

			seq, err := strconv.ParseUint(seqStr, 10, 64)
			if err == nil {
				rv[vbid] = UUIDSeq{
					UUID: uuid,
					Seq:  seq,
				}
			}
		}
	}

	return rv, nil
}

// ----------------------------------------------------------------

// CBStats returns a map of aggregated ("aggStats") and
// per-node stats ("nodesStats"). It implements the FeedStatsFunc
// func signature.
func CBStats(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string, statsKind string) (
	map[string]interface{}, error) {

	bucket, err := bktMap.fetchGoCBBucket(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	nodesStats, err := bucket.Stats(statsKind)
	if err != nil {
		return nil, err
	}

	aggStats := map[string]int64{} // Calculate aggregates.
	for _, nodeStats := range nodesStats {
		for k, v := range nodeStats {
			iv, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				aggStats[k] += iv
			}
		}
	}

	rv := map[string]interface{}{
		"aggStats":   aggStats,
		"nodesStats": nodesStats,
	}

	if statsKind == "" {
		rv["docCount"] = aggStats["curr_items"]
	}

	return rv, nil
}

// ----------------------------------------------------------------

// CBVBucketLookUp looks up the source vBucketID for a given
// document ID and index.
func CBVBucketLookUp(docID, serverIn string,
	sourceDetails *IndexDef, req *http.Request) (string, error) {
	server, uname, pwd, err := parseParams(serverIn, req)
	if err != nil {
		return "", err
	}
	authParams := `{"authUser": "` + uname + `",` + `"authPassword":"` + pwd + `"}`
	if sourceDetails.SourceType != SOURCE_TYPE_COUCHBASE &&
		sourceDetails.SourceType != SOURCE_TYPE_DCP {
		return "", fmt.Errorf("operation not supported on " +
			sourceDetails.SourceType + " type bucket " +
			sourceDetails.SourceName)
	}
	bucket, err := cbBucket(sourceDetails.SourceName, "",
		authParams, server, nil)
	if err != nil {
		return "", err
	}
	defer bucket.Close()

	vb := bucket.IoRouter().KeyToVbucket([]byte(docID))

	return strconv.Itoa(int(vb)), nil
}

// ----------------------------------------------------------------

type Authenticator struct{}

func (a *Authenticator) Credentials(req gocbcore.AuthCredsRequest) ([]gocbcore.UserPassPair, error) {
	endpoint := req.Endpoint

	// get rid of the http:// or https:// prefix from the endpoint
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	username, password, err := cbauth.GetMemcachedServiceAuth(endpoint)
	if err != nil {
		return []gocbcore.UserPassPair{{}}, err
	}

	return []gocbcore.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}
