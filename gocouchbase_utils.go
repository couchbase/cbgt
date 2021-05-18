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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
)

const SOURCE_TYPE_COUCHBASE = "couchbase"
const SOURCE_TYPE_DCP = "dcp"

func init() {
	if gomemcached.MaxBodyLen < int(3e7) { // 30,000,000.
		gomemcached.MaxBodyLen = int(3e7)
	}
}

// ----------------------------------------------------------------

type cbBucketInfo struct {
	cbBkt       *couchbase.Bucket
	lastChecked time.Time
}

type cbBucketMap struct {
	// Mutex to serialize access to entries
	m sync.Mutex
	// Map of couchbase.Bucket instances by bucket <name>:<uuid>
	entries map[string]*cbBucketInfo
}

var statsCBBktMap *cbBucketMap

// Fetches a couchbase bucket instance with the requested uuid,
// if not found creates a new instance and stashes it in the map.
func (cb *cbBucketMap) fetchCouchbaseBucket(name, uuid, params, server string,
	options map[string]string) (*couchbase.Bucket, error) {
	cb.m.Lock()
	defer cb.m.Unlock()

	key := name + ":" + uuid

	createNewInstance := false
	_, exists := cb.entries[key]
	if !exists {
		// If not found, create new bucket instance
		createNewInstance = true
	} else {
		timeSinceLastCheck := time.Since(cb.entries[key].lastChecked)
		if timeSinceLastCheck >= CouchbaseNodesRecheckInterval {
			// If time elapsed since last check is greater than the set
			// CouchbaseNodesRecheckInterval, re-check to see the state
			// of the couchbase cluster.
			if cb.entries[key].cbBkt.NodeListChanged() {
				// If a change has been detected, reset the bucket instance.
				cb.entries[key].cbBkt.Close()
				delete(cb.entries, key)
				createNewInstance = true
			} else {
				// Update the last checked time
				cb.entries[key].lastChecked = time.Now()
			}
		}
	}

	if createNewInstance {
		bucket, err := CouchbaseBucket(name, uuid, params, server, options)
		if err != nil {
			return nil, err
		}
		vbm := bucket.VBServerMap()
		if vbm == nil || len(vbm.VBucketMap) == 0 {
			bucket.Close()

			return nil, fmt.Errorf("gocouchbase_utils: CouchbaseBucket"+
				" vbucket map not available yet,"+
				" server: %s, bucketName: %s",
				server, name)
		}

		cb.entries[key] = &cbBucketInfo{cbBkt: bucket, lastChecked: time.Now()}
	}

	return cb.entries[key].cbBkt, nil
}

// Closes and removes the couchbase bucket instance with the uuid.
func (cb *cbBucketMap) closeCouchbaseBucket(name, uuid string) {
	cb.m.Lock()
	defer cb.m.Unlock()

	key := name + ":" + uuid

	bktInfo, exists := cb.entries[key]
	if exists {
		bktInfo.cbBkt.Close()
		delete(cb.entries, key)
	}
}

func init() {
	// Initialize statsCBBktMap
	statsCBBktMap = &cbBucketMap{
		entries: make(map[string]*cbBucketInfo),
	}
}

// ----------------------------------------------------------------

// CouchbaseBucket is a helper function to connect to a couchbase bucket.
func CouchbaseBucket(sourceName, sourceUUID, sourceParams, serverIn string,
	options map[string]string) (*couchbase.Bucket, error) {
	server, poolName, bucketName :=
		CouchbaseParseSourceName(serverIn, "default", sourceName)

	auth, err := cbAuth(sourceName, sourceParams, options)
	if err != nil {
		return nil, fmt.Errorf("gocouchbase_utils: CouchbaseBucket, cbAuth,"+
			" bucketName: %s, err: %v", bucketName, err)
	}

	// If sourceName were a couchbase REST/HTTP URL, a single server URL
	// is what is built and returned from CouchbaseParseSourceName, in
	// which case the svrs array below will contain just the one server.
	// If the sourceName weren't a URL, the serverIn passed into the
	// API is just returned as is. Note that as the user is permitted
	// to add multiple servers concatenated - delimited by the ';', the
	// following Split operation is necessary to break meaningful URLs
	// apart.
	//
	// Following this, we iterate over all the meaningful URLs and
	// attempt connection with the couchbase cluster until a successful
	// connection is made.
	svrs := strings.Split(server, ";")
	var client couchbase.Client

	for _, svr := range svrs {
		client, err = couchbase.ConnectWithAuth(svr, auth)
		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("gocouchbase_utils: CouchbaseBucket"+
			" connection failed, server: %s, poolName: %s,"+
			" bucketName: %s, sourceParams: %q, err: %v,"+
			" please check that your authUser and authPassword are correct"+
			" and that your couchbase cluster (%q) is available",
			server, poolName, bucketName, sourceParams, err, server)
	}

	pool, err := client.GetPool(poolName)
	if err != nil {
		return nil, fmt.Errorf("gocouchbase_utils: CouchbaseBucket"+
			" failed GetPool, server: %s, poolName: %s,"+
			" bucketName: %s, sourceParams: %q, err: %v",
			server, poolName, bucketName, sourceParams, err)
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, err
	}

	if sourceUUID != "" && sourceUUID != bucket.UUID {
		bucket.Close()

		return nil, ErrCouchbaseMismatchedBucketUUID
	}

	return bucket, nil
}

// ----------------------------------------------------------------

// CouchbasePartitions parses a sourceParams for a couchbase
// data-source/feed.
func CouchbasePartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (
	partitions []string, err error) {
	bucket, err := statsCBBktMap.fetchCouchbaseBucket(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	// Validate bucketTypesAllowed here.
	bucketTypesAllowed := "membase"
	if options["bucketTypesAllowed"] != "" {
		bucketTypesAllowed = options["bucketTypesAllowed"]
	}
	if !strings.Contains(bucketTypesAllowed, bucket.Type) {
		return nil, fmt.Errorf("bucketTypesAllowed: '%v', but request for '%v'",
			bucketTypesAllowed, bucket.Type)
	}

	vbm := bucket.VBServerMap()
	if vbm == nil || len(vbm.VBucketMap) == 0 {
		return nil, fmt.Errorf("gocouchbase_utils: CouchbasePartitions"+
			" no VBServerMap, server: %s, sourceName: %s, err: %v",
			serverIn, sourceName, err)
	}

	// NOTE: We assume that vbucket numbers are continuous
	// integers starting from 0.
	numVBuckets := len(vbm.VBucketMap)
	rv := make([]string, numVBuckets)
	for i := 0; i < numVBuckets; i++ {
		rv[i] = strconv.Itoa(i)
	}
	return rv, nil
}

// ----------------------------------------------------------------

// CouchbasePartitionSeqs returns a map keyed by partition/vbucket ID
// with values of each vbucket's UUID / high_seqno. It implements the
// FeedPartitionsFunc func signature.
func CouchbasePartitionSeqs(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string) (
	map[string]UUIDSeq, error) {
	bucket, err := statsCBBktMap.fetchCouchbaseBucket(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	rv := map[string]UUIDSeq{}

	stats := bucket.GatherStats("vbucket")
	collectionsStats := bucket.GatherStats("collections-details")

	for node, gatheredStats := range stats {
		if gatheredStats.Err != nil {
			return nil, gatheredStats.Err
		}

		nodeStats := gatheredStats.Stats
		if len(nodeStats) <= 0 {
			continue
		}

		for _, vbid := range vbucketIdStrings {
			stateVal, ok := nodeStats["vb_"+vbid]
			if !ok || stateVal != "active" {
				continue
			}

			// obtains seq no. for _default collection of _default scope only
			seqStr, ok := collectionsStats[node].Stats["vb_"+vbid+":0x0:high_seqno"]
			if !ok {
				continue
			}

			seq, err := strconv.ParseUint(seqStr, 10, 64)
			if err == nil {
				rv[vbid+":_default:_default"] = UUIDSeq{
					UUID: "0",
					Seq:  seq,
				}
			}
		}
	}

	return rv, nil
}

// ----------------------------------------------------------------

// CouchbaseStats returns a map of aggregated ("aggStats") and
// per-node stats ("nodesStats"). It implements the FeedStatsFunc
// func signature.
func CouchbaseStats(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string, statsKind string) (
	map[string]interface{}, error) {
	bucket, err := statsCBBktMap.fetchCouchbaseBucket(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	if statsKind == "collections" {
		// collections not supported via go-couchbase
		statsKind = ""
	}

	nodesStats := bucket.GetStats(statsKind)

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

// CouchbaseSourceVBucketLookUp looks up the source vBucketID for a given
// document ID and index.
func CouchbaseSourceVBucketLookUp(docID, serverIn string,
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
	bucket, err := CouchbaseBucket(sourceDetails.SourceName, "",
		authParams, server, nil)
	if err != nil {
		return "", err
	}
	defer bucket.Close()
	vbm := bucket.VBServerMap()
	if vbm == nil || len(vbm.VBucketMap) == 0 {
		return "", fmt.Errorf("gocouchbase_utils: CouchbaseSourceVBucketLookUp"+
			" no VBServerMap, server: %s, sourceName: %s, err: %v",
			server, sourceDetails.SourceName, err)
	}
	return strconv.Itoa(int(bucket.VBHash(docID))), nil
}

// ----------------------------------------------------------------

// CouchbaseSourceUUIDLookUp fetches the sourceUUID for the provided sourceName.
func CouchbaseSourceUUIDLookUp(sourceName, sourceParams, serverIn string,
	options map[string]string) (string, error) {
	bucket, err := CouchbaseBucket(sourceName, "", sourceParams, serverIn, options)
	if err != nil {
		return "", fmt.Errorf("gocouchbase_utils: CouchbaseSourceUUIDLookUp,"+
			" bucketName: %v, err: %v", sourceName, err)
	}

	uuid := bucket.GetUUID()
	bucket.Close()

	return uuid, nil
}

// ----------------------------------------------------------------

// CBAuthParams are common couchbase data-source/feed specific
// connection parameters that may be part of a sourceParams JSON.
type CBAuthParams struct {
	AuthUser     string `json:"authUser"` // May be "" for no auth.
	AuthPassword string `json:"authPassword"`

	AuthSaslUser     string `json:"authSaslUser"` // May be "" for no auth.
	AuthSaslPassword string `json:"authSaslPassword"`
}

func (d *CBAuthParams) GetCredentials() (string, string, string) {
	// TODO: bucketName not necessarily userName.
	return d.AuthUser, d.AuthPassword, d.AuthUser
}

// ----------------------------------------------------------------

// CBAuthParamsSasl implements the cbdatasource.ServerCredProvider
// interface.
type CBAuthParamsSasl struct {
	CBAuthParams
}

func (d *CBAuthParamsSasl) GetSaslCredentials() (string, string) {
	return d.AuthSaslUser, d.AuthSaslPassword
}

func cbAuth(sourceName, sourceParams string, options map[string]string) (
	auth couchbase.AuthHandler, err error) {
	params := &CBAuthParams{}

	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), params)
		if err != nil {
			return nil, fmt.Errorf("gocouchbase_utils: cbAuth" +
				" failed to parse sourceParams JSON to CBAuthParams")
		}
	}

	auth = params

	if params.AuthSaslUser != "" {
		auth = &CBAuthParamsSasl{*params}
	}

	authType := ""
	if options != nil {
		authType = options["authType"]
	}

	if authType == "cbauth" {
		auth = cbauth.NewAuthHandler(nil).ForBucket(sourceName)
	}

	return auth, nil
}
