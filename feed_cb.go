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
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
)

func init() {
	if gomemcached.MaxBodyLen < int(3e7) { // 30,000,000.
		gomemcached.MaxBodyLen = int(3e7)
	}
}

// ParsePartitionsToVBucketIds is specific to couchbase
// data-sources/feeds, converting a set of partition strings from a
// dests map to vbucketId numbers.
func ParsePartitionsToVBucketIds(dests map[string]Dest) ([]uint16, error) {
	vbuckets := make([]uint16, 0, len(dests))
	for partition, _ := range dests {
		if partition != "" {
			vbId, err := strconv.Atoi(partition)
			if err != nil {
				return nil, fmt.Errorf("feed_cb:"+
					" could not parse partition: %s, err: %v", partition, err)
			}
			vbuckets = append(vbuckets, uint16(vbId))
		}
	}
	return vbuckets, nil
}

// VBucketIdToPartitionDest is specific to couchbase
// data-sources/feeds, choosing the right Dest based on a vbucketId.
func VBucketIdToPartitionDest(pf DestPartitionFunc,
	dests map[string]Dest, vbucketId uint16, key []byte) (
	partition string, dest Dest, err error) {
	if vbucketId < uint16(len(vbucketIdStrings)) {
		partition = vbucketIdStrings[vbucketId]
	}
	if partition == "" {
		partition = fmt.Sprintf("%d", vbucketId)
	}
	dest, err = pf(partition, key, dests)
	if err != nil {
		return "", nil, fmt.Errorf("feed_cb: VBucketIdToPartitionDest,"+
			" partition func, vbucketId: %d, err: %v", vbucketId, err)
	}
	return partition, dest, err
}

// vbucketIdStrings is a memoized array of 1024 entries for fast
// conversion of vbucketId's to partition strings via an index lookup.
var vbucketIdStrings []string

func init() {
	vbucketIdStrings = make([]string, 1024)
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
	}
}

// ----------------------------------------------------------------

// CBFeedParams are common couchbase data-source/feed specific
// connection parameters that may be part of a sourceParams JSON.
type CBFeedParams struct {
	AuthUser     string `json:"authUser"` // May be "" for no auth.
	AuthPassword string `json:"authPassword"`
}

// CouchbasePartitions parses a sourceParams for a couchbase
// data-source/feed.
func CouchbasePartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (
	partitions []string, err error) {
	bucket, err := CouchbaseBucket(sourceName, sourceUUID, sourceParams,
		serverIn, options)
	if err != nil {
		return nil, err
	}

	defer bucket.Close()

	vbm := bucket.VBServerMap()
	if vbm == nil {
		return nil, fmt.Errorf("feed_cb: CouchbasePartitions"+
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

// CouchbaseBucket is a helper function to connect to a couchbase bucket.
func CouchbaseBucket(sourceName, sourceUUID, sourceParams, serverIn string,
	options map[string]string) (*couchbase.Bucket, error) {
	server, poolName, bucketName :=
		CouchbaseParseSourceName(serverIn, "default", sourceName)

	params := CBFeedParams{}
	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), &params)
		if err != nil {
			return nil, fmt.Errorf("feed_cb: CouchbaseBucket"+
				" failed sourceParams JSON to CBFeedParams,"+
				" server: %s, poolName: %s, bucketName: %s,"+
				" sourceParams: %q, err: %v",
				server, poolName, bucketName, sourceParams, err)
		}
	}

	authType := ""
	if options != nil {
		authType = options["authType"]
	}

	if authType == "cbauth" {
		auth, err := NewCbAuthHandler(server)
		if err == nil {
			params.AuthUser, params.AuthPassword, err = auth.GetCredentials()
		}
		if err != nil {
			return nil, fmt.Errorf("feed_cb: CouchbaseBucket"+
				" could not retrieve cbauth credentials,"+
				" server: %s, poolName: %s, bucketName: %s,"+
				" sourceParams: %q, err: %v",
				server, poolName, bucketName, sourceParams, err)
		}
	}

	var err error
	var client couchbase.Client

	if params.AuthUser != "" || bucketName != "default" {
		client, err = couchbase.ConnectWithAuthCreds(server,
			params.AuthUser, params.AuthPassword)
	} else {
		client, err = couchbase.Connect(server)
	}
	if err != nil {
		return nil, fmt.Errorf("feed_cb: CouchbaseBucket"+
			" connection failed, server: %s, poolName: %s,"+
			" bucketName: %s, sourceParams: %q, err: %v,"+
			" please check that your authUser and authPassword are correct"+
			" and that your couchbase server (%q) is available",
			server, poolName, bucketName, sourceParams, err, server)
	}

	pool, err := client.GetPool(poolName)
	if err != nil {
		return nil, fmt.Errorf("feed_cb: CouchbaseBucket"+
			" failed GetPool, server: %s, poolName: %s,"+
			" bucketName: %s, sourceParams: %q, err: %v",
			server, poolName, bucketName, sourceParams, err)
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("feed_cb: CouchbaseBucket"+
			" failed GetBucket, server: %s, poolName: %s,"+
			" bucketName: %s, err: %v, please check that your"+
			" bucketName/sourceName (%q) is correct",
			server, poolName, bucketName, err, bucketName)
	}

	return bucket, nil
}

// CouchbaseParseSourceName parses a sourceName, if it's a couchbase
// REST/HTTP URL, into a server URL, poolName and bucketName.
// Otherwise, returns the serverURLDefault, poolNameDefault, and treat
// the sourceName as a bucketName.
func CouchbaseParseSourceName(
	serverURLDefault, poolNameDefault, sourceName string) (
	string, string, string) {
	if !strings.HasPrefix(sourceName, "http://") &&
		!strings.HasPrefix(sourceName, "https://") {
		return serverURLDefault, poolNameDefault, sourceName
	}

	u, err := url.Parse(sourceName)
	if err != nil {
		return serverURLDefault, poolNameDefault, sourceName
	}

	a := strings.Split(u.Path, "/")
	if len(a) != 5 ||
		a[0] != "" ||
		a[1] != "pools" ||
		a[2] == "" ||
		a[3] != "buckets" ||
		a[4] == "" {
		return serverURLDefault, poolNameDefault, sourceName
	}

	v := url.URL{
		Scheme: u.Scheme,
		User:   u.User,
		Host:   u.Host,
	}

	server := v.String()
	poolName := a[2]
	bucketName := a[4]

	return server, poolName, bucketName
}

// -------------------------------------------------

type CbAuthHandler struct {
	HostPort string
}

func (ah *CbAuthHandler) GetSaslCredentials() (string, string, error) {
	u, p, err := cbauth.GetMemcachedServiceAuth(ah.HostPort)
	if err != nil {
		return "", "", err
	}
	return u, p, nil
}

func (ah *CbAuthHandler) GetCredentials() (string, string, error) {
	u, p, err := cbauth.GetHTTPServiceAuth(ah.HostPort)
	if err != nil {
		return "", "", err
	}
	return u, p, nil
}

func NewCbAuthHandler(s string) (*CbAuthHandler, error) {
	u, err := url.Parse(s)
	if err == nil {
		return &CbAuthHandler{HostPort: u.Host}, nil
	}
	return nil, err
}

// -------------------------------------------------

// CouchbasePartitionSeqs returns a map keyed by partition/vbucket ID
// with values of each vbucket's UUID / high_seqno.  It implements the
// FeedPartitionsFunc func signature.
func CouchbasePartitionSeqs(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string) (
	map[string]UUIDSeq, error) {
	bucket, err := CouchbaseBucket(sourceName, sourceUUID, sourceParams,
		serverIn, options)
	if err != nil {
		return nil, err
	}

	rv := map[string]UUIDSeq{}

	stats := bucket.GetStats("vbucket-details")

	for _, nodeStats := range stats {
		// TODO: What if vbucket appears across multiple nodes?  Need
		// to look for the highest (or lowest?) seq number?
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

	bucket.Close()

	return rv, nil
}

// -------------------------------------------------

// CouchbaseStats returns a map of server ID -> map of stat key to map
// value, depending on the statsKind requested.a map keyed by stat
// names, depending on the statsKind.  It implements the FeedStatsFunc
// func signature.

func CouchbaseStats(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string, statsKind string) (
	map[string]interface{}, error) {
	bucket, err := CouchbaseBucket(sourceName, sourceUUID, sourceParams,
		serverIn, options)
	if err != nil {
		return nil, err
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

	bucket.Close()

	rv := map[string]interface{}{
		"aggStats":   aggStats,
		"nodesStats": nodesStats,
	}

	if statsKind == "" {
		rv["docCount"] = aggStats["curr_items"]
	}

	return rv, nil
}
