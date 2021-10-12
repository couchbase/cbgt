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
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
)

// ----------------------------------------------------------------

var ErrCouchbaseMismatchedBucketUUID = fmt.Errorf("mismatched-couchbase-bucket-UUID")

// Frequency of type time.Duration to check the state of the cluster
// that the couchbase.Bucket instance is a part of.
var CouchbaseNodesRecheckInterval = 5 * time.Second

// ----------------------------------------------------------------

// ParsePartitionsToVBucketIds is specific to couchbase
// data-sources/feeds, converting a set of partition strings from a
// dests map to vbucketId numbers.
func ParsePartitionsToVBucketIds(dests map[string]Dest) ([]uint16, error) {
	vbuckets := make([]uint16, 0, len(dests))
	for partition := range dests {
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

func uriAdj(s string) string {
	// Bucket DDL
	// See: https://en.wikipedia.org/wiki/Percent-encoding
	return strings.ReplaceAll(s, "%", "%25")
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

	u, err := url.Parse(uriAdj(sourceName))
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

// ----------------------------------------------------------------

// CBAuthHttpGet is a couchbase-specific http.Get(), for use in a
// cbauth'ed environment.
func CBAuthHttpGet(urlStrIn string) (resp *http.Response, err error) {
	urlStr, err := CBAuthURL(urlStrIn)
	if err != nil {
		return nil, err
	}

	return http.Get(urlStr)
}

// CBAuthHttpGetWithClient is a couchbase-specific http.Get() with *http.Client
// parameterisation, for use in a cbauth'ed environment.
func CBAuthHttpGetWithClient(urlStrIn string, client *http.Client) (resp *http.Response,
	err error) {
	urlStr, err := CBAuthURL(urlStrIn)
	if err != nil {
		return nil, err
	}

	if client != nil {
		return client.Get(urlStr)
	}

	return http.Get(urlStr)
}

// CBAuthURL rewrites a URL with credentials, for use in a cbauth'ed
// environment.
func CBAuthURL(urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}

	cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(u.Host)
	if err != nil {
		return "", err
	}

	u.User = url.UserPassword(cbUser, cbPasswd)

	return u.String(), nil
}

// ----------------------------------------------------------------

func parseParams(src string,
	req *http.Request) (string, string, string, error) {
	// Split the provided src on ";" as the user is permitted
	// to provide multiple servers(urls) concatenated with a ";".
	servers := strings.Split(src, ";")

	var u *url.URL
	var err error
	for _, server := range servers {
		u, err = url.Parse(server)
		if err == nil {
			break
		}
	}

	if err != nil {
		return "", "", "", err
	}
	v := url.URL{
		Scheme: u.Scheme,
		User:   u.User,
		Host:   u.Host,
	}
	uname, pwd, err := cbauth.ExtractCreds(req)
	if err != nil {
		return "", "", "", err
	}
	return v.String(), uname, pwd, nil
}

// ----------------------------------------------------------------

func CloseStatsClients(sourceName, sourceUUID string) {
	// Close couchbase.Bucket instances used for stats
	statsCBBktMap.closeCouchbaseBucket(sourceName, sourceUUID)

	// Release gocbcore.Agent/DCPAgent instances used for stats
	statsAgentsMap.releaseAgents(sourceName)
}
