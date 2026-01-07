//  Copyright 2023-Present Couchbase, Inc.
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
	"net/http"
	"strconv"
	"strings"
	"sync"

	log "github.com/couchbase/clog"
)

var resourceNotFoundStrs = []string{
	"Requested resource not found",
	"Attempt to access non existent bucket",
}

func isResponseEquivalentToResourceNotFound(resp string) bool {
	for _, str := range resourceNotFoundStrs {
		if strings.Contains(string(resp), str) {
			return true
		}
	}

	return false
}

// This implementation of GetPoolsDefaultForBucket works with CBAUTH only;
// For all other authtypes, the application will have to override this function.
var GetPoolsDefaultForBucket = func(server, bucket string, scopes bool) ([]byte, error) {
	if len(bucket) == 0 {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket: bucket not provided")
	}

	url := server + "/pools/default/b/" + bucket
	if scopes {
		url = server + "/pools/default/buckets/" + bucket + "/scopes"
	}

	u, err := CBAuthURL(url)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}

	resp, err := HttpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, err: %v", err)
	}

	if len(respBuf) == 0 {
		return nil, fmt.Errorf("GetPoolsDefaultForBucket, empty response for url: %v", url)
	}

	return respBuf, nil
}

// -------------------------------------------------------

type BucketScopeInfo struct {
	refs   int
	stopCh chan struct{}
	cv     *sync.Cond

	ManifestUID       string
	UUID              string
	Name              string
	NumVBuckets       int
	scopeManifestInfo *manifest
}

type BucketScopeInfoTracker struct {
	server string

	m               sync.RWMutex
	bucketScopeInfo map[string]*BucketScopeInfo
}

// the StreamingEndpointListener must be set up by the importing package before
// setting up the manager instance to enable caching of bucket scope info.
var StreamingEndpointListener func(msg, url string,
	decodeAndNotifyResponse func([]byte) error, stopCh chan struct{}) error

type bucketStreamingResponse struct {
	Name        string `json:"name"`
	UUID        string `json:"uuid"`
	ManifestUID string `json:"collectionsManifestUid"`
	NumVBuckets int    `json:"numVBuckets"`
}

func initBucketScopeInfoTracker(server string) *BucketScopeInfoTracker {
	if StreamingEndpointListener != nil {
		return &BucketScopeInfoTracker{
			server:          server,
			bucketScopeInfo: make(map[string]*BucketScopeInfo),
		}
	}
	log.Warnf("bucket_scope_tracker: streamingEndpointListener hasn't been set" +
		" up, bucket scope info won't be cached")
	return nil
}

func isBucketAlive(mgr *Manager, sourceUUID, bucketName string, force bool,
	cacheUpdateCheck sourceExistsFunc) (bool, error) {
	rv := struct {
		UUID string `json:"uuid"`
	}{}
	if mgr.bucketScopeInfoTracker == nil || force {
		// fallback non-streaming way of fetching bucket/scope info
		resp, err := GetPoolsDefaultForBucket(mgr.Server(), bucketName, false)
		if err != nil {
			return false, err
		}

		err = UnmarshalJSON(resp, &rv)
		if err != nil || len(rv.UUID) == 0 {
			// in case of a marshalling error, let's quickly check if it's
			// the situation of ns_server reporting that the "Requested resource not found.";
			// if so, we can safely assume that the bucket is deleted.
			if isResponseEquivalentToResourceNotFound(string(resp)) {
				return true, nil
			}
			return false, fmt.Errorf("response: %v, err: %v", string(resp), err)
		}
	} else {
		bucketInfo := mgr.bucketScopeInfoTracker.getBucketInfo(bucketName, cacheUpdateCheck)

		if bucketInfo == nil {
			return true, nil
		}

		if len(bucketInfo.UUID) == 0 {
			return false, fmt.Errorf("encountered a marshalling error")
		}
		rv.UUID = bucketInfo.UUID
	}

	if rv.UUID != sourceUUID {
		return true, nil
	}
	return false, nil
}

func obtainBucketManifest(mgr *Manager, bucketName string, force bool,
	cacheUpdateCheck sourceExistsFunc) (*manifest, error) {
	if mgr.bucketScopeInfoTracker == nil || force {
		// fallback non-streaming way of fetching bucket/scope info
		resp, err := GetPoolsDefaultForBucket(mgr.Server(), bucketName, true)
		if err != nil {
			return nil, fmt.Errorf("manifest: error fetching manifest, err: %v", err)
		}
		var rv manifest
		if err = rv.UnmarshalJSON(resp); err != nil {
			return nil, fmt.Errorf("manifest: error parsing respBuf, resp: %s, err: %v", resp, err)
		}
		return &rv, nil
	}

	bucketInfo := mgr.bucketScopeInfoTracker.getBucketInfo(bucketName, cacheUpdateCheck)
	rv := bucketInfo.scopeManifestInfo
	return rv, nil
}

func (b *BucketScopeInfoTracker) updateManifestInfoLOCKED(bucket, ManifestUID string) error {
	respBytes, err := GetPoolsDefaultForBucket(b.server, bucket, true)
	if err != nil {
		return err
	}

	scopeManifestInfo := b.bucketScopeInfo[bucket].scopeManifestInfo
	if b.bucketScopeInfo[bucket].scopeManifestInfo == nil {
		scopeManifestInfo = &manifest{}
	}

	err = scopeManifestInfo.UnmarshalJSON(respBytes)
	if err != nil {
		return err
	}

	b.bucketScopeInfo[bucket].scopeManifestInfo = scopeManifestInfo
	return nil
}

// the conditional variable serves the purpose of notifying any listeners
// that the cache has updated by a broadcase and that they can check whether
// their condition has been met. This is used when we want to wait for a network
// call to update the cache so that we avoid stale cache reads causing index hanging
// issues.
func broadcastCacheUpdate(cacheVal *BucketScopeInfo) {
	if cacheVal != nil {
		cacheVal.cv.L.Lock()
		cacheVal.cv.Broadcast()
		cacheVal.cv.L.Unlock()
	}
}

func (b *BucketScopeInfoTracker) createListener(bucket string) {
	// create a listener for the bucket
	urlPath := b.server + "/pools/default/bs/" + bucket

	// the callback function gets the byte slice data from the streaming endpoint
	// decodes it according to a certain format and updates the cache with the
	// latest info. in case of scope info, we refresh the info by doing a network
	// call only when the manifestUID changes.
	decodeAndNotifyResponse := func(data []byte) error {
		b.m.Lock()
		var isSourceDeleted bool
		defer func() {
			bucketScopeInfo := b.bucketScopeInfo[bucket]
			if isSourceDeleted {
				// if the source is deleted, remove the cache entry
				delete(b.bucketScopeInfo, bucket)
			}
			// decodeAndNotifyResponse must always broadcast since theres a definite
			// change in the bucket scope info
			broadcastCacheUpdate(bucketScopeInfo)
			b.m.Unlock()
		}()
		if isResponseEquivalentToResourceNotFound(string(data)) {
			if !isClosed(b.bucketScopeInfo[bucket].stopCh) {
				close(b.bucketScopeInfo[bucket].stopCh)
			}
			isSourceDeleted = true
			return fmt.Errorf("%s", string(data))
		}
		var streamResp *bucketStreamingResponse
		err := UnmarshalJSON(data, &streamResp)
		if err != nil {
			// indicates that its a marshal error, however the bucket still exists
			// since at this point its definitely not a 404
			b.bucketScopeInfo[bucket].UUID = ""
			return err
		}

		b.bucketScopeInfo[bucket].UUID = streamResp.UUID
		b.bucketScopeInfo[bucket].Name = streamResp.Name
		if streamResp.ManifestUID != b.bucketScopeInfo[bucket].ManifestUID {
			err := b.updateManifestInfoLOCKED(bucket, streamResp.ManifestUID)
			if err != nil {
				return err
			}
			b.bucketScopeInfo[bucket].ManifestUID = streamResp.ManifestUID
		}
		b.bucketScopeInfo[bucket].NumVBuckets = streamResp.NumVBuckets
		return nil
	}

	b.m.RLock()
	stopCh := b.bucketScopeInfo[bucket].stopCh
	b.m.RUnlock()

	// stop channel must get incorporated with the cleanup process,
	// if no bucket is there on this node, clean it up.
	StreamingEndpointListener("bucket_scopes_pools_listener_"+bucket, urlPath,
		decodeAndNotifyResponse, stopCh)
}

// this is a callback functions that's used to check whether the feed's source
// doesn't exist anymore, and if so, we can stop waiting for an update from the
// streaming endpoint and return the cached bucket scope info (which got updated
// recently causing the func to exit)
type sourceExistsFunc func(map[string]*BucketScopeInfo) bool

func (b *BucketScopeInfoTracker) getBucketInfo(bucket string, cacheUpdateCheck sourceExistsFunc) *BucketScopeInfo {
	b.m.RLock()
	if b.bucketScopeInfo[bucket] == nil {
		b.m.RUnlock()
		return nil
	}
	cachedBucketScopeMap := b.bucketScopeInfo
	notify := cachedBucketScopeMap[bucket].cv
	b.m.RUnlock()

	if cacheUpdateCheck != nil {
		notify.L.Lock()
		for cacheUpdateCheck(cachedBucketScopeMap) {
			notify.Wait()
		}
		notify.L.Unlock()
	}

	b.m.RLock()
	rv := b.bucketScopeInfo[bucket]
	b.m.RUnlock()

	return rv
}

func untrackBucket(mgr *Manager, name string) {
	if mgr.bucketScopeInfoTracker != nil {
		mgr.bucketScopeInfoTracker.m.Lock()
		defer mgr.bucketScopeInfoTracker.m.Unlock()

		bucketInfo, ok := mgr.bucketScopeInfoTracker.bucketScopeInfo[name]
		if ok {
			bucketInfo.refs--
			if bucketInfo.refs == 0 && !isClosed(bucketInfo.stopCh) {
				// close the channel to stop the listener and broadcast
				close(bucketInfo.stopCh)
				broadcastCacheUpdate(bucketInfo)
			}
		}
	}
}

func trackBucket(mgr *Manager, name string) {
	if mgr.bucketScopeInfoTracker != nil {
		mgr.bucketScopeInfoTracker.m.Lock()
		bucketInfo, ok := mgr.bucketScopeInfoTracker.bucketScopeInfo[name]
		if ok && bucketInfo.refs > 0 {
			bucketInfo.refs++
			mgr.bucketScopeInfoTracker.m.Unlock()
			// no-op, no need to create a tracker for a bucket that already exists
			return
		}

		mgr.bucketScopeInfoTracker.bucketScopeInfo[name] = &BucketScopeInfo{
			stopCh: make(chan struct{}),
			cv:     sync.NewCond(&sync.Mutex{}),
		}
		mgr.bucketScopeInfoTracker.bucketScopeInfo[name].refs = 1
		mgr.bucketScopeInfoTracker.m.Unlock()

		// create a async routine to listen to the streaming endpoint for the bucket
		go mgr.bucketScopeInfoTracker.createListener(name)
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// -----------------------------------------------------------------------------

type manifest struct {
	uid    uint64
	scopes []manifestScope
}

func (m *manifest) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID    string          `json:"uid"`
		Scopes []manifestScope `json:"scopes"`
	}{}
	if err := UnmarshalJSON(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 64)
	if err != nil {
		return err
	}

	m.uid = decUID
	m.scopes = decData.Scopes
	return nil
}

type manifestScope struct {
	uid         uint32
	name        string
	collections []manifestCollection
}

func (m *manifestScope) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID         string               `json:"uid"`
		Name        string               `json:"name"`
		Collections []manifestCollection `json:"collections"`
	}{}
	if err := UnmarshalJSON(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 32)
	if err != nil {
		return err
	}

	m.uid = uint32(decUID)
	m.name = decData.Name
	m.collections = decData.Collections
	return nil
}

type manifestCollection struct {
	uid  uint32
	name string
}

func (m *manifestCollection) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID  string `json:"uid"`
		Name string `json:"name"`
	}{}
	if err := UnmarshalJSON(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 32)
	if err != nil {
		return err
	}

	m.uid = uint32(decUID)
	m.name = decData.Name
	return nil
}
