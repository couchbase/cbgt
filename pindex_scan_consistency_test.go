//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Helper functions
// =============================================================================

// mockFetchSeqNos returns a fetch function that returns the provided seqnos.
func mockFetchSeqNos(seqnos map[string]uint64, err error) func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error) {
	return func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error) {
		return seqnos, err
	}
}

// countingFetchSeqNos returns a fetch function that counts invocations.
func countingFetchSeqNos(seqnos map[string]uint64, delay time.Duration) (func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error), *atomic.Int32) {
	var count atomic.Int32
	return func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error) {
		count.Add(1)
		time.Sleep(delay)
		return seqnos, nil
	}, &count
}

func makeRequest(bucket, scope, collection string) *seqNosRequest {
	return &seqNosRequest{
		sourceName: bucket,
		ks:         keyspaceFor(bucket, scope, collection),
		respCh:     make(chan *seqNosResponse, 1),
	}
}

// =============================================================================
// SeqNosReader Tests
// =============================================================================

func TestSeqNosReader_SingleRequest(t *testing.T) {
	expectedSeqNos := map[string]uint64{"0": 100, "1": 200, "2": 300}
	fetchFn := mockFetchSeqNos(expectedSeqNos, nil)

	reader := newSeqNosReader(3, fetchFn, nil)
	reader.start()
	defer close(reader.stopCh)

	req := makeRequest("bucket1", "scope1", "coll1")
	reader.inbox <- req

	resp := req.response(nil)
	if resp.err != nil {
		t.Fatalf("expected no error, got: %v", resp.err)
	}
	if !reflect.DeepEqual(resp.seqNos, expectedSeqNos) {
		t.Fatalf("expected seqnos %v, got %v", expectedSeqNos, resp.seqNos)
	}
}

func TestSeqNosReader_FetchError(t *testing.T) {
	expectedErr := errors.New("fetch failed")
	fetchFn := mockFetchSeqNos(nil, expectedErr)

	reader := newSeqNosReader(3, fetchFn, nil)
	reader.start()
	defer close(reader.stopCh)

	req := makeRequest("bucket1", "scope1", "coll1")
	reader.inbox <- req

	resp := req.response(nil)
	if resp.err == nil {
		t.Fatalf("expected error, got nil")
	}
	if resp.err != expectedErr {
		t.Fatalf("expected error %v, got %v", expectedErr, resp.err)
	}
	if resp.seqNos != nil {
		t.Fatalf("expected nil seqnos, got %v", resp.seqNos)
	}
}

func TestSeqNosReader_MultipleKeyspaces(t *testing.T) {
	expectedSeqNos := map[string]uint64{"0": 100, "1": 200, "2": 300}
	fetchFn := mockFetchSeqNos(expectedSeqNos, nil)

	reader := newSeqNosReader(3, fetchFn, nil)
	reader.start()
	defer close(reader.stopCh)

	// Send requests for different keyspaces
	req1 := makeRequest("bucket1", "scope1", "coll1")
	req2 := makeRequest("bucket2", "scope1", "coll1")
	req3 := makeRequest("bucket1", "scope2", "coll2")

	reader.inbox <- req1
	reader.inbox <- req2
	reader.inbox <- req3

	// All should succeed
	for i, req := range []*seqNosRequest{req1, req2, req3} {
		resp := req.response(nil)
		if resp.err != nil {
			t.Fatalf("request %d: expected no error, got: %v", i, resp.err)
		}
		if !reflect.DeepEqual(resp.seqNos, expectedSeqNos) {
			t.Fatalf("request %d: expected seqnos %v, got %v", i, expectedSeqNos, resp.seqNos)
		}
	}
}

func TestSeqNosReader_BatchesConcurrentRequests(t *testing.T) {
	expectedSeqNos := map[string]uint64{"0": 100, "1": 200, "2": 300}
	fetchFn, fetchCount := countingFetchSeqNos(expectedSeqNos, 50*time.Millisecond)

	reader := newSeqNosReader(3, fetchFn, nil)
	reader.start()
	defer close(reader.stopCh)

	// Send multiple requests for the same keyspace concurrently
	numRequests := 10
	requests := make([]*seqNosRequest, numRequests)
	var wg sync.WaitGroup

	for i := 0; i < numRequests; i++ {
		requests[i] = makeRequest("bucket1", "scope1", "coll1")
		wg.Add(1)
		go func(req *seqNosRequest) {
			defer wg.Done()
			reader.inbox <- req
		}(requests[i])
	}

	// Wait for all requests to be sent
	wg.Wait()

	// Collect responses
	for i, req := range requests {
		resp := req.response(nil)
		if resp.err != nil {
			t.Fatalf("request %d: expected no error, got: %v", i, resp.err)
		}
		if !reflect.DeepEqual(resp.seqNos, expectedSeqNos) {
			t.Fatalf("request %d: expected seqnos %v, got %v", i, expectedSeqNos, resp.seqNos)
		}
	}

	// Fetch should have been called fewer times than numRequests due to batching
	count := fetchCount.Load()
	if count >= int32(numRequests) {
		t.Fatalf("expected fewer than %d fetch calls due to batching, got %d", numRequests, count)
	}
	t.Logf("batching efficiency: %d fetch calls for %d requests", count, numRequests)
}

func TestSeqNosReader_DifferentKeyspacesNotBatched(t *testing.T) {
	expectedSeqNos := map[string]uint64{"0": 100, "1": 200, "2": 300}
	fetchFn, fetchCount := countingFetchSeqNos(expectedSeqNos, 10*time.Millisecond)

	reader := newSeqNosReader(3, fetchFn, nil)
	reader.start()
	defer close(reader.stopCh)

	// Send requests for different keyspaces
	numRequests := 5
	requests := make([]*seqNosRequest, numRequests)

	for i := 0; i < numRequests; i++ {
		// Each request has a different collection
		requests[i] = makeRequest("bucket1", "scope1", "coll"+string(rune('0'+i)))
		reader.inbox <- requests[i]
	}

	// Collect responses
	for i, req := range requests {
		resp := req.response(nil)
		if resp.err != nil {
			t.Fatalf("request %d: expected no error, got: %v", i, resp.err)
		}
		if !reflect.DeepEqual(resp.seqNos, expectedSeqNos) {
			t.Fatalf("request %d: expected seqnos %v, got %v", i, expectedSeqNos, resp.seqNos)
		}
	}

	// Each keyspace should have its own fetch
	count := fetchCount.Load()
	if count != int32(numRequests) {
		t.Fatalf("expected %d fetch calls (one per keyspace), got %d", numRequests, count)
	}
}

func TestSeqNosReader_GlobalSeqNosWithEmptyScopeCollection(t *testing.T) {
	expectedSeqNos := map[string]uint64{"0": 100, "1": 200, "2": 300}
	fetchFn, fetchCount := countingFetchSeqNos(expectedSeqNos, 50*time.Millisecond)

	reader := newSeqNosReader(3, fetchFn, nil)
	reader.start()
	defer close(reader.stopCh)

	// Send requests with empty scope/collection (implies global seqnos) for same bucket
	numRequests := 5
	requests := make([]*seqNosRequest, numRequests)
	var wg sync.WaitGroup

	for i := 0; i < numRequests; i++ {
		// Empty scope and collection implies global seqnos - all should batch together
		requests[i] = makeRequest("bucket1", "", "")
		wg.Add(1)
		go func(req *seqNosRequest) {
			defer wg.Done()
			reader.inbox <- req
		}(requests[i])
	}

	wg.Wait()

	// Collect responses
	for i, req := range requests {
		resp := req.response(nil)
		if resp.err != nil {
			t.Fatalf("request %d: expected no error, got: %v", i, resp.err)
		}
		if !reflect.DeepEqual(resp.seqNos, expectedSeqNos) {
			t.Fatalf("request %d: expected seqnos %v, got %v", i, expectedSeqNos, resp.seqNos)
		}
	}

	// With empty scope/collection (global seqnos), all requests should batch
	count := fetchCount.Load()
	if count >= int32(numRequests) {
		t.Fatalf("expected fewer than %d fetch calls (global seqnos batching), got %d", numRequests, count)
	}
	t.Logf("global seqnos batching: %d fetch calls for %d requests", count, numRequests)
}
