package cbgt

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCreateListener_DeferBroadcast(t *testing.T) {
	// Create a BucketScopeInfoTracker with a single bucket
	tracker := &BucketScopeInfoTracker{
		server:          "testServer",
		bucketScopeInfo: make(map[string]*BucketScopeInfo),
	}
	tracker.bucketScopeInfo["testBucket"] = &BucketScopeInfo{
		refs:   1,
		stopCh: make(chan struct{}),
		cv:     sync.NewCond(&sync.Mutex{}),
	}

	// Create a mock streaming endpoint that simulates a real streaming endpoint
	mockEndpoint := func(msg string, url string, decodeAndNotifyResponse func([]byte) error, stopCh chan struct{}) error {
		// Simulate a streaming response with a few messages
		messages := []string{
			`{"name":"testBucket","uuid":"123","collectionsManifestUid":"456","numVBuckets":1024}`,
			`{"name":"testBucket","uuid":"123","collectionsManifestUid":"789","numVBuckets":1024}`,
		}

		for _, msg := range messages {
			select {
			case <-stopCh:
				return nil
			default:
				err := decodeAndNotifyResponse([]byte(msg))
				if err != nil {
					return err
				}
				// Simulate some delay between messages
				time.Sleep(50 * time.Millisecond)
			}
		}
		return nil
	}

	// Override the StreamingEndpointListener for testing
	originalListener := StreamingEndpointListener
	StreamingEndpointListener = mockEndpoint
	defer func() { StreamingEndpointListener = originalListener }()

	// Create a channel to signal when the listener is ready
	listenerReady := make(chan struct{})

	// Start the listener in a goroutine
	go func() {
		tracker.createListener("testBucket")
		close(listenerReady)
	}()

	// Create a cacheUpdateCheck function that always returns true
	cacheUpdateCheck := func(bucketScopeInfoMap map[string]*BucketScopeInfo) bool {
		_, ok := bucketScopeInfoMap["testBucket"]
		if !ok {
			return false
		}
		return true
	}

	// Wait for the listener to be ready before calling getBucketInfo
	<-listenerReady

	// Call getBucketInfo in a goroutine
	done := make(chan struct{})
	go func() {
		tracker.getBucketInfo("testBucket", cacheUpdateCheck)
		close(done)
	}()

	// Check if getBucketInfo returns (it should because of the defer broadcast)
	select {
	case <-done:
		// This is what we want - the function should return because of the defer broadcast
		t.Log("getBucketInfo returned as expected due to defer broadcast")
	case <-time.After(10 * time.Second):
		// This is not what we want - the function should not hang
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		t.Fatalf("getBucketInfo hung when it should have returned. Goroutine stack trace:\n%s", buf)
	}
}
