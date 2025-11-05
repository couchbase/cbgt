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
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore/v10"
)

// -----------------------------------------------------------------------------
//
// SCAN PLUS
//
// =============================================================================
// Sequence Number Reader - Batched fetching of vBucket sequence numbers
// =============================================================================
//
// Architecture Overview:
//
//   GetSeqNos (public API)
//        |
//        v
//   seqNosReader (single run() goroutine, routes requests to workers)
//        +---> worker 0: workerQueue -> process() ---> cbFetchSeqNos
//        +---> worker 1: workerQueue -> process() ---> cbFetchSeqNos
//		  ...
//        +---> worker N: workerQueue -> process() ---> cbFetchSeqNos
//
// Each worker has two goroutines: workerQueue.run() owns the FIFO queue and
// coalesces concurrent requests for the same keyspace; process() pops batches
// and makes KV calls. seqNosReader tracks per-key pending counts to release
// worker assignments once all outstanding requests for a key are answered.
//
// =============================================================================

// -----------------------------------------------------------------------------
// Constants and Configuration
// -----------------------------------------------------------------------------

const (
	scanPlusReqChanSize                  = 20000
	defaultScanPlusFetchBucketWideSeqNos = false
	defaultScanPlusNumRetries            = 5
	defaultScanPlusNumWorkers            = 10

	// Retry backoff parameters for GetSeqNos (in milliseconds).
	scanPlusRetryBaseDelayMS = 200.0   // 200ms
	scanPlusRetryMaxDelayMS  = 10000.0 // 10s
)

// scanPlusFetchBucketWideSeqNos controls whether to fetch bucket-wide sequence
// numbers (spanning all collections) instead of per-collection sequence numbers.
// When true, scope/collection are ignored.
//
// When to set to true: if there are a large number of collections indexed,
// this reduces the need to fetch and merge seqnos at a collection level.
//
// When to set to false: for fine-grained, collection-specific seqnos. Example:
// if one collection is mostly static and another is continuously ingesting,
// this avoids waiting on the hotter collection when targeting the static one.
var scanPlusFetchBucketWideSeqNos atomic.Bool

// scanPlusNumRetries is the number of times to retry the seqnos request to KV
var scanPlusNumRetries atomic.Int32

// scanPlusReader is the singleton seqNosReader instance.
var scanPlusReader *seqNosReader

type ScanPlusReaderOptions struct {
	numWorkers  *int
	errCallback func()
	doneCh      chan struct{}
}

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

func init() {
	scanPlusFetchBucketWideSeqNos.Store(defaultScanPlusFetchBucketWideSeqNos)
	scanPlusNumRetries.Store(defaultScanPlusNumRetries)
	scanPlusReader = newSeqNosReader(defaultScanPlusNumWorkers, cbFetchSeqNos, func() {})
	scanPlusReader.start()
}

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// RefreshScanPlusOptions updates the scanPlusFetchBucketWideSeqNos flag and worker count
// based on the provided options map. Only updates values that are explicitly
// set in the options map.
func RefreshScanPlusOptions(options map[string]string, errCallback func()) error {
	bucketLevel, nw, nr, err := parseScanPlusOptions(options)
	if err != nil {
		return err
	}

	// Refresh reader options only if explicitly set.
	if nw != nil || errCallback != nil {
		// block until reader applies these options
		doneCh := make(chan struct{})
		scanPlusReader.refreshCh <- ScanPlusReaderOptions{
			numWorkers:  nw,
			errCallback: errCallback,
			doneCh:      doneCh,
		}
		<-doneCh
	}

	if bucketLevel != nil {
		scanPlusFetchBucketWideSeqNos.Store(*bucketLevel)
	}
	if nr != nil {
		scanPlusNumRetries.Store(int32(*nr))
	}

	return nil
}

// GetSeqNos retrieves the high sequence numbers for all vBuckets.
// If scanPlusFetchBucketWideSeqNos is true, fetches bucket-wide seqnos (ignoring scope/collection).
// Otherwise, fetches per-collection seqnos and returns the element-wise maximum.
func GetSeqNos(sourceName, sourceUUID, sourceParams, serverIn string,
	scopeName string, collectionNames []string,
	cancelCh <-chan bool) (map[string]uint64, error) {

	retries := int(scanPlusNumRetries.Load())

	var requests []*seqNosRequest
	if scanPlusFetchBucketWideSeqNos.Load() {
		requests = []*seqNosRequest{
			newSeqNosRequest(sourceName, sourceUUID, sourceParams, serverIn, "", ""),
		}
	} else {
		requests = make([]*seqNosRequest, 0, len(collectionNames))
		for _, coll := range collectionNames {
			requests = append(requests,
				newSeqNosRequest(sourceName, sourceUUID, sourceParams, serverIn, scopeName, coll))
		}
	}

	for _, req := range requests {
		scanPlusReader.inbox <- req
	}

	results := make([]*seqNosResponse, len(requests))
	var wg sync.WaitGroup

	for i, req := range requests {
		wg.Add(1)
		go func(i int, req *seqNosRequest) {
			defer wg.Done()
			resp := req.response(cancelCh)
			if resp.err != nil {
				for attempt := range retries {
					time.Sleep(retryBackoff(attempt))
					req = newSeqNosRequest(req.sourceName, req.sourceUUID, req.sourceParams,
						req.serverIn, req.ks.scope, req.ks.collection)
					scanPlusReader.inbox <- req
					resp = req.response(cancelCh)
					if resp.err == nil {
						break
					}
				}
			}
			results[i] = resp
		}(i, req)
	}

	wg.Wait()

	var maxSeqNos map[string]uint64
	for i, res := range results {
		if res.err != nil {
			return nil, fmt.Errorf("pindex_scan_consistency: GetSeqNos, err: %v", res.err)
		}
		if i == 0 {
			maxSeqNos = make(map[string]uint64, len(res.seqNos))
			for vbID, seqNo := range res.seqNos {
				maxSeqNos[vbID] = seqNo
			}
		} else {
			for vbID, seqNo := range res.seqNos {
				if seqNo > maxSeqNos[vbID] {
					maxSeqNos[vbID] = seqNo
				}
			}
		}
	}

	return maxSeqNos, nil
}

// --- Utilities ---

// retryBackoff returns the sleep duration for a given retry attempt using
// exponential backoff (base * 2^attempt, capped at max) plus a random jitter
// of up to the same magnitude.
func retryBackoff(attempt int) time.Duration {
	backoffMS := math.Min(scanPlusRetryBaseDelayMS*math.Pow(2, float64(attempt)), scanPlusRetryMaxDelayMS)
	jitterMS := rand.Float64() * backoffMS
	return time.Duration((backoffMS + jitterMS) * float64(time.Millisecond))
}

// parseScanPlusOptions parses the scan plus options from the options map.
// Returns pointers for each option, where nil means the option was not set.
// This allows callers to distinguish between "not set" and "zero value".
func parseScanPlusOptions(options map[string]string) (
	bucketLevelSeqNos *bool, numworkers *int, numretries *int, err error,
) {
	if bucketLevelSeqNosStr := options["scanPlusFetchBucketWideSeqNos"]; bucketLevelSeqNosStr != "" {
		var bucketLevel bool
		bucketLevel, err = strconv.ParseBool(bucketLevelSeqNosStr)
		if err != nil {
			return
		}
		bucketLevelSeqNos = &bucketLevel
		log.Printf("pindex_scan_consistency: options refreshed: BucketLevelSeqNos: %v", bucketLevel)
	}

	if numWorkersStr := options["scanPlusNumWorkers"]; numWorkersStr != "" {
		var nw int
		nw, err = strconv.Atoi(numWorkersStr)
		if err != nil {
			return
		}
		if nw < 1 {
			err = fmt.Errorf("invalid number of workers: %d", nw)
			return
		}
		numworkers = &nw
		log.Printf("pindex_scan_consistency: options refreshed: NumWorkers: %v", nw)
	}

	if numRetriesStr := options["scanPlusNumRetries"]; numRetriesStr != "" {
		var nr int
		nr, err = strconv.Atoi(numRetriesStr)
		if err != nil {
			return
		}
		if nr < 1 {
			err = fmt.Errorf("invalid number of retries: %d", nr)
			return
		}
		numretries = &nr
		log.Printf("pindex_scan_consistency: options refreshed: NumRetries: %v", nr)
	}

	return
}

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

// seqNosRequest represents a request to fetch sequence numbers for a keyspace.
type seqNosRequest struct {
	sourceName   string
	sourceUUID   string
	sourceParams string
	serverIn     string
	ks           keyspace

	respCh chan *seqNosResponse
}

func newSeqNosRequest(sourceName, sourceUUID, sourceParams, serverIn,
	scopeName, collectionName string) *seqNosRequest {
	return &seqNosRequest{
		sourceName:   sourceName,
		sourceUUID:   sourceUUID,
		sourceParams: sourceParams,
		serverIn:     serverIn,
		ks:           keyspaceFor(sourceName, scopeName, collectionName),
		respCh:       make(chan *seqNosResponse, 1),
	}
}

// response blocks until the response is available and returns it directly.
// If cancelCh is non-nil and receives a value, the call returns immediately
// with a cancellation error.
func (r *seqNosRequest) response(cancelCh <-chan bool) *seqNosResponse {
	select {
	case <-cancelCh:
		return &seqNosResponse{err: fmt.Errorf("pindex_scan_consistency: GetSeqNos cancelled")}
	case resp := <-r.respCh:
		return resp
	}
}

// seqNosResponse contains the result of a sequence number fetch.
type seqNosResponse struct {
	seqNos map[string]uint64
	err    error
}

// keyspace identifies a unique keyspace (bucket/scope/collection).
type keyspace struct {
	bucket     string
	scope      string
	collection string
}

func keyspaceFor(bucket, scope, collection string) keyspace {
	return keyspace{bucket: bucket, scope: scope, collection: collection}
}

// workerEvent is sent from a worker to the reader after each batch completes.
type workerEvent struct {
	workerID  int
	key       keyspace
	batchSize int
	err       error
}

// keyInfo tracks the worker assignment and outstanding request count for a keyspace.
type keyInfo struct {
	workerID     int
	pendingCount int
}

// -----------------------------------------------------------------------------
// Worker Queue
// -----------------------------------------------------------------------------

// workerQueue is a FIFO queue that coalesces concurrent requests for the same
// keyspace into a single batch. Its run() goroutine is the sole owner of all
// queue state. push() and pop() are the only external entry points.
type workerQueue struct {
	reqCh  chan *seqNosRequest
	popCh  chan struct{}
	respCh chan []*seqNosRequest // fixed response channel; single consumer only
	doneCh chan struct{}

	queue    []keyspace
	queueMap map[keyspace][]*seqNosRequest
}

func newWorkerQueue(doneCh chan struct{}) *workerQueue {
	return &workerQueue{
		reqCh:    make(chan *seqNosRequest, scanPlusReqChanSize),
		popCh:    make(chan struct{}, 1),
		respCh:   make(chan []*seqNosRequest, 1),
		doneCh:   doneCh,
		queueMap: make(map[keyspace][]*seqNosRequest),
	}
}

func (wq *workerQueue) push(req *seqNosRequest) {
	wq.reqCh <- req
}

// pop signals readiness and returns the fixed response channel on which the
// next batch will be delivered.
// NOTE: single consumer ONLY. Concurrent calls to pop() can cause corruptions
func (wq *workerQueue) pop() chan []*seqNosRequest {
	wq.popCh <- struct{}{}
	return wq.respCh
}

// run is the queue goroutine. It coalesces requests by keyspace and serves
// them in FIFO order. If a pop arrives when the queue is empty, it is held
// until the next push.
func (wq *workerQueue) run() {
	var waiting bool // true when process() is blocked waiting for the next batch
	for {
		select {
		case req := <-wq.reqCh:
			wq.enqueue(req)
			if waiting {
				wq.respCh <- wq.popFront()
				waiting = false
			}

		case <-wq.popCh:
			if len(wq.queue) > 0 {
				wq.respCh <- wq.popFront()
			} else {
				waiting = true
			}

		case <-wq.doneCh:
			wq.drain()
			return
		}
	}
}

func (wq *workerQueue) enqueue(req *seqNosRequest) {
	key := req.ks
	if _, ok := wq.queueMap[key]; ok {
		wq.queueMap[key] = append(wq.queueMap[key], req)
	} else {
		wq.queue = append(wq.queue, key)
		wq.queueMap[key] = []*seqNosRequest{req}
	}
}

func (wq *workerQueue) popFront() []*seqNosRequest {
	key := wq.queue[0]
	wq.queue = wq.queue[1:]
	reqs := wq.queueMap[key]
	delete(wq.queueMap, key)
	return reqs
}

func (wq *workerQueue) drain() {
	// in case respCh sends at the same time as doneCh in worker.process()
	// and doneCh is selected this drains out that hanging request
	select {
	case reqs := <-wq.respCh:
		for _, req := range reqs {
			req.respCh <- &seqNosResponse{err: errors.New("seqnosreader closed")}
		}
	default:
	}

	for key, reqs := range wq.queueMap {
		for _, req := range reqs {
			req.respCh <- &seqNosResponse{err: errors.New("seqnosreader closed")}
		}
		delete(wq.queueMap, key)
	}
	wq.queue = wq.queue[:0]
	for {
		select {
		case req := <-wq.reqCh:
			req.respCh <- &seqNosResponse{err: errors.New("seqnosreader closed")}
		default:
			return
		}
	}
}

// -----------------------------------------------------------------------------
// Worker
// -----------------------------------------------------------------------------

// worker fetches sequence numbers for requests routed to it by seqNosReader.
// Two goroutines run per worker:
//   - workerQueue.run(): owns all queue/coalescing state; no locking needed.
//   - process():         pops batches from the queue and makes KV fetch calls.
type worker struct {
	id          int
	readerCh    chan *workerEvent
	doneCh      chan struct{}
	keyCount    int // keyspaces assigned to this worker; managed by seqNosReader
	fetchSeqNos func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error)
	queue       *workerQueue
}

func newWorker(id int, readerCh chan *workerEvent,
	fetchSeqNos func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error),
) *worker {
	doneCh := make(chan struct{})
	return &worker{
		id:          id,
		readerCh:    readerCh,
		doneCh:      doneCh,
		fetchSeqNos: fetchSeqNos,
		queue:       newWorkerQueue(doneCh),
	}
}

// process is the KV-fetch goroutine. It requests the next batch from the
// queue, performs one KV fetch, fans out the response, and reports back to
// the reader. Shutdown is checked between every KV fetch.
func (w *worker) process() {
	for {
		respCh := w.queue.pop()
		var reqs []*seqNosRequest
		select {
		case reqs = <-respCh:
		case <-w.doneCh:
			return
		}

		result, err := w.fetchSeqNos(reqs[0], w.doneCh)
		for _, req := range reqs {
			req.respCh <- &seqNosResponse{seqNos: result, err: err}
		}

		select {
		case w.readerCh <- &workerEvent{
			workerID:  w.id,
			key:       reqs[0].ks,
			batchSize: len(reqs),
			err:       err,
		}:
		case <-w.doneCh:
			return
		}
	}
}

// -----------------------------------------------------------------------------
// SeqNosReader
// -----------------------------------------------------------------------------

// seqNosReader routes requests to workers and tracks per-keyspace assignments.
// All state is owned by the single run() goroutine — no locking needed.
type seqNosReader struct {
	// Channels
	inbox        chan *seqNosRequest
	workerEvents chan *workerEvent
	refreshCh    chan ScanPlusReaderOptions
	stopCh       chan struct{}

	// keyMap tracks the worker assignment and pending count for each active keyspace.
	keyMap     map[keyspace]*keyInfo
	numWorkers int
	workers    []*worker
	wg         sync.WaitGroup

	// Callback for fetching sequence numbers (injected for testability)
	fetchSeqNos func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error)

	// Callback on worker error (used for stats collection)
	errCallback func()
}

func newSeqNosReader(numWorkers int, fetchSeqNos func(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error), errCallback func()) *seqNosReader {
	return &seqNosReader{
		inbox:        make(chan *seqNosRequest, scanPlusReqChanSize),
		workerEvents: make(chan *workerEvent, scanPlusReqChanSize),
		refreshCh:    make(chan ScanPlusReaderOptions), // blocking by design
		stopCh:       make(chan struct{}),
		keyMap:       make(map[keyspace]*keyInfo),
		numWorkers:   numWorkers,
		fetchSeqNos:  fetchSeqNos,
		errCallback:  errCallback,
	}
}

func (s *seqNosReader) start() {
	s.startWorkers(s.numWorkers)
	go s.run()
}

func (s *seqNosReader) startWorkers(n int) {
	for i := range n {
		w := newWorker(i, s.workerEvents, s.fetchSeqNos)
		s.wg.Add(2)
		go func() {
			w.queue.run()
			s.wg.Done()
		}()
		go func() {
			w.process()
			s.wg.Done()
		}()
		s.workers = append(s.workers, w)
	}
}

// run is the single event-loop goroutine for seqNosReader.
// All state mutations (workerMap, pendingCount, keyCount) happen here,
// so no mutex is required.
func (s *seqNosReader) run() {
	for {
		select {
		case req := <-s.inbox:
			s.handleRequest(req)

		case ev := <-s.workerEvents:
			s.handleWorkerEvent(ev)

		case options := <-s.refreshCh:
			s.handleWorkerRefresh(options)

		case <-s.stopCh:
			s.shutdown()
			return
		}
	}
}

// --- Event Handlers ---

// handleRequest routes an incoming request directly to its assigned worker,
// or picks a new worker for previously unseen keyspaces.
func (s *seqNosReader) handleRequest(req *seqNosRequest) {
	if ki, ok := s.keyMap[req.ks]; ok {
		ki.pendingCount++
		s.workers[ki.workerID].queue.push(req)
		return
	}

	w := s.pickWorker()
	s.keyMap[req.ks] = &keyInfo{workerID: w.id, pendingCount: 1}
	w.keyCount++
	w.queue.push(req)
}

// handleWorkerEvent decrements the pending count for the completed batch.
// When a key's count reaches zero the worker assignment is released.
func (s *seqNosReader) handleWorkerEvent(ev *workerEvent) {
	if ev.err != nil && s.errCallback != nil {
		s.errCallback()
	}

	ki := s.keyMap[ev.key]
	ki.pendingCount -= ev.batchSize
	if ki.pendingCount == 0 {
		delete(s.keyMap, ev.key)
		s.workers[ev.workerID].keyCount--
	}
}

// handleWorkerRefresh applies runtime reader option updates.
// Updates number of workers or error callback or both.
// If num workers updated: drain all channels and caches, stop and restart
// the workers.
// If errCallback updated, just update the callback function in place
func (s *seqNosReader) handleWorkerRefresh(options ScanPlusReaderOptions) {
	if options.doneCh != nil {
		defer close(options.doneCh)
	}

	refreshWorkers := options.numWorkers != nil && *options.numWorkers != len(s.workers)
	updateErrCallback := options.errCallback != nil

	if !refreshWorkers && !updateErrCallback {
		return
	}

	if updateErrCallback {
		s.errCallback = options.errCallback
	}

	if !refreshWorkers {
		return
	}

	s.stopWorkers()
	s.drainWorkerEvents()
	s.drainInbox("seqnosreader refreshing")
	clear(s.keyMap)

	s.numWorkers = *options.numWorkers
	s.workers = s.workers[:0]
	s.startWorkers(*options.numWorkers)
}

// --- Shutdown ---

func (s *seqNosReader) shutdown() {
	s.stopWorkers()
	s.drainWorkerEvents()
	s.drainInbox("seqnosreader closed")
	clear(s.keyMap)
}

// stopWorkers signals all workers to stop, drains pending events,
// and waits for all workers to exit.
func (s *seqNosReader) stopWorkers() {
	// Signal all workers to stop
	for _, w := range s.workers {
		close(w.doneCh)
	}
	// Drain workerEvents to unblock any worker blocked on sending.
	for {
		select {
		case <-s.workerEvents:
		default:
			s.wg.Wait()
			return
		}
	}
}

func (s *seqNosReader) drainWorkerEvents() {
	for {
		select {
		case <-s.workerEvents:
		default:
			return
		}
	}
}

func (s *seqNosReader) drainInbox(errMsg string) {
	for {
		select {
		case req := <-s.inbox:
			req.respCh <- &seqNosResponse{err: errors.New(errMsg)}
		default:
			return
		}
	}
}

// --- Utilities ---

func (s *seqNosReader) pickWorker() *worker {
	best := s.workers[0]
	for _, w := range s.workers[1:] {
		if w.keyCount < best.keyCount {
			best = w
		}
	}
	return best
}

// -----------------------------------------------------------------------------
// Fetch Implementation
// -----------------------------------------------------------------------------

// cbFetchSeqNos fetches high sequence numbers from Couchbase.
// If scope/collection are empty, fetches bucket-level (global) seqnos.
// Otherwise, fetches collection-specific seqnos.
// doneCh may be closed to interrupt any blocking KV call and return immediately.
func cbFetchSeqNos(req *seqNosRequest, doneCh <-chan struct{}) (map[string]uint64, error) {
	info, err := cbGetClusterInfo(req.sourceName, req.sourceUUID,
		req.sourceParams, req.serverIn, nil, doneCh)
	if err != nil {
		if errors.Is(err, gocbcore.ErrDCPStreamDisconnected) {
			// this error comes up if doneCh is closed
			return nil, fmt.Errorf("pindex_scan_consistency: cbFetchSeqNos, seqnosreader closed")
		}
		return nil, fmt.Errorf("pindex_scan_consistency: cbFetchSeqNos, %w", err)
	}

	// Build filter options list based on scope/collection
	var filterOptionsList []*gocbcore.GetVbucketSeqnoFilterOptions
	if req.ks.scope == "" || req.ks.collection == "" {
		// Global seqnos - no filter
		filterOptionsList = []*gocbcore.GetVbucketSeqnoFilterOptions{nil}
	} else {
		// Collection-specific seqnos
		collID, err := findCollectionID(info.manifest, req.ks.scope, req.ks.collection)
		if err != nil {
			return nil, fmt.Errorf("pindex_scan_consistency: cbFetchSeqNos, %w", err)
		}
		filterOptionsList = []*gocbcore.GetVbucketSeqnoFilterOptions{
			{CollectionID: collID},
		}
	}

	seqNos := make(map[string]uint64, info.numVBuckets)
	vbCount := 0

	err = cbFetchVBucketSeqNos(
		info,
		filterOptionsList,
		doneCh,
		func(entries []gocbcore.VbSeqNoEntry, filterOpts *gocbcore.GetVbucketSeqnoFilterOptions) {
			for _, entry := range entries {
				seqNos[strconv.Itoa(int(entry.VbID))] = uint64(entry.SeqNo)
				vbCount++
			}
		},
	)

	if err != nil {
		if errors.Is(err, gocbcore.ErrDCPStreamDisconnected) {
			// this error comes up if doneCh is closed
			return nil, fmt.Errorf("pindex_scan_consistency: cbFetchSeqNos, seqnosreader closed")
		}
		return nil, fmt.Errorf("pindex_scan_consistency: cbFetchSeqNos, %w", err)
	}

	if vbCount != info.numVBuckets {
		return nil, fmt.Errorf("pindex_scan_consistency: cbFetchSeqNos,"+
			" incomplete seqnos: got %d of %d vbuckets", vbCount, info.numVBuckets)
	}

	return seqNos, nil
}

func findCollectionID(manifest *gocbcore.Manifest, scopeName,
	collName string) (uint32, error) {
	for _, scope := range manifest.Scopes {
		if scope.Name != scopeName {
			continue
		}
		for _, coll := range scope.Collections {
			if coll.Name == collName {
				return coll.UID, nil
			}
		}
	}
	return 0, fmt.Errorf("collection %s.%s not found", scopeName, collName)
}
