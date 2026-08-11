//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//  Close-lifecycle tests for GocbcoreDCPFeed (MB-70770), covering the two
//  cbgt layers -- the observer fence (closed feeds drop data events) and
//  the local stream-routing teardown (retained OpenStream PendingOps
//  canceled at close) -- plus the fence's in-flight-callback limitation.

package cbgt

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/couchbase/gocbcore/v10"
)

// testRecordingDest is a minimal cbgt.Dest + cbgt.DestEx recording which
// data calls reached it. With enteredUpdate/updateGate set, DataUpdate
// signals entry then parks until the gate closes -- modeling a callback
// blocked INSIDE the Dest call.
type testRecordingDest struct {
	dataUpdates    []uint64 // seqs of DataUpdate/DataUpdateEx received
	dataDeletes    []uint64 // seqs of DataDelete/DataDeleteEx received
	snapshotStarts int
	opaqueSets     int

	enteredUpdate chan struct{} // closed on entering DataUpdate(Ex)
	updateGate    chan struct{} // DataUpdate(Ex) parks on this if non-nil
}

func (d *testRecordingDest) parkOnUpdateGate() {
	if d.enteredUpdate != nil {
		close(d.enteredUpdate)
		d.enteredUpdate = nil
	}
	if d.updateGate != nil {
		<-d.updateGate
	}
}

func (d *testRecordingDest) Close(remove bool) error { return nil }

func (d *testRecordingDest) DataUpdate(partition string, key []byte, seq uint64,
	val []byte, cas uint64, extrasType DestExtrasType, extras []byte) error {
	d.parkOnUpdateGate()
	d.dataUpdates = append(d.dataUpdates, seq)
	return nil
}

func (d *testRecordingDest) DataUpdateEx(partition string, key []byte, seq uint64,
	val []byte, cas uint64, extrasType DestExtrasType, req interface{}) error {
	d.parkOnUpdateGate()
	d.dataUpdates = append(d.dataUpdates, seq)
	return nil
}

func (d *testRecordingDest) DataDelete(partition string, key []byte, seq uint64,
	cas uint64, extrasType DestExtrasType, extras []byte) error {
	d.dataDeletes = append(d.dataDeletes, seq)
	return nil
}

func (d *testRecordingDest) DataDeleteEx(partition string, key []byte, seq uint64,
	cas uint64, extrasType DestExtrasType, req interface{}) error {
	d.dataDeletes = append(d.dataDeletes, seq)
	return nil
}

func (d *testRecordingDest) SnapshotStart(partition string, snapStart, snapEnd uint64) error {
	d.snapshotStarts++
	return nil
}

func (d *testRecordingDest) OpaqueGet(partition string) ([]byte, uint64, error) {
	return nil, 0, nil
}

func (d *testRecordingDest) OpaqueSet(partition string, value []byte) error {
	d.opaqueSets++
	return nil
}

func (d *testRecordingDest) Rollback(partition string, rollbackSeq uint64) error {
	return nil
}

func (d *testRecordingDest) RollbackEx(partition string, partitionUUID uint64,
	rollbackSeq uint64) error {
	return nil
}

func (d *testRecordingDest) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel ConsistencyLevel, consistencySeq uint64,
	cancelCh <-chan bool) error {
	return nil
}

func (d *testRecordingDest) Count(pindex *PIndex, cancelCh <-chan bool) (uint64, error) {
	return 0, nil
}

func (d *testRecordingDest) Query(pindex *PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return nil
}

func (d *testRecordingDest) Stats(w io.Writer) error { return nil }

// TestFeedCloseDropsStaleObserverEvents marks a feed closed the way
// close() does, then invokes the StreamObserver callbacks as a
// still-draining DCP connection would: no event may reach the Dest, and
// each drop must be counted.
func TestFeedCloseDropsStaleObserverEvents(t *testing.T) {
	rec := &testRecordingDest{}

	f := &GocbcoreDCPFeed{
		name:       "closed-feed",
		indexName:  "close-test-index",
		bucketName: "b",
		pf:         BasicPartitionFunc,
		dests:      map[string]Dest{"0": rec},
		stats:      NewDestStats(),
		dcpStats:   &gocbcoreDCPFeedStats{},
		currVBs:    []*vbucketState{{snapStart: 21, snapEnd: 25, snapSaved: true}},
		// snapSaved: the marker's metadata was already persisted before
		// close, so checkAndUpdateVBucketState skips the Dest.
		lastReceivedSeqno:   make([]uint64, 1),
		supportsCollections: false,
		closeCh:             make(chan struct{}),
	}

	// Torn down: close() sets closed and mirrors it into closedAtomic.
	f.m.Lock()
	f.closed = true
	atomic.StoreUint32(&f.closedAtomic, 1)
	f.m.Unlock()

	// Late/buffered events arrive from the (still-draining) DCP stream.
	f.SnapshotMarker(gocbcore.DcpSnapshotMarker{
		StartSeqNo: 21,
		EndSeqNo:   25,
		VbID:       0,
	})

	f.Mutation(gocbcore.DcpMutation{
		SeqNo:    21,
		RevNo:    21,
		Cas:      1,
		VbID:     0,
		Datatype: 1,
		Key:      []byte("K7"),
		Value:    []byte(`{"name":"K7","counter":2}`),
	})

	f.Deletion(gocbcore.DcpDeletion{
		SeqNo: 22,
		RevNo: 22,
		Cas:   2,
		VbID:  0,
		Key:   []byte("K7"),
	})

	// The fence: nothing reached the Dest.
	if len(rec.dataUpdates) != 0 {
		t.Errorf("closed feed: DataUpdateEx seqs delivered = %v, want none "+
			"(observer fence in Mutation())", rec.dataUpdates)
	}
	if len(rec.dataDeletes) != 0 {
		t.Errorf("closed feed: DataDeleteEx seqs delivered = %v, want none "+
			"(observer fence in Deletion())", rec.dataDeletes)
	}
	if rec.snapshotStarts != 0 {
		t.Errorf("closed feed: SnapshotStart calls = %d, want 0 "+
			"(observer fence in SnapshotMarker())", rec.snapshotStarts)
	}

	// The feed's own bookkeeping did not advance either.
	if f.lastReceivedSeqno[0] != 0 {
		t.Errorf("closed feed: lastReceivedSeqno = %d, want 0", f.lastReceivedSeqno[0])
	}

	// Each dropped event was counted.
	if got := atomic.LoadUint64(&f.dcpStats.TotDCPStaleEventsDropped); got != 3 {
		t.Errorf("closed feed: TotDCPStaleEventsDropped = %d, want 3", got)
	}
	if got := atomic.LoadUint64(&f.dcpStats.TotDCPMutations); got != 0 {
		t.Errorf("closed feed: TotDCPMutations = %d, want 0", got)
	}
	if got := atomic.LoadUint64(&f.dcpStats.TotDCPDeletions); got != 0 {
		t.Errorf("closed feed: TotDCPDeletions = %d, want 0", got)
	}
}

// TestFeedCloseInFlightCallbackNotFenced documents the fence's known,
// accepted limitation: it is necessary but not sufficient. close() never
// waits for in-flight observer callbacks, so a callback that passed the
// check while the feed was open and then blocked inside the Dest call
// completes its delivery after close() returns -- by which point a
// successor feed may have applied a later delete to the same Dest. The
// companion cbft seq guard is what neutralizes this window.
func TestFeedCloseInFlightCallbackNotFenced(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	rec := &testRecordingDest{
		enteredUpdate: entered,
		updateGate:    gate,
	}

	f := &GocbcoreDCPFeed{
		name:                "closed-feed",
		indexName:           "close-test-index",
		bucketName:          "b",
		pf:                  BasicPartitionFunc,
		dests:               map[string]Dest{"0": rec},
		stats:               NewDestStats(),
		dcpStats:            &gocbcoreDCPFeedStats{},
		currVBs:             []*vbucketState{{snapStart: 21, snapEnd: 25, snapSaved: true}},
		lastReceivedSeqno:   make([]uint64, 1),
		supportsCollections: false,
		closeCh:             make(chan struct{}),
	}
	// NOTE: feed is OPEN here.

	// A buffered mutation passes the (open) fence and parks in the Dest.
	mutationDone := make(chan struct{})
	go func() {
		defer close(mutationDone)
		f.Mutation(gocbcore.DcpMutation{
			SeqNo:    21,
			RevNo:    21,
			Cas:      1,
			VbID:     0,
			Datatype: 1,
			Key:      []byte("K7"),
			Value:    []byte(`{"name":"K7","counter":2}`),
		})
	}()
	<-entered

	// Tear the feed down while the callback is in flight.
	f.m.Lock()
	f.closed = true
	atomic.StoreUint32(&f.closedAtomic, 1)
	f.m.Unlock()

	// (A successor feed would now be re-streaming into the same Dest and
	// could apply delete(K7)@22 here.)

	// Released, the stale op REACHES the Dest despite the feed being
	// closed -- the fence cannot catch it.
	close(gate)
	<-mutationDone

	if len(rec.dataUpdates) != 1 || rec.dataUpdates[0] != 21 {
		t.Errorf("in-flight callback: DataUpdateEx seqs delivered = %v,"+
			" want [21] (delivery completed past the fence)", rec.dataUpdates)
	}
	if got := atomic.LoadUint64(&f.dcpStats.TotDCPStaleEventsDropped); got != 0 {
		t.Errorf("in-flight callback: TotDCPStaleEventsDropped = %d,"+
			" want 0 (the fence never saw this op)", got)
	}
}

// ----------------------------------------------------------------
// MB-70770 layer 1: local stream-routing teardown via retained
// OpenStream PendingOps.

// fakePendingOp is a fake gocbcore.PendingOp recording Cancel calls.
type fakePendingOp struct {
	cancels int32
}

func (o *fakePendingOp) Cancel() {
	atomic.AddInt32(&o.cancels, 1)
}

// TestFeedCloseCancelsRetainedStreamOps: a closing feed must Cancel()
// every retained OpenStream PendingOp, removing each stream's routing
// entry so buffered packets are dropped as orphans. This is the teardown
// that still works when CloseStream never reaches the owning node.
func TestFeedCloseCancelsRetainedStreamOps(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1, 2}
	feed := newTestFeed(t, mgr, "feed-cancel-ops", vbs, nil)

	// Simulate initiateStreamEx having retained each stream's op.
	fakes := map[uint16]*fakePendingOp{}
	for _, vb := range vbs {
		fakes[vb] = &fakePendingOp{}
		feed.retainStreamOp(vb, fakes[vb])
	}

	if err := feed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, vb := range vbs {
		if got := atomic.LoadInt32(&fakes[vb].cancels); got != 1 {
			t.Errorf("close: vb %d retained op Cancel calls = %d, want 1",
				vb, got)
		}
	}

	feed.m.Lock()
	remaining := len(feed.streamOps)
	feed.m.Unlock()
	if remaining != 0 {
		t.Errorf("close: %d retained stream ops left, want 0", remaining)
	}

	// Close is idempotent: a second Close must not re-cancel.
	if err := feed.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for _, vb := range vbs {
		if got := atomic.LoadInt32(&fakes[vb].cancels); got != 1 {
			t.Errorf("second close: vb %d Cancel calls = %d, want 1", vb, got)
		}
	}
}

// TestRetainStreamOpAfterCloseCancelsImmediately: when close() runs
// between initiateStreamEx's closed-check and the retain, the op must be
// canceled on the spot, never left routable.
func TestRetainStreamOpAfterCloseCancelsImmediately(t *testing.T) {
	rec := &testRecordingDest{}
	f := &GocbcoreDCPFeed{
		name:              "late-retain-feed",
		pf:                BasicPartitionFunc,
		dests:             map[string]Dest{"0": rec},
		stats:             NewDestStats(),
		dcpStats:          &gocbcoreDCPFeedStats{},
		currVBs:           []*vbucketState{{}},
		lastReceivedSeqno: make([]uint64, 1),
		closeCh:           make(chan struct{}),
	}
	f.m.Lock()
	f.closed = true
	atomic.StoreUint32(&f.closedAtomic, 1)
	f.m.Unlock()

	op := &fakePendingOp{}
	f.retainStreamOp(0, op)

	if got := atomic.LoadInt32(&op.cancels); got != 1 {
		t.Errorf("late retain on closed feed: Cancel calls = %d, want 1", got)
	}
	f.m.Lock()
	remaining := len(f.streamOps)
	f.m.Unlock()
	if remaining != 0 {
		t.Errorf("late retain on closed feed: op was retained (%d entries)",
			remaining)
	}
}

// TestFeedEndClearsRetainedStreamOp: a terminal End must drop the vb's
// retained handle, so the map cannot accumulate stale ones.
func TestFeedEndClearsRetainedStreamOp(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1}
	feed := newTestFeed(t, mgr, "feed-end-clears", vbs, nil)

	op0 := &fakePendingOp{}
	op1 := &fakePendingOp{}
	feed.retainStreamOp(0, op0)
	feed.retainStreamOp(1, op1)

	// vb 0's stream ends naturally.
	feed.End(gocbcore.DcpStreamEnd{VbID: 0}, nil)

	feed.m.Lock()
	_, has0 := feed.streamOps[0]
	_, has1 := feed.streamOps[1]
	feed.m.Unlock()
	if has0 {
		t.Errorf("End(vb 0): retained op not cleared")
	}
	if !has1 {
		t.Errorf("End(vb 0): vb 1's retained op should remain")
	}
	// End itself never cancels; that is close()'s job.
	if got := atomic.LoadInt32(&op0.cancels); got != 0 {
		t.Errorf("End(vb 0): Cancel calls = %d, want 0", got)
	}

	// Feed close still cancels the remaining retained op (vb 1) — and
	// only that one.
	if err := feed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := atomic.LoadInt32(&op0.cancels); got != 0 {
		t.Errorf("close after End: vb 0 Cancel calls = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&op1.cancels); got != 1 {
		t.Errorf("close after End: vb 1 Cancel calls = %d, want 1", got)
	}
}

// TestRetainStreamOpSupersededCanceled: retaining a new op for a vb that
// already has one (the End-driven reconnect shape, which does not
// complete() the vb) must cancel the superseded handle -- overwriting it
// would leave its routing entry uncancelable at close.
func TestRetainStreamOpSupersededCanceled(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	feed := newTestFeed(t, mgr, "feed-supersede", []uint16{0}, nil)

	opA := &fakePendingOp{}
	opB := &fakePendingOp{}
	feed.retainStreamOp(0, opA)
	feed.retainStreamOp(0, opB) // reconnect retains a fresh op

	if got := atomic.LoadInt32(&opA.cancels); got != 1 {
		t.Errorf("superseded op Cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&opB.cancels); got != 0 {
		t.Errorf("replacement op Cancel calls = %d, want 0", got)
	}
	feed.m.Lock()
	retained := feed.streamOps[0]
	feed.m.Unlock()
	if retained != gocbcore.PendingOp(opB) {
		t.Errorf("retained op for vb 0 is not the replacement")
	}
	// Superseding must not disturb the live stream's bookkeeping.
	feed.m.Lock()
	active := feed.active[0]
	feed.m.Unlock()
	if !active {
		t.Errorf("vb 0 no longer active after supersede")
	}

	// Close cancels the replacement (once) and never re-cancels A.
	if err := feed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := atomic.LoadInt32(&opA.cancels); got != 1 {
		t.Errorf("after close: superseded op Cancel calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&opB.cancels); got != 1 {
		t.Errorf("after close: replacement op Cancel calls = %d, want 1", got)
	}
}
