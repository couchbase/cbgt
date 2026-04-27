//  Copyright 2026-Present Couchbase, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// These tests pinpoint why GocbcoreDCPFeed.End() may fail to drive
// stopAfterRemaining to zero in a Sync Gateway-style "one shot"
// (markReached) configuration. They construct the feed by hand so they
// can run without a live Couchbase cluster; the bits of close() that
// would normally need a real *gocbcore.DCPAgent are bypassed by the
// existing early-return in closeAllStreamsLOCKED when nothing is active.
// CloseDCPAgent is a package-level func var, so we replace it with a
// no-op for the duration of these tests.

// ----------------------------------------------------------------
// Test fixtures

// recordingMEH captures OnUnregisterFeed / OnFeedError / OnRegisterPIndex
// calls for assertion.
type recordingMEH struct {
	mu              sync.Mutex
	unregisterFeeds []Feed
	feedErrors      []error
	unregisterCh    chan Feed
}

func newRecordingMEH() *recordingMEH {
	return &recordingMEH{
		unregisterCh: make(chan Feed, 64),
	}
}

func (m *recordingMEH) OnRegisterPIndex(*PIndex)                      {}
func (m *recordingMEH) OnUnregisterPIndex(*PIndex)                    {}
func (m *recordingMEH) OnRefreshManagerOptions(map[string]string)     {}
func (m *recordingMEH) OnFeedError(_ string, _ Feed, err error) {
	m.mu.Lock()
	m.feedErrors = append(m.feedErrors, err)
	m.mu.Unlock()
}
func (m *recordingMEH) OnUnregisterFeed(f Feed) {
	m.mu.Lock()
	m.unregisterFeeds = append(m.unregisterFeeds, f)
	m.mu.Unlock()
	select {
	case m.unregisterCh <- f:
	default:
	}
}

// nopDest is a Dest stub that swallows callbacks. It implements just
// enough of cbgt.Dest for End() handling not to fault.
type nopDest struct{}

func (nopDest) Close(_ bool) error { return nil }
func (nopDest) DataUpdate(string, []byte, uint64, []byte, uint64,
	DestExtrasType, []byte) error {
	return nil
}
func (nopDest) DataDelete(string, []byte, uint64, uint64,
	DestExtrasType, []byte) error {
	return nil
}
func (nopDest) SnapshotStart(string, uint64, uint64) error { return nil }
func (nopDest) OpaqueGet(string) ([]byte, uint64, error)   { return nil, 0, nil }
func (nopDest) OpaqueSet(string, []byte) error             { return nil }
func (nopDest) Rollback(string, uint64) error              { return nil }
func (nopDest) ConsistencyWait(string, string, ConsistencyLevel,
	uint64, <-chan bool) error {
	return nil
}
func (nopDest) Count(*PIndex, <-chan bool) (uint64, error)            { return 0, nil }
func (nopDest) Query(*PIndex, []byte, io.Writer, <-chan bool) error { return nil }
func (nopDest) Stats(io.Writer) error                                { return nil }

// withFakeCloseDCPAgent swaps CloseDCPAgent for a no-op so close()
// does not dereference a nil agent.
func withFakeCloseDCPAgent(t *testing.T) {
	t.Helper()
	orig := CloseDCPAgent
	CloseDCPAgent = func(string, string, *gocbcore.DCPAgent) error { return nil }
	t.Cleanup(func() { CloseDCPAgent = orig })
}

// newTestFeed wires up a GocbcoreDCPFeed with the given vbuckets and
// stopAfter map, registered against mgr. It does not start any DCP
// streams; the caller drives lifecycle via End()/initiateStream-shaped
// pokes against the feed's mutable state.
func newTestFeed(t *testing.T, mgr *Manager, name string,
	vbucketIds []uint16, stopAfter map[string]UUIDSeq) *GocbcoreDCPFeed {
	t.Helper()

	dests := map[string]Dest{}
	for _, vb := range vbucketIds {
		dests[fmt.Sprintf("%d", vb)] = nopDest{}
	}

	largest := uint16(0)
	for _, vb := range vbucketIds {
		if vb > largest {
			largest = vb
		}
	}

	var stopAfterRemaining int32
	if stopAfter != nil {
		for _, vb := range vbucketIds {
			if _, ok := stopAfter[fmt.Sprintf("%d", vb)]; ok {
				stopAfterRemaining++
			}
		}
	}

	currVBs := make([]*vbucketState, largest+1)
	for _, vb := range vbucketIds {
		currVBs[vb] = &vbucketState{}
	}

	feed := &GocbcoreDCPFeed{
		name:               name,
		indexName:          name,
		indexUUID:          name + "-uuid",
		bucketName:         "test-bucket",
		bucketUUID:         "test-bucket-uuid",
		params:             NewDCPFeedParams(),
		pf:                 BasicPartitionFunc,
		dests:              dests,
		mgr:                mgr,
		stopAfter:          stopAfter,
		stopAfterRemaining: stopAfterRemaining,
		vbucketIds:         vbucketIds,
		lastReceivedSeqno:  make([]uint64, largest+1),
		currVBs:            currVBs,
		dcpStats:           &gocbcoreDCPFeedStats{},
		stats:              NewDestStats(),
		active:             make(map[uint16]bool),
		closeCh:            make(chan struct{}),
	}

	// Mark every vbucket active just like initiateStreamEx(isNewStream=true)
	// would. remaining is what wait() blocks on.
	for _, vb := range vbucketIds {
		feed.remaining.Add(1)
		feed.active[vb] = true
	}

	if err := mgr.registerFeed(feed); err != nil {
		t.Fatalf("registerFeed: %v", err)
	}
	return feed
}

func waitForUnregister(t *testing.T, meh *recordingMEH, want string,
	timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case got := <-meh.unregisterCh:
			if got.Name() == want {
				return
			}
		case <-deadline:
			meh.mu.Lock()
			seen := make([]string, 0, len(meh.unregisterFeeds))
			for _, f := range meh.unregisterFeeds {
				seen = append(seen, f.Name())
			}
			meh.mu.Unlock()
			t.Fatalf("timed out waiting for OnUnregisterFeed(%q); seen=%v",
				want, seen)
		}
	}
}

// ----------------------------------------------------------------
// Tests

// TestStopAfterParamsRoundTrip mirrors the SG CBG-5184 path:
//
//	SGFeedSourceParams { DCPFeedParams; StopAfterSourceParams; DbName }
//
// is marshalled by sync_gateway and the resulting JSON is what cbgt's
// newGocbcoreDCPFeed unmarshals into a StopAfterSourceParams. This
// catches the silent failure mode where the stopAfter / markPartitionSeqs
// JSON tags are dropped along the way and f.stopAfter ends up nil.
func TestStopAfterParamsRoundTrip(t *testing.T) {
	type sgFeedSourceParams struct {
		DCPFeedParams
		StopAfterSourceParams
		DbName string `json:"sg_dbname,omitempty"`
	}

	const numVbs = 8
	in := sgFeedSourceParams{
		DCPFeedParams: DCPFeedParams{
			AutoReconnectAfterRollback: true,
			IncludeXAttrs:              true,
			Scope:                      "_default",
			Collections:                []string{"_default"},
		},
		StopAfterSourceParams: StopAfterSourceParams{
			StopAfter:         "markReached",
			MarkPartitionSeqs: map[string]UUIDSeq{},
		},
		DbName: "db1",
	}
	for vb := 0; vb < numVbs; vb++ {
		in.MarkPartitionSeqs[fmt.Sprintf("%d", vb)] =
			UUIDSeq{Seq: uint64(100 + vb)}
	}

	bytes, err := MarshalJSON(in)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var out StopAfterSourceParams
	if err := UnmarshalJSON(bytes, &out); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	if out.StopAfter != "markReached" {
		t.Fatalf("StopAfter not preserved: got %q want %q",
			out.StopAfter, "markReached")
	}
	if got, want := len(out.MarkPartitionSeqs), numVbs; got != want {
		t.Fatalf("MarkPartitionSeqs lost entries: got %d want %d", got, want)
	}
	for vb := 0; vb < numVbs; vb++ {
		key := fmt.Sprintf("%d", vb)
		us, ok := out.MarkPartitionSeqs[key]
		if !ok {
			t.Fatalf("missing partition %q after round-trip", key)
		}
		if us.Seq != uint64(100+vb) {
			t.Fatalf("partition %q seq mismatch: got %d want %d",
				key, us.Seq, 100+vb)
		}
	}
}

// TestEndNaturalCompletionTriggersUnregister is the happy path:
// every stopAfter-targeted vb gets a natural End(err==nil), so the feed
// must self-close, OnUnregisterFeed must fire, and CompletedPartitions()
// must contain each vb that hit the natural-end pathway.
func TestEndNaturalCompletionTriggersUnregister(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1, 2, 3}
	stopAfter := map[string]UUIDSeq{
		"0": {Seq: 10},
		"1": {Seq: 20},
		"2": {Seq: 30},
		"3": {Seq: 40},
	}
	feed := newTestFeed(t, mgr, "feed-natural", vbs, stopAfter)

	for _, vb := range vbs {
		feed.End(gocbcore.DcpStreamEnd{VbID: vb}, nil)
	}

	waitForUnregister(t, meh, "feed-natural", 2*time.Second)

	if got := feed.stopAfterRemaining; got != 0 {
		t.Fatalf("stopAfterRemaining: got %d want 0", got)
	}

	got := feed.CompletedPartitions()
	if len(got) != len(vbs) {
		t.Fatalf("CompletedPartitions: got %d entries, want %d (%v)",
			len(got), len(vbs), got)
	}
	for _, vb := range vbs {
		key := fmt.Sprintf("%d", vb)
		if _, ok := got[key]; !ok {
			t.Fatalf("CompletedPartitions: missing partition %q (%v)",
				key, got)
		}
	}
}

// TestEndNaturalCompletionWithExtraVbuckets covers the subset case:
// the feed has more vbuckets than stopAfter targets. Only the targeted
// ones drive the counter; the extras are force-closed inside Close().
// Without the early-return guard in closeAllStreamsLOCKED this would
// panic on the nil agent.
func TestEndNaturalCompletionWithExtraVbuckets(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1, 2, 3, 4, 5}
	stopAfter := map[string]UUIDSeq{
		"0": {Seq: 10},
		"1": {Seq: 20},
		"2": {Seq: 30},
	}
	feed := newTestFeed(t, mgr, "feed-subset", vbs, stopAfter)

	// Drain the non-targeted vbs first so closeAllStreamsLOCKED has nothing
	// to do. In a real feed Close() would invoke agent.CloseStream(); here
	// we make those vbs inactive via complete() so the early-return guard
	// kicks in.
	for _, vb := range []uint16{3, 4, 5} {
		feed.complete(vb)
	}

	for _, vb := range []uint16{0, 1, 2} {
		feed.End(gocbcore.DcpStreamEnd{VbID: vb}, nil)
	}

	waitForUnregister(t, meh, "feed-subset", 2*time.Second)
}

// TestEndFilterEmptyInStopAfterShutsDown locks in the semantics of
// gocbcore.ErrDCPStreamFilterEmpty: it is the KV FilterEmpty (0x07)
// end-stream reason, raised when the subscribed collection or scope
// has been dropped. That is a misconfiguration / external state
// change, not a natural completion, so the feed must surface it via
// OnFeedError (initiateShutdown) regardless of stopAfter mode and
// must NOT advance stopAfterRemaining or CompletedPartitions for the
// affected vb.
func TestEndFilterEmptyInStopAfterShutsDown(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1, 2}
	stopAfter := map[string]UUIDSeq{
		"0": {Seq: 10},
		"1": {Seq: 20},
		"2": {Seq: 30},
	}
	feed := newTestFeed(t, mgr, "feed-filter-empty", vbs, stopAfter)

	feed.End(gocbcore.DcpStreamEnd{VbID: 0}, nil)
	feed.End(gocbcore.DcpStreamEnd{VbID: 1}, nil)
	feed.End(gocbcore.DcpStreamEnd{VbID: 2},
		gocbcore.ErrDCPStreamFilterEmpty)

	// Counter must remain >0 (the filter-empty vb did not "complete").
	if got := feed.stopAfterRemaining; got != 1 {
		t.Fatalf("stopAfterRemaining: got %d want 1 "+
			"(filter-empty must NOT decrement)", got)
	}

	// OnFeedError must have surfaced the filter-empty exit.
	deadline := time.After(500 * time.Millisecond)
	for {
		meh.mu.Lock()
		gotErrs := len(meh.feedErrors)
		meh.mu.Unlock()
		if gotErrs > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected OnFeedError to be called for filter-empty " +
				"in stopAfter mode")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// OnUnregisterFeed must NOT fire (counter still wedged).
	select {
	case f := <-meh.unregisterCh:
		t.Fatalf("unexpected OnUnregisterFeed for %s", f.Name())
	case <-time.After(100 * time.Millisecond):
	}

	// Only the two natural-end vbs should be in CompletedPartitions.
	got := feed.CompletedPartitions()
	if len(got) != 2 {
		t.Fatalf("CompletedPartitions: got %d entries, want 2 (%v)",
			len(got), got)
	}
	if _, ok := got["2"]; ok {
		t.Fatalf("CompletedPartitions unexpectedly contains the "+
			"filter-empty vb \"2\": %v", got)
	}
}

// TestEndFilterEmptyOutsideStopAfterStillShutsDown ensures the
// non-stopAfter / non-targeted path still invokes initiateShutdown
// and surfaces OnFeedError. This locks the existing behaviour for
// continuous feeds.
func TestEndFilterEmptyOutsideStopAfterStillShutsDown(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0}
	feed := newTestFeed(t, mgr, "feed-filter-empty-no-stopafter", vbs, nil)

	feed.End(gocbcore.DcpStreamEnd{VbID: 0},
		gocbcore.ErrDCPStreamFilterEmpty)

	deadline := time.After(500 * time.Millisecond)
	for {
		meh.mu.Lock()
		gotErrs := len(meh.feedErrors)
		meh.mu.Unlock()
		if gotErrs > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected OnFeedError to be called for filter-empty " +
				"in non-stopAfter mode")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestEndConsumerClosedDoesNotDecrementStopAfter covers the other
// silent path: ErrDCPStreamClosed (consumer-initiated close) calls
// complete() but never decrements stopAfterRemaining. In the real
// feed this is intentional because the consumer is the feed itself
// during shutdown, but if anything else calls agent.CloseStream out
// of band, the counter is wedged.
func TestEndConsumerClosedDoesNotDecrementStopAfter(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1}
	stopAfter := map[string]UUIDSeq{
		"0": {Seq: 10},
		"1": {Seq: 20},
	}
	feed := newTestFeed(t, mgr, "feed-consumer-closed", vbs, stopAfter)

	feed.End(gocbcore.DcpStreamEnd{VbID: 0}, nil)
	feed.End(gocbcore.DcpStreamEnd{VbID: 1},
		gocbcore.ErrDCPStreamClosed)

	select {
	case f := <-meh.unregisterCh:
		t.Fatalf("unexpected OnUnregisterFeed for %s", f.Name())
	case <-time.After(200 * time.Millisecond):
	}
	if got := feed.stopAfterRemaining; got != 1 {
		t.Fatalf("stopAfterRemaining: got %d want 1", got)
	}
}

// TestStopAfterMissingPartitionDoesNotDecrement is the SG bug class
// where MarkPartitionSeqs is built from bucket-level high seqnos but
// some vbuckets are absent from the map (or use a different key
// format). The natural End for those vbs does not match the
// stopAfter map and is silently ignored.
func TestStopAfterMissingPartitionDoesNotDecrement(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1, 2}
	// Only vbs 0 and 1 are in stopAfter; vb 2 is missing.
	stopAfter := map[string]UUIDSeq{
		"0": {Seq: 10},
		"1": {Seq: 20},
	}
	feed := newTestFeed(t, mgr, "feed-missing-partition", vbs, stopAfter)

	if feed.stopAfterRemaining != 2 {
		t.Fatalf("stopAfterRemaining init: got %d want 2",
			feed.stopAfterRemaining)
	}

	// Natural End on vb 2 must be a no-op for the counter.
	feed.End(gocbcore.DcpStreamEnd{VbID: 2}, nil)
	if got := feed.stopAfterRemaining; got != 2 {
		t.Fatalf("stopAfterRemaining: got %d want 2 "+
			"(end on non-stopAfter vb must not decrement)", got)
	}

	// Now drive the two real targets to completion.
	feed.End(gocbcore.DcpStreamEnd{VbID: 0}, nil)
	feed.End(gocbcore.DcpStreamEnd{VbID: 1}, nil)

	waitForUnregister(t, meh, "feed-missing-partition", 2*time.Second)
}

// TestSelfCloseFiresUnregisterOnce sanity-checks that even if Close()
// is invoked from multiple goroutines (e.g. stopAfter completion
// racing with an explicit cbgtContext.Stop), OnUnregisterFeed is
// fired exactly once.
func TestSelfCloseFiresUnregisterOnce(t *testing.T) {
	withFakeCloseDCPAgent(t)

	meh := newRecordingMEH()
	mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		"", "", meh)

	vbs := []uint16{0, 1}
	stopAfter := map[string]UUIDSeq{
		"0": {Seq: 10},
		"1": {Seq: 20},
	}
	feed := newTestFeed(t, mgr, "feed-once", vbs, stopAfter)

	feed.End(gocbcore.DcpStreamEnd{VbID: 0}, nil)
	feed.End(gocbcore.DcpStreamEnd{VbID: 1}, nil)
	waitForUnregister(t, meh, "feed-once", 2*time.Second)

	// Concurrent extra Close attempts must be no-ops.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = feed.Close() }()
	}
	wg.Wait()

	meh.mu.Lock()
	count := 0
	for _, f := range meh.unregisterFeeds {
		if f.Name() == "feed-once" {
			count++
		}
	}
	meh.mu.Unlock()
	if count != 1 {
		t.Fatalf("OnUnregisterFeed for feed-once fired %d times, want 1", count)
	}
}

// TestCompletedPartitionsDistinguishesTeardownFromCompletion is the
// scenario the reviewer cares about: inside OnUnregisterFeed, type-assert
// to FeedPartitionCompletion and compare CompletedPartitions() to the
// set of vbuckets the feed was tracking. If the sets match, every vb
// reached the natural-end pathway; otherwise the manager tore the feed
// down before some vbs got there.
func TestCompletedPartitionsDistinguishesTeardownFromCompletion(t *testing.T) {
	withFakeCloseDCPAgent(t)

	t.Run("complete", func(t *testing.T) {
		meh := newRecordingMEH()
		mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
			"", "", meh)

		vbs := []uint16{0, 1, 2}
		stopAfter := map[string]UUIDSeq{
			"0": {Seq: 10},
			"1": {Seq: 20},
			"2": {Seq: 30},
		}
		feed := newTestFeed(t, mgr, "feed-complete", vbs, stopAfter)

		for _, vb := range vbs {
			feed.End(gocbcore.DcpStreamEnd{VbID: vb}, nil)
		}
		waitForUnregister(t, meh, "feed-complete", 2*time.Second)

		// SG-side check via the public interface.
		var completion FeedPartitionCompletion = feed
		got := completion.CompletedPartitions()
		for _, vb := range vbs {
			if _, ok := got[fmt.Sprintf("%d", vb)]; !ok {
				t.Fatalf("vb %d missing from CompletedPartitions: %v",
					vb, got)
			}
		}
	})

	t.Run("teardown", func(t *testing.T) {
		meh := newRecordingMEH()
		mgr := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
			"", "", meh)

		vbs := []uint16{0, 1, 2}
		stopAfter := map[string]UUIDSeq{
			"0": {Seq: 10},
			"1": {Seq: 20},
			"2": {Seq: 30},
		}
		feed := newTestFeed(t, mgr, "feed-teardown", vbs, stopAfter)

		// Only vb 0 reaches natural end. vbs 1 and 2 are still in
		// progress. Inspecting CompletedPartitions at this point models
		// what a manager-driven teardown's OnUnregisterFeed would see:
		// only the vbs that hit a terminal-success path are present,
		// regardless of how many vbs the feed was tracking.
		feed.End(gocbcore.DcpStreamEnd{VbID: 0}, nil)

		got := feed.CompletedPartitions()
		if len(got) != 1 {
			t.Fatalf("CompletedPartitions: got %d entries, want 1 (%v)",
				len(got), got)
		}
		if _, ok := got["0"]; !ok {
			t.Fatalf("vb 0 missing from CompletedPartitions: %v", got)
		}
		for _, vb := range []uint16{1, 2} {
			if _, ok := got[fmt.Sprintf("%d", vb)]; ok {
				t.Fatalf("vb %d unexpectedly present in CompletedPartitions: %v",
					vb, got)
			}
		}
	})
}

