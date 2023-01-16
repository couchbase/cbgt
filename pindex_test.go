//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"container/list"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/rcrowley/go-metrics"
)

func TestOpenPIndex(t *testing.T) {
	pindex, err := OpenPIndex(nil, "not-a-real-file")
	if pindex != nil || err == nil {
		t.Errorf("expected OpenPIndex to fail on a bad file")
	}
}

func TestNewPIndex(t *testing.T) {
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := NewPIndex(nil, "fake", "uuid",
		"blackhole", "indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID",
		"sourceParams", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to work")
	}
	err = pindex.Close(true)
	if err != nil {
		t.Errorf("expected Close to work")
	}
}

func TestClonePIndex(t *testing.T) {
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := NewPIndex(nil, "fake", "uuid",
		"blackhole", "indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID",
		"sourceParams", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to work")
	}
	clone := pindex.Clone()
	if !reflect.DeepEqual(pindex, clone) {
		t.Errorf("expected clone to be exactly similar to pindex")
	}
	err = pindex.Close(true)
	if err != nil {
		t.Errorf("expected Close to work")
	}
	err = clone.Close(true)
	if err != nil {
		t.Errorf("expected Close to work on clone")
	}
}

func TestNewPIndexImpl(t *testing.T) {
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	restart := func() {
		t.Errorf("not expecting a restart")
	}

	indexParams := ""

	pindexImpl, dest, err :=
		NewPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE",
			indexParams, emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on unknown impl type")
	}

	pindexImpl, dest, err =
		OpenPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE", emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on unknown impl type")
	}
}

func TestBlackholePIndexImpl(t *testing.T) {
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	restart := func() {
		t.Errorf("not expecting a restart")
	}

	pindex, dest, err :=
		OpenBlackHolePIndexImpl("blackhole", emptyDir, restart)
	if err == nil || pindex != nil || dest != nil {
		t.Errorf("expected OpenBlackHolePIndexImpl to error on emptyDir")
	}

	pindex, dest, err =
		NewBlackHolePIndexImpl("blackhole", "", emptyDir, restart)
	if err != nil || pindex == nil || dest == nil {
		t.Errorf("expected NewBlackHolePIndexImpl to work")
	}

	pindex, dest, err =
		OpenBlackHolePIndexImpl("blackhole", emptyDir, restart)
	if err != nil || pindex == nil || dest == nil {
		t.Errorf("expected OpenBlackHolePIndexImpl to work")
	}

	if dest.Close() != nil ||
		dest.DataUpdate("", nil, 0, nil,
			0, DEST_EXTRAS_TYPE_NIL, nil) != nil ||
		dest.DataDelete("", nil, 0,
			0, DEST_EXTRAS_TYPE_NIL, nil) != nil ||
		dest.SnapshotStart("", 0, 0) != nil ||
		dest.OpaqueSet("", nil) != nil ||
		dest.Rollback("", 0) != nil ||
		dest.ConsistencyWait("", "", "", 0, nil) != nil ||
		dest.Query(nil, nil, nil, nil) != nil {
		t.Errorf("expected no errors from a blackhole pindex impl")
	}

	c, err := dest.Count(nil, nil)
	if err != nil || c != 0 {
		t.Errorf("expected 0, no err")
	}

	b := &bytes.Buffer{}
	err = dest.Stats(b)
	if err != nil {
		t.Errorf("expected 0, no err")
	}
	if string(b.Bytes()) != "null" {
		t.Errorf("expected null")
	}

	v, lastSeq, err := dest.OpaqueGet("")
	if err != nil || v != nil || lastSeq != 0 {
		t.Errorf("expected nothing from blackhole.OpaqueGet()")
	}

	bt := PIndexImplTypes["blackhole"]
	if bt == nil {
		t.Errorf("expected blackhole in PIndexImplTypes")
	}
	if bt.New == nil || bt.Open == nil {
		t.Errorf("blackhole should have open and new funcs")
	}
	if bt.Count != nil {
		t.Errorf("expected blackhole count nil")
	}
	if bt.Query != nil {
		t.Errorf("expected blackhole query nil")
	}
}

func TestErrorConsistencyWait(t *testing.T) {
	e := &ErrorConsistencyWait{}
	if e.Error() == "" {
		t.Errorf("expected err")
	}
}

func TestErrorConsistencyWaitDone(t *testing.T) {
	currSeqFunc := func() uint64 {
		return 101
	}

	cancelCh := make(chan bool)
	doneCh := make(chan error)

	var cwdErr error
	endCh := make(chan struct{})

	go func() {
		cwdErr = ConsistencyWaitDone("partition",
			cancelCh,
			doneCh,
			currSeqFunc)
		close(endCh)
	}()

	close(cancelCh)

	<-endCh

	if cwdErr == nil {
		t.Errorf("expected err")
	}

	// --------------------------

	cancelCh = make(chan bool)
	doneCh = make(chan error)

	cwdErr = nil
	endCh = make(chan struct{})

	go func() {
		cwdErr = ConsistencyWaitDone("partition",
			cancelCh,
			doneCh,
			currSeqFunc)
		close(endCh)
	}()

	doneErr := fmt.Errorf("doneErr")
	doneCh <- doneErr

	<-endCh

	if cwdErr != doneErr {
		t.Errorf("expected doneErr")
	}
}

func TestPIndexStoreStats(t *testing.T) {
	s := PIndexStoreStats{
		TimerBatchStore: metrics.NewTimer(),
		Errors:          list.New(),
	}

	w := bytes.NewBuffer(nil)
	s.WriteJSON(w)
	if w.String() == "" {
		t.Errorf("expected some writes")
	}

	s.Errors.PushBack("hello")
	s.Errors.PushBack("world")

	w2 := bytes.NewBuffer(nil)
	s.WriteJSON(w2)
	if w2.String() == "" {
		t.Errorf("expected some writes")
	}
}
