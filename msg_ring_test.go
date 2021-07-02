//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestMsgRing(t *testing.T) {
	if m, err := NewMsgRing(nil, 0); err == nil || m != nil {
		t.Errorf("expected no inner io.Writer to fail")
	}

	if m, err := NewMsgRing(os.Stderr, 0); err == nil || m != nil {
		t.Errorf("expected 0 ring size io.Writer to fail")
	}

	if m, err := NewMsgRing(os.Stderr, -1); err == nil || m != nil {
		t.Errorf("expected 0 ring size io.Writer to fail")
	}

	// ------------------------------------------------

	m, err := NewMsgRing(ioutil.Discard, 1)
	if err != nil || m == nil {
		t.Errorf("expected NewMsgRing to work")
	}
	msgs := m.Messages()
	if msgs == nil || len(msgs) != 0 {
		t.Errorf("expected messages to be empty")
	}

	n, err := m.Write([]byte("test0\n"))
	if err != nil || n != 6 {
		t.Errorf("expected write to work")
	}
	msgs = m.Messages()
	if len(msgs) != 1 {
		t.Errorf("expected messages to have 1 msg")
	}
	if string(msgs[0]) != "test0\n" {
		t.Errorf("expected messages[0] to equal test0")
	}

	n, err = m.Write([]byte("test1\n"))
	if err != nil || n != 6 {
		t.Errorf("expected write to work")
	}
	msgs = m.Messages()
	if len(msgs) != 1 {
		t.Errorf("expected messages to still have 1 msg")
	}
	if string(msgs[0]) != "test1\n" {
		t.Errorf("expected messages[0] to equal test1")
	}

	// ------------------------------------------------

	m, err = NewMsgRing(ioutil.Discard, 2)
	if err != nil || m == nil {
		t.Errorf("expected NewMsgRing to work")
	}
	msgs = m.Messages()
	if msgs == nil || len(msgs) != 0 {
		t.Errorf("expected messages to be empty")
	}

	n, err = m.Write([]byte("test0\n"))
	if err != nil || n != 6 {
		t.Errorf("expected write to work")
	}
	msgs = m.Messages()
	if len(msgs) != 1 {
		t.Errorf("expected messages to have 1 msg")
	}
	if string(msgs[0]) != "test0\n" {
		t.Errorf("expected messages[0] to equal test0")
	}

	n, err = m.Write([]byte("test1\n"))
	if err != nil || n != 6 {
		t.Errorf("expected write to work")
	}
	msgs = m.Messages()
	if len(msgs) != 2 {
		t.Errorf("expected messages to still have 2 msgs")
	}
	if string(msgs[0]) != "test0\n" {
		t.Errorf("expected messages[0] to equal test0")
	}
	if string(msgs[1]) != "test1\n" {
		t.Errorf("expected messages[1] to equal test1")
	}

	n, err = m.Write([]byte("test2\n"))
	if err != nil || n != 6 {
		t.Errorf("expected write to work")
	}
	msgs = m.Messages()
	if len(msgs) != 2 {
		t.Errorf("expected messages to still have 2 msgs")
	}
	if string(msgs[0]) != "test1\n" {
		t.Errorf("expected messages[0] to equal test1")
	}
	if string(msgs[1]) != "test2\n" {
		t.Errorf("expected messages[1] to equal test2")
	}
}
