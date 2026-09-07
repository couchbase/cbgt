//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"testing"
)

func TestSameRev(t *testing.T) {
	tests := []struct {
		a, b interface{}
		want bool
	}{
		{nil, nil, true},
		{nil, []byte("r1"), false},
		{[]byte("r1"), nil, false},
		{[]byte("r1"), []byte("r1"), true},
		{[]byte("r1"), []byte("r2"), false},
		{[]byte(nil), []byte(nil), true},
		// metakv refuses a rev that is not a []byte on a mutation, so
		// such a rev must never be treated as a match.
		{"r1", "r1", false},
		{[]byte("r1"), "r1", false},
	}

	for i, test := range tests {
		if got := sameRev(test.a, test.b); got != test.want {
			t.Errorf("test %d: sameRev(%v, %v) = %t, want %t",
				i, test.a, test.b, got, test.want)
		}
	}
}

// Exercises the CAS <=> metakv rev bookkeeping that backs the CAS
// checks on the non-composite CfgMetaKv entries, without needing a
// metakv to talk to.
func TestCfgMetaKvRevEntries(t *testing.T) {
	c := &CfgMetaKv{revEntries: map[string]cfgMetaKvRev{}}

	rev1, rev2 := []byte("rev-1"), []byte("rev-2")

	cas1 := c.recordRevLOCKED("k", rev1)
	if cas1 == 0 {
		t.Fatalf("expected a non-zero cas")
	}

	// An unchanged rev keeps handing back the same cas, so that a
	// re-read in between a read-compute-write does not spuriously
	// invalidate the cas that the caller is holding.
	if cas := c.recordRevLOCKED("k", rev1); cas != cas1 {
		t.Errorf("expected the cas: %x to be reused for an unchanged"+
			" rev, got: %x", cas1, cas)
	}

	// A separate key gets its own cas, but shares the counter.
	casOther := c.recordRevLOCKED("other", rev1)
	if casOther == cas1 {
		t.Errorf("expected distinct keys to get distinct cas values")
	}

	rev, err := c.revForCASLOCKED("k", cas1)
	if err != nil {
		t.Fatalf("unexpected err for the current cas: %v", err)
	}
	if !sameRev(rev, rev1) {
		t.Errorf("expected rev: %v, got: %v", rev1, rev)
	}

	// A "don't care" cas and a forced cas mutate unconditionally.
	for _, cas := range []uint64{0, CFG_CAS_FORCE} {
		rev, err = c.revForCASLOCKED("k", cas)
		if err != nil || rev != nil {
			t.Errorf("expected a nil rev and no err for cas: %x,"+
				" got rev: %v, err: %v", cas, rev, err)
		}
	}

	// A moved on rev retires the cas that was handed out for the
	// previous one.
	cas2 := c.recordRevLOCKED("k", rev2)
	if cas2 == cas1 {
		t.Fatalf("expected a fresh cas once the rev moved on")
	}

	if _, err = c.revForCASLOCKED("k", cas1); err == nil {
		t.Errorf("expected an err for a retired cas")
	} else if _, ok := err.(*CfgCASError); !ok {
		t.Errorf("expected a CfgCASError for a retired cas, got: %v", err)
	}

	// A cas that was never handed out is a conflict as well.
	if _, err = c.revForCASLOCKED("k", cas2+100); err == nil {
		t.Errorf("expected an err for an unknown cas")
	}
	if _, err = c.revForCASLOCKED("never-read", 1); err == nil {
		t.Errorf("expected an err for an unread key")
	}

	// A cas minted without a rev behind it cannot be CAS'ed on, so
	// that the entry gets re-read rather than blindly overwritten.
	cas3 := c.forgetRevLOCKED("k")
	if cas3 == cas2 {
		t.Fatalf("expected a fresh cas from forgetRevLOCKED")
	}
	if _, err = c.revForCASLOCKED("k", cas3); err == nil {
		t.Errorf("expected an err for a cas with no rev behind it")
	} else if _, ok := err.(*CfgCASError); !ok {
		t.Errorf("expected a CfgCASError for a cas with no rev, got: %v", err)
	}

	// ... and a forced mutation still goes through on such a key.
	if rev, err = c.revForCASLOCKED("k", CFG_CAS_FORCE); err != nil ||
		rev != nil {
		t.Errorf("expected a forced mutation to skip the rev check,"+
			" rev: %v, err: %v", rev, err)
	}
}
