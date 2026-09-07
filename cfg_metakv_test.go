//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build metakv_test
// +build metakv_test

package cbgt

import (
	"fmt"
	"testing"
)

func compareNodeDefs(a, b *NodeDefs) bool {
	for k, v := range a.NodeDefs {
		m := b.NodeDefs[k]
		fmt.Printf("nodedefs %v %v\n", m, v)
		if m.UUID != v.UUID {
			return false
		}
	}
	return true
}

func splitKeyTest(g *CfgMetaKv, t *testing.T, splitKey, uuid string) {
	nodeDefs, cas1, err := CfgGetNodeDefs(g, splitKey)
	if err != nil {
		t.Errorf("unexpected error getting node defs: %v", err)
		return
	}

	// expecting empty node defs at this point.
	if nodeDefs == nil {
		nodeDefs = NewNodeDefs(CfgGetVersion(g))
	}

	nodeDefs.NodeDefs[uuid] = &NodeDef{
		HostPort:    "13",
		UUID:        uuid,
		ImplVersion: CfgGetVersion(g),
	}
	uuid2 := NewUUID()
	nodeDefs.NodeDefs[uuid2] = &NodeDef{
		HostPort:    "14",
		UUID:        uuid2,
		ImplVersion: CfgGetVersion(g),
	}

	_, err = CfgSetNodeDefs(g, splitKey, nodeDefs, cas1)
	if err != nil {
		t.Errorf("unexpected error setting node defs: %v", err)
		return
	}

	l, _ := g.listChildPaths(CfgNodeDefsKey(splitKey))
	// Expect the length to be 1 since each node's cfg will save only
	// the one whose UUID matches its UUID.
	if len(l) != 1 {
		t.Errorf("incorrect keys %v", l)
	}

	nodeDefs, cas3, err := CfgGetNodeDefs(g, splitKey)
	if err != nil {
		t.Errorf("unexpected error getting node defs: %v", err)
		return
	}

	nodeDefs.NodeDefs[uuid] = &NodeDef{
		HostPort:    "15",
		UUID:        uuid,
		ImplVersion: CfgGetVersion(g),
	}

	// Initially setting it with an older CAS.
	_, err = CfgSetNodeDefs(g, splitKey, nodeDefs, cas1)
	if err != nil {
		if _, ok := err.(*CfgCASError); ok {
			_, err = CfgSetNodeDefs(g, splitKey, nodeDefs, cas3)
			if err != nil {
				t.Errorf("unexpected error setting node defs: %v", err)
				return
			}
		} else {
			t.Errorf("unexpected error setting node defs: %v", err)
			return
		}
	}
}

// Tests setting and getting a simple key from cfg metakv.
func TestMetaKV(t *testing.T) {
	options := make(map[string]string)
	options["nsServerUrl"] = "http://127.0.0.0:9000"

	g, _ := NewCfgMetaKv(NewUUID(), options)
	defer g.Del("test", CFG_CAS_FORCE)

	// A zero cas is a "don't care", create-or-update set.
	casSet, err := g.Set("test", []byte("test2"), 0)
	if err != nil {
		t.Fatalf("error in setting simple key in metakv: %v", err)
	}

	val, casGet, err := g.Get("test", 0)
	if err != nil {
		t.Fatalf("error in getting simple key from metakv: %v", err)
	}
	if "test2" != string(val) {
		t.Errorf("wrong get value from metakv")
	}

	// metakv never reports the rev that a Set() landed on, so a cas
	// from a Set() cannot be CAS'ed on -- only a Get() mints one that
	// resolves back to a rev.
	if casGet == casSet {
		t.Errorf("expected the Get cas to differ from the Set cas: %x",
			casSet)
	}

	_, err = g.Set("test", []byte("test3"), casSet)
	if _, ok := err.(*CfgCASError); !ok {
		t.Errorf("expected a CfgCASError on a Set's cas, got: %v", err)
	}

	// Nor may a cas that was never handed out for this key at all;
	// this one is caught locally, before metakv is ever asked.
	_, err = g.Set("test", []byte("test3"), casGet+1)
	if _, ok := err.(*CfgCASError); !ok {
		t.Errorf("expected a CfgCASError on an unknown cas, got: %v", err)
	}

	val, _, err = g.Get("test", 0)
	if err != nil || "test2" != string(val) {
		t.Errorf("expected the rejected Set()'s to have left the entry"+
			" alone, val: %s, err: %v", val, err)
	}

	// ... whereas the cas that the Get() handed out must.
	if _, err = g.Set("test", []byte("test3"), casGet); err != nil {
		t.Errorf("unexpected error setting with the Get cas: %v", err)
	}

	// A missing entry has no cas to hand out.
	if val, casGet, err = g.Get("key-not-there", 0); err != nil ||
		val != nil || casGet != 0 {
		t.Errorf("expected no hit on key-not-there,"+
			" val: %s, cas: %x, err: %v", val, casGet, err)
	}
}

// Tests the CAS check that runs inside metakv rather than in the local
// rev bookkeeping: a cas that is still the current one for this
// CfgMetaKv, but whose rev another node has already moved past.
func TestMetaKVRevMismatch(t *testing.T) {
	options := make(map[string]string)
	options["nsServerUrl"] = "http://127.0.0.0:9000"

	g, _ := NewCfgMetaKv(NewUUID(), options)

	// A second CfgMetaKv stands in for another node: it mutates the
	// entry behind g's back, so the rev that g holds goes stale in
	// metakv without g's own bookkeeping noticing.
	other, _ := NewCfgMetaKv(NewUUID(), options)

	defer g.Del("test-rev", CFG_CAS_FORCE)

	if _, err := g.Set("test-rev", []byte("v1"), 0); err != nil {
		t.Fatalf("error in setting the key: %v", err)
	}

	// g mints a cas for the rev that the entry is at right now.
	val, cas, err := g.Get("test-rev", 0)
	if err != nil || "v1" != string(val) {
		t.Fatalf("error in getting the key, val: %s, err: %v", val, err)
	}
	if cas == 0 {
		t.Fatalf("expected a non-zero cas for an entry with a rev")
	}

	// The other node lands its update first, moving the rev on.
	if _, err = other.Set("test-rev", []byte("v2"), 0); err != nil {
		t.Fatalf("error in setting the key from the other node: %v", err)
	}

	// g's cas is still g's current one, so this gets past the local
	// check and is rejected by metakv itself.
	_, err = g.Set("test-rev", []byte("v3"), cas)
	if _, ok := err.(*CfgCASError); !ok {
		t.Errorf("expected a CfgCASError from metakv's rev check on Set,"+
			" got: %v", err)
	}

	// The losing Set() must not have clobbered the winner.
	if val, _, err = g.Get("test-rev", 0); err != nil || "v2" != string(val) {
		t.Errorf("expected the other node's update to have survived,"+
			" val: %s, err: %v", val, err)
	}

	// Del() is guarded by the same rev check.  Re-stale g's cas the
	// same way, as the Get() above has since refreshed it.
	_, cas, err = g.Get("test-rev", 0)
	if err != nil {
		t.Fatalf("error in re-getting the key: %v", err)
	}

	if _, err = other.Set("test-rev", []byte("v4"), 0); err != nil {
		t.Fatalf("error in re-setting the key from the other node: %v", err)
	}

	if err = g.Del("test-rev", cas); err != nil {
		if _, ok := err.(*CfgCASError); !ok {
			t.Errorf("expected a CfgCASError from metakv's rev check on"+
				" Del, got: %v", err)
		}
	} else {
		t.Errorf("expected the Del with a stale rev to be rejected")
	}

	if val, _, err = g.Get("test-rev", 0); err != nil || "v4" != string(val) {
		t.Errorf("expected the rejected Del to have left the entry alone,"+
			" val: %s, err: %v", val, err)
	}
}

func TestCfgSetNodeDefs(t *testing.T) {
	options := make(map[string]string)
	options["nsServerUrl"] = "http://127.0.0.0:9000"

	nodeUUID := NewUUID()
	g, _ := NewCfgMetaKv(nodeUUID, options)

	splitKeyTest(g, t, NODE_DEFS_KNOWN, nodeUUID)
	splitKeyTest(g, t, NODE_DEFS_WANTED, nodeUUID)
}

// ------------------------------------------------

// Disabled as metakv just spams with endless log messages of...
//
//	2015/08/21 22:17:39 metakv notifier failed \
//	   (Post /_metakv: Unable to initialize cbauth's revrpc: \
//	   Some cbauth environment variables are not set. \
//	   I.e.: (rpc-url: `', user: `', pwd: `'))
func disabled_TestCfgMetaKvIllConfigured(t *testing.T) {
	options := make(map[string]string)
	options["nsServerUrl"] = "http://127.0.0.0:9000"

	m, err := NewCfgMetaKv(NewUUID(), options)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	err = m.Load()
	if err != nil {
		t.Errorf("expected no load err")
	}

	err = m.Refresh()
	if err != nil {
		t.Errorf("expected no refresh err")
	}

	ech := make(chan CfgEvent, 100)
	err = m.Subscribe("hello", ech)
	if err != nil {
		t.Errorf("expected no subscribe err")
	}

	val, cas, err := m.Get("key-not-there", 0)
	if err != nil || val != nil || cas != 0 {
		t.Errorf("expected no err on get on key-not-there")
	}

	cas, err = m.Set("key", []byte("val"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected err on set because metakv not properly setup")
	}

	err = m.Del("key", 0)
	if err == nil {
		t.Errorf("expected err on del because metakv not properly setup")
	}
}

func TestCompatibilityVersion(t *testing.T) {
	v, _ := CompatibilityVersion("5.0.0")
	if v != 327680 {
		t.Errorf("version expected: %d, actual: %d", v, v)
	}

	v, _ = CompatibilityVersion("5.5.0")
	if v != 327685 {
		t.Errorf("version expected: %d, actual: %d", v, v)
	}

	v, _ = CompatibilityVersion("6.0.0")
	if v != 393216 {
		t.Errorf("version expected: %d, actual: %d", v, v)
	}

	v, _ = CompatibilityVersion("0.0")
	if v != 0 {
		t.Errorf("version expected: %d, actual: %d", v, v)
	}

	v, _ = CompatibilityVersion("")
	if v != 1 {
		t.Errorf("version expected: %d, actual: %d", v, v)
	}
}
