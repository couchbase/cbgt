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
	cas, _ := g.Set("test", []byte("test2"), 2)
	val, _, err := g.Get("test", cas)
	if err != nil {
		t.Errorf("error in setting simple key in metakv")
	}
	if "test2" != string(val) {
		t.Errorf("wrong get value from metakv")
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
