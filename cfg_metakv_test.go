//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build metakv_test

package cbgt

import (
	"encoding/json"
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

func splitKeyTest(g *CfgMetaKv, t *testing.T, splitKey string) {
	c := &NodeDefs{
		UUID:        "abcd",
		NodeDefs:    make(map[string]*NodeDef),
		ImplVersion: "111",
	}
	c.NodeDefs["1"] = &NodeDef{
		HostPort:    "12",
		UUID:        "111",
		ImplVersion: "2",
	}
	c.NodeDefs["2"] = &NodeDef{
		HostPort:    "13",
		UUID:        "111",
		ImplVersion: "2",
	}
	c.NodeDefs["3"] = &NodeDef{
		HostPort:    "14",
		UUID:        "111",
		ImplVersion: "2",
	}
	val, _ := json.Marshal(c)
	cas, err := g.Set(splitKey, val, 0)
	if err != nil {
		t.Errorf("error in setting nodedefs-wanted key to metakv")
	}
	//check if splitting happend.so take the keys directly from metakv.
	l, _ := g.listChildPaths(splitKey)
	if len(l) != 3 {
		t.Errorf("incorrect keys %v", l)
	}
	val, cas2, err := g.Get(splitKey, cas)
	if err != nil {
		t.Errorf("error in getting nodedefs-wanted key")
	}
	k := &NodeDefs{}
	json.Unmarshal(val, k)
	if !compareNodeDefs(k, c) {
		t.Errorf("set and get key for nodeDefs are different")
	}
	d := &NodeDefs{
		UUID:        "abcd1",
		NodeDefs:    make(map[string]*NodeDef),
		ImplVersion: "222",
	}
	d.NodeDefs["4"] = &NodeDef{
		HostPort:    "12",
		UUID:        "111",
		ImplVersion: "2",
	}
	val, _ = json.Marshal(d)
	cas3, err := g.Set(splitKey, val, cas2)
	if err != nil {
		t.Errorf("error in setting nodedefs-wanted key to metakv")
	}
	l, _ = g.listChildPaths(splitKey)
	if len(l) != 4 {
		t.Errorf("incorrect keys %v", l)
	}
	val, _, err = g.Get(splitKey, cas3)
	if err != nil {
		t.Errorf("error in setting key")
	}
	k = &NodeDefs{}
	json.Unmarshal(val, k)
	c.NodeDefs["4"] = d.NodeDefs["4"]
	if !compareNodeDefs(k, c) {
		t.Errorf("set and get key for nodeDefs are different")
	}
}

func TestMetaKV(t *testing.T) {
	g, _ := NewCfgMetaKv()
	cas, _ := g.Set("test", []byte("test2"), 2)
	val, _, err := g.Get("test", cas)
	if err != nil {
		t.Errorf("error in setting simple key in metakv")
	}
	if "test2" != string(val) {
		t.Errorf("wrong get value from metakv")
	}
	splitKeyTest(g, t, CfgNodeDefsKey(NODE_DEFS_KNOWN))
	splitKeyTest(g, t, CfgNodeDefsKey(NODE_DEFS_WANTED))
}

// ------------------------------------------------

// Disabled as metakv just spams with endless log messages of...
//
//    2015/08/21 22:17:39 metakv notifier failed \
//       (Post /_metakv: Unable to initialize cbauth's revrpc: \
//       Some cbauth environment variables are not set. \
//       I.e.: (rpc-url: `', user: `', pwd: `'))
//
func disabled_TestCfgMetaKvIllConfigured(t *testing.T) {
	m, err := NewCfgMetaKv()
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
