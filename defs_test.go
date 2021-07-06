//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestIndexDefs(t *testing.T) {
	d := NewIndexDefs("1.2.3")
	buf, _ := json.Marshal(d)
	d2 := &IndexDefs{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("Unmarshal IndexDefs err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetIndexDefs(cfg)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetIndexDefs on new cfg should be nil")
	}
	cas, err = CfgSetIndexDefs(cfg, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetIndexDefs create on new cfg")
	}
	cas1, err := CfgSetIndexDefs(cfg, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetIndexDefs(cfg, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetIndexDefs recreate")
	}
	d4, cas, err := CfgGetIndexDefs(cfg)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestNodeDefs(t *testing.T) {
	d := NewNodeDefs("1.2.3")
	buf, _ := json.Marshal(d)
	d2 := &NodeDefs{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalNodeDefs err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetNodeDefs on new cfg should be nil")
	}
	cas, err = CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetNodeDefs create on new cfg")
	}
	cas1, err := CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetNodeDefs recreate")
	}
	d4, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestPlanPIndexes(t *testing.T) {
	d := NewPlanPIndexes("1.2.3")
	buf, _ := json.Marshal(d)
	d2 := &PlanPIndexes{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalPlanPIndexes err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetPlanPIndexes on new cfg should be nil")
	}
	cas, err = CfgSetPlanPIndexes(cfg, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetPlanPIndexes create on new cfg")
	}
	cas1, err := CfgSetPlanPIndexes(cfg, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetPlanPIndexes(cfg, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetPlanPIndexes recreate")
	}
	d4, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestSamePlanPIndexes(t *testing.T) {
	a := NewPlanPIndexes("0.0.1")
	b := NewPlanPIndexes("0.0.1")
	c := NewPlanPIndexes("0.1.0")

	if !SamePlanPIndexes(nil, nil) {
		t.Errorf("expected same nil to nil")
	}
	if SamePlanPIndexes(a, nil) {
		t.Errorf("expected not same to nil")
	}
	if SamePlanPIndexes(nil, a) {
		t.Errorf("expected not same to nil")
	}
	if !SamePlanPIndexes(a, b) {
		t.Errorf("expected same, a: %v, b: %v", a, b)
	}
	if !SamePlanPIndexes(a, b) {
		t.Errorf("expected same, a: %v, b: %v", a, b)
	}
	if !SamePlanPIndexes(a, c) {
		t.Errorf("expected same, a: %v, c: %v", a, c)
	}
	if !SamePlanPIndexes(c, a) {
		t.Errorf("expected same, a: %v, c: %v", a, c)
	}

	a.PlanPIndexes["foo"] = &PlanPIndex{
		Name: "foo",
	}

	if SamePlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(a, c) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(c, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}

	if SubsetPlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if !SubsetPlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}

	b.PlanPIndexes["foo"] = &PlanPIndex{
		Name:      "foo",
		IndexName: "differnet-than-foo-in-a",
	}

	if SamePlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}

	ca := CopyPlanPIndexes(a, "v1")
	if !SamePlanPIndexes(a, ca) {
		t.Errorf("expected same, a: %v, ca: %v", a, ca)
	}

	cb := CopyPlanPIndexes(b, "v2")
	if !SamePlanPIndexes(b, cb) {
		t.Errorf("expected same, b: %v, cb: %v", b, cb)
	}
}

func TestSamePlanPIndex(t *testing.T) {
	ppi0 := &PlanPIndex{
		Name:             "0",
		UUID:             "x",
		IndexName:        "x",
		IndexUUID:        "x",
		IndexParams:      "x",
		SourceType:       "x",
		SourceName:       "x",
		SourceUUID:       "x",
		SourcePartitions: "x",
		Nodes:            make(map[string]*PlanPIndexNode),
	}
	ppi1 := &PlanPIndex{
		Name:             "1",
		UUID:             "x",
		IndexName:        "x",
		IndexUUID:        "x",
		IndexParams:      "x",
		SourceType:       "x",
		SourceName:       "x",
		SourceUUID:       "x",
		SourcePartitions: "x",
		Nodes:            make(map[string]*PlanPIndexNode),
	}

	if !SamePlanPIndex(ppi0, ppi0) {
		t.Errorf("expected SamePlanPindex to be true")
	}
	if SamePlanPIndex(ppi0, ppi1) {
		t.Errorf("expected SamePlanPindex to be false")
	}
	if SamePlanPIndex(ppi1, ppi0) {
		t.Errorf("expected SamePlanPindex to be false")
	}
}

func TestPIndexMatchesPlan(t *testing.T) {
	plan := &PlanPIndex{
		Name: "hi",
		UUID: "111",
	}
	px := &PIndex{
		Name: "hi",
		UUID: "222",
	}
	py := &PIndex{
		Name: "hello",
		UUID: "111",
	}
	if PIndexMatchesPlan(px, plan) == false {
		t.Errorf("expected pindex to match the plan")
	}
	if PIndexMatchesPlan(py, plan) == true {
		t.Errorf("expected pindex to not match the plan")
	}
}

func TestCfgGetHelpers(t *testing.T) {
	errCfg := &ErrorOnlyCfg{}

	if _, err := CheckVersion(errCfg, "my-version"); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetIndexDefs(errCfg); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetNodeDefs(errCfg, NODE_DEFS_KNOWN); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetPlanPIndexes(errCfg); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
}

func TestGetNodePlanParam(t *testing.T) {
	if GetNodePlanParam(nil, "", "", "") != nil {
		t.Errorf("expected nil to nil")
	}
	if GetNodePlanParam(map[string]map[string]*NodePlanParam{
		"nodeUUID0": {},
	}, "wrong-nodeUUID", "indexDefName", "planPIndexName") != nil {
		t.Errorf("expected nil")
	}
	if GetNodePlanParam(map[string]map[string]*NodePlanParam{
		"nodeUUID0": {},
	}, "nodeUUID0", "indexDefName", "planPIndexName") != nil {
		t.Errorf("expected nil")
	}
	if GetNodePlanParam(map[string]map[string]*NodePlanParam{
		"nodeUUID0": {
			"indexDefName": nil,
		},
	}, "nodeUUID0", "indexDefName", "planPIndexName") != nil {
		t.Errorf("expected nil")
	}
	if GetNodePlanParam(map[string]map[string]*NodePlanParam{
		"nodeUUID0": {
			"indexDefName": &NodePlanParam{},
		},
	}, "nodeUUID0", "indexDefName", "planPIndexName") == nil {
		t.Errorf("expected not nil")
	}
	if GetNodePlanParam(map[string]map[string]*NodePlanParam{
		"nodeUUID0": {
			"planPIndexName": &NodePlanParam{},
		},
	}, "nodeUUID0", "indexDefName", "planPIndexName") == nil {
		t.Errorf("expected not nil")
	}
}

func TestIndexDefJSON(t *testing.T) {
	id1 := IndexDef{}
	b, err := json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	var id2 IndexDef
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}

	id1.Params = `{"foo":"bar"}`
	id1.SourceParams = `{"hey":[0,1,2],"ho":"there"}`
	b, err = json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}

	id1.Params = `null`
	id1.SourceParams = `null`
	b, err = json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}

	id1.Params = ``
	id1.SourceParams = ``
	b, err = json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}
}

func TestPlanPIndexJSON(t *testing.T) {
	id1 := PlanPIndex{}
	b, err := json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	var id2 PlanPIndex
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}

	id1.IndexParams = `{"foo":"bar"}`
	id1.SourceParams = `{"hey":[0,1,2],"ho":"there"}`
	b, err = json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}

	id1.IndexParams = `null`
	id2.SourceParams = `null`
	b, err = json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}

	id1.IndexParams = ``
	id1.SourceParams = ``
	b, err = json.Marshal(id1)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	err = json.Unmarshal(b, &id2)
	if err != nil {
		t.Errorf("expected no err, got: %v", err)
	}
	if !reflect.DeepEqual(&id1, &id2) {
		t.Errorf("expected equal: %#v, versus: %#v", id1, id2)
	}
}

func TestMetaKvIsLeanFeatureSupportedSuccess(t *testing.T) {

	value := []byte(`{"uuid":"1530042671","nodeDefs":{"710948f76ea4f807dd4e41e44fe74c13":
		{"hostPort":"127.0.0.1:9202","uuid":"710948f76ea4f807dd4e41e44fe74c13",
		"implVersion":"5.0.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
		"container":"","weight":1,"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":` +
		`\"127.0.0.1:9002\",\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\": ` +
		`\"v0.5.0\"}"},"7879038ec4529cc4815f5d927c3df476":{"hostPort":"192.168.1.3:9200",
		"uuid":"7879038ec4529cc4815f5d927c3df476","implVersion":"5.0.0","tags":["feed",
		"janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
		"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":\"192.168.1.3:9000\", ` +
		`\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\":\"v0.5.0\"}"},
		"ecc1c3cad5a58523511e6ff2fd38f6be":{"hostPort":"127.0.0.1:9201",
		"uuid":"ecc1c3cad5a58523511e6ff2fd38f6be","implVersion":"5.0.0",
		"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"",
		"weight":1,"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":\"127.0.0.1:9001\",` +
		`\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\":\"v0.5.0\"}"}},
		"implVersion":"5.0.0"}`)

	nodeDefs := &NodeDefs{}
	err := json.Unmarshal(value, nodeDefs)
	if err != nil {
		t.Errorf("json parsing failed, err: %v", err)
	}

	if !IsFeatureSupportedByCluster(NodeFeatureLeanPlan, nodeDefs) {
		t.Errorf(" `leanPlan` feature support check should have passed")
	}
}

func TestMetaKvIsLeanFeatureSupportedFailure(t *testing.T) {

	value := []byte(`{"uuid":"1530042671","nodeDefs":{"710948f76ea4f807dd4e41e44fe74c13":
		{"hostPort":"127.0.0.1:9202","uuid":"710948f76ea4f807dd4e41e44fe74c13",
		"implVersion":"5.0.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
		"container":"","weight":1,"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":` +
		`\"127.0.0.1:9002\",\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\": ` +
		`\"v0.5.0\"}"},"7879038ec4529cc4815f5d927c3df476":{"hostPort":"192.168.1.3:9200",
		"uuid":"7879038ec4529cc4815f5d927c3df476","implVersion":"5.0.0","tags":["feed",
		"janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
		"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":\"192.168.1.3:9000\", ` +
		`\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\":\"v0.5.0\"}"},
		"ecc1c3cad5a58523511e6ff2fd38f6be":{"hostPort":"127.0.0.1:9201",
		"uuid":"ecc1c3cad5a58523511e6ff2fd38f6be","implVersion":"5.0.0",
		"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"",
		"weight":1,"extras":"{\"features\":\"leanPlanNext\",\"nsHostPort\":\"127.0.0.1:9001\",` +
		`\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\":\"v0.5.0\"}"}},
		"implVersion":"5.0.0"}`)

	nodeDefs := &NodeDefs{}
	err := json.Unmarshal(value, nodeDefs)
	if err != nil {
		t.Errorf("json parsing failed, err: %v", err)
	}

	if IsFeatureSupportedByCluster(NodeFeatureLeanPlan, nodeDefs) {
		t.Errorf(" `leanPlan` feature support check should have failed")
	}

	// no nodeDefs given, then expect no feature support
	value = []byte(`{"uuid":"1530042671","nodeDefs":{},"implVersion":"5.0.0"}`)

	nodeDefs = &NodeDefs{}
	err = json.Unmarshal(value, nodeDefs)
	if err != nil {
		t.Errorf("json parsing failed, err: %v", err)
	}

	if IsFeatureSupportedByCluster(NodeFeatureLeanPlan, nodeDefs) {
		t.Errorf(" `leanPlan` feature support check should have failed")
	}
}
