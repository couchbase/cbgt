//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

// Implements ManagerEventHandlers interface.
type TestMEH struct {
	lastPIndex *PIndex
	lastCall   string
	ch         chan bool
}

func (meh *TestMEH) OnRegisterPIndex(pindex *PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnRegisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func (meh *TestMEH) OnUnregisterPIndex(pindex *PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnUnregisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func (meh *TestMEH) OnFeedError(srcType string, r Feed,
	err error) {
}

func TestPIndexPath(t *testing.T) {
	m := NewManager(VERSION, nil, NewUUID(), nil,
		"", 1, "", "", "dir", "svr", nil)
	p := m.PIndexPath("x")
	expected :=
		"dir" + string(os.PathSeparator) + "x" + pindexPathSuffix
	if p != expected {
		t.Errorf("wrong pindex path %s, %s", p, expected)
	}
	n, ok := m.ParsePIndexPath(p)
	if !ok || n != "x" {
		t.Errorf("parse pindex path not ok, %v, %v", n, ok)
	}
	n, ok =
		m.ParsePIndexPath("totally not a pindex path" + pindexPathSuffix)
	if ok {
		t.Errorf("expected not-ok on bad pindex path")
	}
	if n != "" {
		t.Errorf("expected empty string on bad pindex path")
	}
	n, ok =
		m.ParsePIndexPath("dir" + string(os.PathSeparator) + "not-a-pindex")
	if ok {
		t.Errorf("expected not-ok on bad pindex path")
	}
	if n != "" {
		t.Errorf("expected empty string on bad pindex path")
	}
}

func TestManagerStart(t *testing.T) {
	m := NewManager(VERSION, nil, NewUUID(), nil,
		"", 1, "", "", "dir", "not-a-real-svr", nil)
	if m.Start("") == nil {
		t.Errorf("expected NewManager() with bad svr should fail")
	}
	if m.DataDir() != "dir" {
		t.Errorf("wrong data dir")
	}
	m.Stop()

	m = NewManager(VERSION, nil, NewUUID(), nil,
		"", 1, "", "", "not-a-real-dir", "", nil)
	if m.Start("") == nil {
		t.Errorf("expected NewManager() with bad dir should fail")
	}
	if m.DataDir() != "not-a-real-dir" {
		t.Errorf("wrong data dir")
	}
	m.Stop()

	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m = NewManager(VERSION, nil, NewUUID(), nil,
		"", 1, "", "", emptyDir, "", nil)
	if err := m.Start(""); err != nil {
		t.Errorf("expected NewManager() with empty dir to work,"+
			" err: %v", err)
	}

	cfg := NewCfgMem()
	m = NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start("known"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas != 0 || nd != nil {
		t.Errorf("expected no node defs wanted")
	}
	m.Stop()

	cfg = NewCfgMem()
	m = NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err = m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	m.Stop()
}

func TestManagerRestart(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()

	pit, err := PIndexImplTypeForIndex(cfg, "foo")
	if err == nil || pit != nil {
		t.Error("expected pit for uncreated foo index")
	}

	m := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err = m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", PlanParams{},
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err on create-with-prevIndexUUID")
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", PlanParams{}, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", PlanParams{},
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err update wrong prevIndexUUID")
	}
	m.Kick("test0")
	m.PlannerNOOP("test0")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected 1 feed, 1 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	for _, pindex := range pindexes {
		pindex.Dest.Close()
		if m.GetPIndex(pindex.Name) != pindex {
			t.Errorf("expected GetPIndex() to match")
		}
	}

	m2 := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	m2.uuid = m.uuid
	if err = m2.Start("wanted"); err != nil {
		t.Errorf("expected reload Manager.Start() to work, err: %v", err)
	}
	m2.Kick("test2")
	m2.PlannerNOOP("test2")
	attempts := 0
	for {
		feeds, pindexes = m2.CurrentMaps()
		if len(feeds) == 1 && len(pindexes) == 1 {
			break
		}
		attempts++
		if attempts > 10 {
			t.Errorf("expected load 1 feed, 1 pindex, got: %+v, %+v",
				feeds, pindexes)
		}
		time.Sleep(100 * time.Millisecond)
	}

	pit, err = PIndexImplTypeForIndex(cfg, "foo")
	if err != nil || pit == nil {
		t.Error("expected pit for foo index")
	}
	pit, err = PIndexImplTypeForIndex(cfg, "bogus")
	if err == nil || pit != nil {
		t.Error("expected no pit for bogus index")
	}
}

func TestManagerCreateDeleteIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	m := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", PlanParams{}, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", PlanParams{}, ""); err == nil {
		t.Errorf("expected re-CreateIndex() to fail")
	}
	if err := m.DeleteIndex("not-an-actual-index-name"); err == nil {
		t.Errorf("expected bad DeleteIndex() to fail")
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to be 1 feed and 1 pindex,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}

	if err := m.DeleteIndex("foo"); err != nil {
		t.Errorf("expected DeleteIndex() to work on actual index,"+
			" err: %v", err)
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("expected to be 0 feed and 0 pindex,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
}

func TestManagerDeleteAllIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	m := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo1", "", PlanParams{}, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo2", "", PlanParams{}, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("primary", "default", "1234", sourceParams,
		"blackhole", "foo3", "", PlanParams{}, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 3 || len(pindexes) != 3 {
		t.Errorf("expected to be 3 feeds and 3 pindexs,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	m.DeleteAllIndexFromSource("primary", "default", "123")
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to be 1 feed and 1 pindex,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
}

func TestManagerRegisterPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "blackhole",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID",
		sourceParams, "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("expected NewPIndex() to work")
	}
	px := m.unregisterPIndex(p.Name, nil)
	if px != nil {
		t.Errorf("expected unregisterPIndex() on newborn manager to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err = m.registerPIndex(p)
	if err != nil {
		t.Errorf("expected first registerPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnRegisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	err = m.registerPIndex(p)
	if err == nil {
		t.Errorf("expected second registerPIndex() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	pc, ok := pindexes[p.Name]
	if !ok || p != pc {
		t.Errorf("wrong pindex in current pindexes")
	}

	px = m.unregisterPIndex(p.Name, pc)
	if px == nil {
		t.Errorf("expected first unregisterPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	px = m.unregisterPIndex(p.Name, nil)
	if px != nil {
		t.Errorf("expected second unregisterPIndex() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
}

func TestManagerRegisterFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(),
		nil, "", 1, "", "", emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	f := &ErrorOnlyFeed{name: "f0"}
	fx := m.unregisterFeed(f.Name())
	if fx != nil {
		t.Errorf("expected unregisterFeed() on newborn manager to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err := m.registerFeed(f)
	if err != nil {
		t.Errorf("expected first registerFeed() to work")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err = m.registerFeed(f)
	if err == nil {
		t.Errorf("expected second registerFeed() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 1 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	fc, ok := feeds[f.Name()]
	if !ok || f != fc {
		t.Errorf("wrong feed in current feeds")
	}

	fx = m.unregisterFeed(f.Name())
	if fx == nil {
		t.Errorf("expected first unregisterFeed() to work")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	fx = m.unregisterFeed(f.Name())
	if fx != nil {
		t.Errorf("expected second unregisterFeed() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
}

func TestManagerStartDCPFeed(t *testing.T) {
	testManagerStartDCPFeed(t, source_gocouchbase)
	testManagerStartDCPFeed(t, source_gocouchbase_dcp)
}

func testManagerStartDCPFeed(t *testing.T, sourceType string) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	err := mgr.startFeedByType("feedName",
		"indexName", "indexUUID", sourceType,
		"sourceName", "sourceUUID", sourceParams, nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for sourceType: %s, err: %v",
			sourceType, err)
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*DCPFeed); !ok {
		t.Errorf("expected a DCPFeed")
	}
	err = mgr.startFeedByType("feedName",
		"indexName", "indexUUID", sourceType,
		"sourceName", "sourceUUID", sourceParams, nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
	err = mgr.startFeedByType("feedName2",
		"indexName2", "indexUUID2", sourceType,
		"sourceName2", "sourceUUID2", "NOT-VALID-JSON-sourceParams", nil)
	if err == nil {
		t.Errorf("expected startFeedByType fail on non-json sourceParams")
	}
}

func TestManagerStartTAPFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	err := mgr.startFeedByType("feedName",
		"indexName", "indexUUID", source_gocouchbase_tap,
		"sourceName", "sourceUUID", sourceParams, nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for simple sourceType")
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*TAPFeed); !ok {
		t.Errorf("expected a TAPFeed")
	}
	err = mgr.startFeedByType("feedName",
		"indexName", "indexUUID", source_gocouchbase_tap,
		"sourceName", "sourceUUID", sourceParams, nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
	err = mgr.startFeedByType("feedName2",
		"indexName2", "indexUUID2", source_gocouchbase_tap,
		"sourceName2", "sourceUUID2", "NOT-VALID-JSON-sourceParams", nil)
	if err == nil {
		t.Errorf("expected startFeedByType fail on non-json sourceParams")
	}
}

func TestManagerStartNILFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	err := mgr.startFeedByType("feedName",
		"indexName", "indexUUID", "nil",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for nil sourceType")
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*NILFeed); !ok {
		t.Errorf("expected a NILFeed")
	}
	err = mgr.startFeedByType("feedName",
		"indexName", "indexUUID", "nil",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
}

func TestManagerStartSimpleFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	err := mgr.startFeedByType("feedName",
		"indexName", "indexUUID", "primary",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for simple sourceType")
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*PrimaryFeed); !ok {
		t.Errorf("expected a SimpleFeed")
	}
	err = mgr.startFeedByType("feedName",
		"indexName", "indexUUID", "primary",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
}

func TestManagerTags(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	tm := mgr.tagsMap
	if tm != nil {
		t.Errorf("expected nil Tags()")
	}

	mgr = NewManager(VERSION, cfg, NewUUID(), []string{"a", "b"}, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	tm = mgr.tagsMap
	te := map[string]bool{}
	te["a"] = true
	te["b"] = true
	if tm == nil || !reflect.DeepEqual(tm, te) {
		t.Errorf("expected equal Tags()")
	}
}

func TestManagerClosePIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(),
		nil, "", 1, "", "", emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}
	m.Start("wanted")
	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "blackhole",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID",
		sourceParams, "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("error creating pindex: %v", err)
	}
	m.registerPIndex(p)
	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	err = m.ClosePIndex(p)
	if err != nil {
		t.Errorf("expected ClosePIndex() to work")
	}
	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("meh callbacks were wrong")
	}
}

func TestManagerRemovePIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "", emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}
	m.Start("wanted")
	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "blackhole",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID",
		sourceParams, "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("error creating pindex: %v", err)
	}
	m.registerPIndex(p)
	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	err = m.RemovePIndex(p)
	if err != nil {
		t.Errorf("expected RemovePIndex() to work")
	}
	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("meh callbacks were wrong")
	}
}

func TestManagerStrangeWorkReqs(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	meh := &TestMEH{}
	m := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", meh)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary",
		"sourceName", "sourceUUID", sourceParams,
		"blackhole", "foo", "", PlanParams{}, ""); err != nil {
		t.Errorf("expected simple CreateIndex() to work")
	}
	if err := syncWorkReq(m.plannerCh, "whoa-this-isn't-a-valid-op",
		"test", nil); err == nil {
		t.Errorf("expected error on weird work req to planner")
	}
	if err := syncWorkReq(m.janitorCh, "whoa-this-isn't-a-valid-op",
		"test", nil); err == nil {
		t.Errorf("expected error on weird work req to janitor")
	}
}

func TestManagerStartFeedByType(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		emptyDir, "", nil)
	err := m.startFeedByType("feedName", "indexName", "indexUUID",
		"sourceType-is-unknown",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err == nil {
		t.Errorf("expected err on unknown source type")
	}
}

func TestManagerStartPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		emptyDir, "", nil)
	err := m.startPIndex(&PlanPIndex{IndexType: "unknown-index-type"})
	if err == nil {
		t.Errorf("expected err on unknown index type")
	}
	err = m.startPIndex(&PlanPIndex{
		IndexType: "blackhole",
		IndexName: "a",
	})
	if err != nil {
		t.Errorf("expected new blackhole pindex to work, err: %v", err)
	}
}

func TestManagerReStartPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", 1, "", "",
		emptyDir, "", meh)

	err := m.startPIndex(&PlanPIndex{
		Name:      "p",
		IndexType: "blackhole",
		IndexName: "i",
	})
	if err != nil {
		t.Errorf("expected first start to work")
	}
	err = m.stopPIndex(meh.lastPIndex, true)
	if err != nil {
		t.Errorf("expected close pindex to work")
	}
	err = m.startPIndex(&PlanPIndex{
		Name:      "p",
		IndexType: "blackhole",
		IndexName: "i"})
	if err != nil {
		t.Errorf("expected close+restart pindex to work")
	}
}

func testManagerSimpleFeed(t *testing.T,
	sourceParams string, planParams PlanParams,
	andThen func(*Manager, *PrimaryFeed, *TestMEH)) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	meh := &TestMEH{}
	m := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", meh)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if err := m.CreateIndex("primary",
		"sourceName", "sourceUUID", sourceParams,
		"blackhole", "foo", "", planParams, ""); err != nil {
		t.Errorf("expected simple CreateIndex() to work")
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected 1 feed, 1 pindex,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if meh.lastPIndex == nil {
		t.Errorf("expected to be meh.lastPIndex")
	}
	feedName := FeedNameForPIndex(meh.lastPIndex, "")
	feed, exists := feeds[feedName]
	if !exists || feed == nil {
		t.Errorf("expected there to be feed: %s", feedName)
	}
	sf, ok := feed.(*PrimaryFeed)
	if !ok || sf == nil {
		t.Errorf("expected feed to be simple")
	}
	if sf.Dests() == nil {
		t.Errorf("expected simple feed dests to be there")
	}
	andThen(m, sf, meh)
}

func TestManagerCreateSimpleFeed(t *testing.T) {
	sourceParams := ""
	testManagerSimpleFeed(t, sourceParams, PlanParams{},
		func(mgr *Manager, sf *PrimaryFeed, meh *TestMEH) {
			err := sf.Close()
			if err != nil {
				t.Errorf("expected simple feed close to work")
			}
		})
}

func TestRemoveNodeDef(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	m := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsWanted")
	}

	err = m.RemoveNodeDef(NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected no error on RemoveNodeDef WANTED")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}

	err = m.RemoveNodeDef(NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected no error on RemoveNodeDef WANTED")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}

	err = m.RemoveNodeDef(NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected no error on RemoveNodeDef WANTED")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}

	err = m.RemoveNodeDef(NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected no error on RemoveNodeDef WANTED")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}
}

func TestRegisterUnwanted(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	uuid := NewUUID()
	cfg := NewCfgMem()
	m := NewManager(VERSION, cfg, uuid, nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsWanted")
	}

	m1 := NewManager(VERSION, cfg, uuid, nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err = m1.Start("unchanged"); err != nil {
		t.Errorf("expected Manager.Start(unchanged) to work, err: %v", err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsWanted")
	}

	m2 := NewManager(VERSION, cfg, uuid, nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err = m2.Start("unwanted"); err != nil {
		t.Errorf("expected Manager.Start(unwanted) to work, err: %v", err)
	}
	if err != nil {
		t.Errorf("expected no error on RemoveNodeDef WANTED")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}

	m3 := NewManager(VERSION, cfg, uuid, nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	if err = m3.Start("unknown"); err != nil {
		t.Errorf("expected Manager.Start(unknown) to work, err: %v", err)
	}
	if err != nil {
		t.Errorf("expected no error on RemoveNodeDef WANTED")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}
}

func TestUnregisterNodes(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	uuid := NewUUID()

	cfg := NewCfgMem()

	err := UnregisterNodes(cfg, VERSION, []string{uuid})
	if err != nil {
		t.Errorf("expected no err when removing unknown uuid")
	}

	m := NewManager(VERSION, cfg, uuid, nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	err = m.Start("wanted")
	if err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 1 || nd.NodeDefs[m.uuid] == nil {
		t.Errorf("expected mgr to be in nodeDefsWanted")
	}

	if len(m.Options()) != 0 {
		t.Errorf("expected no options")
	}

	err = UnregisterNodes(cfg, VERSION, []string{uuid})
	if err != nil {
		t.Errorf("expected no err")
	}

	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected no mgr to be in nodeDefsKnown")
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 0 || nd.NodeDefs[m.uuid] != nil {
		t.Errorf("expected mgr to not be in nodeDefsWanted")
	}

	err = UnregisterNodes(cfg, VERSION, []string{uuid})
	if err != nil {
		t.Errorf("expected no err when removing already removed uuid")
	}
}

func TestManagerRestartPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(),
		nil, "", 1, "", "", emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}
	m.Start("wanted")
	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "blackhole",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID",
		sourceParams, "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("error creating pindex: %v", err)
	}
	m.registerPIndex(p)
	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	req := &pindexRestartReq{pindex: p, planPIndexName: p.Name + "_temp"}
	err = m.restartPIndex(req)
	if err != nil {
		t.Errorf("expected rebootPIndex() to work")
	}
	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != pindexes[p.Name+"_temp"] || meh.lastCall != "OnRegisterPIndex" {
		t.Errorf("meh callbacks were wrong")
	}
	if m.stats.TotJanitorRestartPIndex != 1 {
		t.Errorf("janitor should have restarted the pindex once")
	}
}

func TestManagerMultipleServers(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	serverStr := "localhost:1000;localhost:1001;localhost:1002"
	m := NewManager(VERSION, nil, NewUUID(),
		nil, "", 1, "", "", emptyDir, serverStr, meh)
	err := m.Start("wanted")
	if err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if m.Server() != serverStr {
		t.Errorf("expected Manager.Server() to return %v, but got %v",
			serverStr, m.Server())
	}
}

func TestManagerPIndexRestartWithFeedAllotmentOptionChange(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	pit, err := PIndexImplTypeForIndex(cfg, "foo")
	if err == nil || pit != nil {
		t.Error("expected pit for uncreated foo index")
	}

	options := map[string]string{"feedAllotment": FeedAllotmentOnePerPIndex}
	m := NewManagerEx(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil, options)
	if err = m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := "{\"numPartitions\":6}"
	planParams := PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams,
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err on create-with-prevIndexUUID")
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams,
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err update wrong prevIndexUUID")
	}

	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}

	// update the feedAllotment to "oneFeedPerIndex" for `foo`
	// and expect only 1 feed
	sourceParams = "{\"numPartitions\":6, \"feedAllotment\":\"oneFeedPerIndex\"}"
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 6 {
		t.Errorf("expected 1 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 6 {
		t.Errorf("expected pindex restarted count to be 6, but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}
	err = m.DeleteIndex("foo")
	if err != nil {
		t.Errorf("expected index: foo to get deleted, err: %+v", err)
	}

	// other indexes should still fallback to the manager default of
	// oneFeedPerPIndex
	sourceParams = "{\"numPartitions\":6}"
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo_second", "", planParams, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex for index: foo_second,"+
			"feeds: %+v, pindexes: %+v", feeds, pindexes)
	}
	err = m.DeleteIndex("foo_second")
	if err != nil {
		t.Errorf("expected index: foo_second to get deleted, err: %+v", err)
	}

	// create another index, update feedAllotment back and forth
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo_third", "", planParams, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}

	// update the feedAllotment to "oneFeedPerIndex" for `foo`
	// and expect only 1 feed
	sourceParams = "{\"numPartitions\":6, \"feedAllotment\":\"oneFeedPerIndex\"}"
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo_third", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 6 {
		t.Errorf("expected 1 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 12 {
		t.Errorf("expected total pindex restarted count to be 12, but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	// set back to the manager default of oneFeedPerPIndex
	sourceParams = "{\"numPartitions\":6, \"feedAllotment\":\"oneFeedPerPIndex\"}"
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo_third", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex for index: foo_second,"+
			"feeds: %+v, pindexes: %+v", feeds, pindexes)
	}

}

func TestManagerPIndexRestartWithReplicaCountChange(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	pit, err := PIndexImplTypeForIndex(cfg, "foo")
	if err == nil || pit != nil {
		t.Error("expected pit for uncreated foo index")
	}

	options1 := map[string]string{
		"feedAllotment":      FeedAllotmentOnePerPIndex,
		"maxReplicasAllowed": "10",
	}
	m := NewManagerEx(VERSION, cfg, NewUUID(), nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil, options1)
	if err = m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := "{\"numPartitions\":6}"
	planParams := PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams,
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err on create-with-prevIndexUUID")
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams,
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err update wrong prevIndexUUID")
	}

	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}

	// update the replicaCount to "1"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 1,
		NumReplicas:            1,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 6 {
		t.Errorf("expected pindex restarted count to be 6, but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	// update the replicaCount to "0"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 1,
		NumReplicas:            0,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 12 {
		t.Errorf("expected pindex restarted count to be 12 but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	// update the replicaCount to "2"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 1,
		NumReplicas:            2,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 18 {
		t.Errorf("expected pindex restarted count to be 18 but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	// update the sourceParams and ensure that pindexes aren't restarting
	indexParams := "{\"store\": {\"indexType\": \"scorch\"}}"
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", indexParams, planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 18 {
		t.Errorf("expected pindex restarted count to be 18 but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	// disable the index restart over manager options
	options2 := map[string]string{
		"feedAllotment":          FeedAllotmentOnePerPIndex,
		"maxReplicasAllowed":     "10",
		"rebuildOnReplicaUpdate": "true",
	}
	m.SetOptions(options2)

	// update the replicaCount to "1"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 1,
		NumReplicas:            1,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 18 {
		t.Errorf("expected pindex restarted count to be 18 but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	// enable the index restart over manager options
	options3 := map[string]string{
		"feedAllotment":          FeedAllotmentOnePerPIndex,
		"maxReplicasAllowed":     "10",
		"rebuildOnReplicaUpdate": "false",
	}
	m.SetOptions(options3)

	// update the replicaCount to "0"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 1,
		NumReplicas:            0,
	}
	if err = m.CreateIndex("primary", "default", "123", sourceParams,
		"blackhole", "foo", "", planParams, "*"); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 6 || len(pindexes) != 6 {
		t.Errorf("expected 6 feed, 6 pindex, feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if m.stats.TotJanitorRestartPIndex != 24 {
		t.Errorf("expected pindex restarted count to be 24 but got: %d",
			m.stats.TotJanitorRestartPIndex)
	}

	err = m.DeleteIndex("foo")
	if err != nil {
		t.Errorf("expected index: foo to get deleted, err: %+v", err)
	}

}
