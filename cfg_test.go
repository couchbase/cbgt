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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

type ErrorOnlyCfg struct{}

func (c *ErrorOnlyCfg) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	return nil, 0, fmt.Errorf("error only")
}

func (c *ErrorOnlyCfg) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	return 0, fmt.Errorf("error only")
}

func (c *ErrorOnlyCfg) Del(key string, cas uint64) error {
	return fmt.Errorf("error only")
}

func (c *ErrorOnlyCfg) Subscribe(key string, ch chan CfgEvent) error {
	return fmt.Errorf("error only")
}

func (c *ErrorOnlyCfg) Refresh() error {
	return fmt.Errorf("error only")
}

// ------------------------------------------------

type ErrorAfterCfg struct {
	inner    Cfg
	errAfter int
	numOps   int
}

func (c *ErrorAfterCfg) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.numOps++
	if c.numOps > c.errAfter {
		return nil, 0, fmt.Errorf("error after")
	}
	return c.inner.Get(key, cas)
}

func (c *ErrorAfterCfg) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.numOps++
	if c.numOps > c.errAfter {
		return 0, fmt.Errorf("error after")
	}
	return c.inner.Set(key, val, cas)
}

func (c *ErrorAfterCfg) Del(key string, cas uint64) error {
	c.numOps++
	if c.numOps > c.errAfter {
		return fmt.Errorf("error after")
	}
	return c.inner.Del(key, cas)
}

func (c *ErrorAfterCfg) Subscribe(key string, ch chan CfgEvent) error {
	c.numOps++
	if c.numOps > c.errAfter {
		return fmt.Errorf("error after")
	}
	return c.inner.Subscribe(key, ch)
}

func (c *ErrorAfterCfg) Refresh() error {
	c.numOps++
	if c.numOps > c.errAfter {
		return fmt.Errorf("error after")
	}
	return c.inner.Refresh()
}

// ------------------------------------------------

func TestCfgMem(t *testing.T) {
	testCfg(t, NewCfgMem())
}

func TestCfgSimple(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgSimple(emptyDir + string(os.PathSeparator) + "test.cfg")
	testCfg(t, cfg)
}

func testCfg(t *testing.T, c Cfg) {
	v, cas, err := c.Get("nope", 0)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to miss on brand new CfgMem")
	}
	v, cas, err = c.Get("nope", 100)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to miss on brand new CfgMem with wrong CAS")
	}
	cas, err = c.Set("a", []byte("A"), 100)
	if err == nil || cas != 0 {
		t.Errorf("expected creation Set() to fail when no entry and wrong CAS")
	}

	cas1, err := c.Set("a", []byte("A"), 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected creation Set() to ok CAS 0")
	}
	cas, err = c.Set("a", []byte("A"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected re-creation Set() to fail with CAS 0")
	}
	cas, err = c.Set("a", []byte("A"), 100)
	if err == nil || cas != 0 {
		t.Errorf("expected update Set() to fail when entry and wrong CAS")
	}
	v, cas, err = c.Get("a", 100)
	if err == nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to fail on wrong CAS")
	}
	v, cas, err = c.Get("a", 0)
	if err != nil || string(v) != "A" || cas != cas1 {
		t.Errorf("expected Get() to succeed on 0 CAS")
	}
	v, cas, err = c.Get("a", cas1)
	if err != nil || string(v) != "A" || cas != cas1 {
		t.Errorf("expected Get() to succeed on right CAS")
	}

	cas2, err := c.Set("a", []byte("AA"), cas1)
	if err != nil || cas2 != 2 {
		t.Errorf("expected update Set() to succeed when right CAS")
	}
	cas, err = c.Set("a", []byte("AA-should-fail"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected re-creation Set() to fail with CAS 0")
	}
	cas, err = c.Set("a", []byte("AA"), cas1)
	if err == nil || cas != 0 {
		t.Errorf("expected update Set() to fail when retried after success")
	}
	v, cas, err = c.Get("a", 100)
	if err == nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to fail on wrong CAS")
	}
	v, cas, err = c.Get("a", 0)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on 0 CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on right CAS")
	}

	err = c.Del("nope", 0)
	if err != nil {
		t.Errorf("expected Del() to succeed on missing item when 0 CAS")
	}
	err = c.Del("nope", 100)
	if err == nil {
		t.Errorf("expected Del() to fail on missing item when non-zero CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on right CAS")
	}
	err = c.Del("a", 100)
	if err == nil {
		t.Errorf("expected Del() to fail when wrong CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on right CAS")
	}
	err = c.Del("a", cas2)
	if err != nil {
		t.Errorf("expected Del() to succeed when right CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() with CAS to miss after Del(): "+
			" %v, %v, %v", err, v, cas)
	}
	v, cas, err = c.Get("a", 0)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() with 0 CAS to miss after Del(): "+
			" %v, %v, %v", err, v, cas)
	}
}

func TestCfgCASError(t *testing.T) {
	err := &CfgCASError{}
	if err.Error() != "CAS mismatch" {
		t.Errorf("expected error string wasn't right")
	}
}

func TestCfgSimpleLoad(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	c := NewCfgSimple(emptyDir + string(os.PathSeparator) + "not-a-file.cfg")
	if err := c.Load(); err == nil {
		t.Errorf("expected Load() to fail on bogus file")
	}

	path := emptyDir + string(os.PathSeparator) + "test.cfg"

	c1 := NewCfgSimple(path)
	cas1, err := c1.Set("a", []byte("A"), 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected Set() on initial cfg simple")
	}

	c2 := NewCfgSimple(path)
	if err := c2.Load(); err != nil {
		t.Errorf("expected Load() to work")
	}
	v, cas, err := c2.Get("a", 0)
	if err != nil || v == nil || cas != cas1 {
		t.Errorf("expected Get() to succeed")
	}
	if string(v) != "A" {
		t.Errorf("exepcted to read what we wrote")
	}

	badPath := emptyDir + string(os.PathSeparator) + "bad.cfg"
	ioutil.WriteFile(badPath, []byte("}hey this is bad json :-{"), 0600)
	c3 := NewCfgSimple(badPath)
	if err = c3.Load(); err == nil {
		t.Errorf("expected Load() to fail on bad json file")
	}
}

func TestCfgSimpleSave(t *testing.T) {
	path := "totally/not/a/dir/test.cfg"
	c1 := NewCfgSimple(path)
	cas, err := c1.Set("a", []byte("A"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected Save() to bad dir to fail")
	}
}

func TestCfgSimpleSubscribe(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	path := emptyDir + string(os.PathSeparator) + "test.cfg"

	ec := make(chan CfgEvent, 1)
	ec2 := make(chan CfgEvent, 1)

	c := NewCfgSimple(path)
	c.Subscribe("a", ec)
	c.Subscribe("aaa", ec2)

	cas1, err := c.Set("a", []byte("A"), 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected Set() on initial cfg simple")
	}
	runtime.Gosched()
	e := <-ec
	if e.Key != "a" || e.CAS != 1 {
		t.Errorf("expected event on Set()")
	}
	select {
	case <-ec2:
		t.Errorf("expected no events for ec2")
	default:
	}

	cas2, err := c.Set("a", []byte("AA"), cas1)
	if err != nil || cas2 != 2 {
		t.Errorf("expected Set() on initial cfg simple")
	}
	runtime.Gosched()
	e = <-ec
	if e.Key != "a" || e.CAS != 2 {
		t.Errorf("expected event on Set()")
	}
	select {
	case <-ec2:
		t.Errorf("expected no events for ec2")
	default:
	}

	err = c.Del("a", cas2)
	if err != nil {
		t.Errorf("expected Del() to work")
	}
	runtime.Gosched()
	e = <-ec
	if e.Key != "a" || e.CAS != 0 {
		t.Errorf("expected event on Del()")
	}
	select {
	case <-ec2:
		t.Errorf("expected no events for ec2")
	default:
	}

	cas3, err := c.Set("a", []byte("AA"), 0)
	if err != nil || cas3 != 3 {
		t.Errorf("expected Set() on initial cfg simple")
	}
	runtime.Gosched()
	e = <-ec
	if e.Key != "a" || e.CAS != 3 {
		t.Errorf("expected event on Set()")
	}
	select {
	case <-ec2:
		t.Errorf("expected no events for ec2")
	default:
	}

	err = c.Refresh()
	if err != nil {
		t.Errorf("expected Refresh() to work, got err: %v", err)
	}
	e = <-ec
	if e.Key != "a" || e.CAS != 3 {
		t.Errorf("expected a event after Refresh()")
	}
	e = <-ec2
	if e.Key != "aaa" || e.CAS != 0 {
		t.Errorf("expected aaa event after Refresh()")
	}
}

// ------------------------------------------------

func TestCfgMemRev(t *testing.T) {
	m := NewCfgMem()
	cas, err := m.Set("key", []byte("val"), CFG_CAS_FORCE)
	if err != nil {
		t.Errorf("expected no err")
	}
	rev, err := m.GetRev("key", cas)
	if err != nil {
		t.Errorf("expected no err on GetRev with right cas")
	}
	if rev != nil {
		t.Errorf("expected rev of nil when not set")
	}
	rev, err = m.GetRev("key-not-there", 0)
	if err != nil || rev != nil {
		t.Errorf("expected no hit on GetRev on key-not-there")
	}
	rev, err = m.GetRev("key", 0)
	if err != nil {
		t.Errorf("expected no err on GetRev with 0 cas")
	}
	if rev != nil {
		t.Errorf("expected rev of nil when not set")
	}
	err = m.SetRev("key-not-there", 10, "rev111")
	if err == nil {
		t.Errorf("expected err on SetRev with non-existing key")
	}
	err = m.SetRev("key", cas+10, "rev100")
	if err == nil {
		t.Errorf("expected err on SetRev with wrong cas")
	}
	err = m.SetRev("key", cas, "rev-yes")
	if err != nil {
		t.Errorf("expected no err on setrev with right cas")
	}
	rev, err = m.GetRev("key", cas+20)
	if err == nil || rev != nil {
		t.Errorf("expected no err on GetRev with wrong cas")
	}
	rev, err = m.GetRev("key", cas)
	if err != nil {
		t.Errorf("expected no err on GetRev with right cas")
	}
	if rev != "rev-yes" {
		t.Errorf("expected rev-yes")
	}
	err = m.SetRev("key", 0, "rev-yep")
	if err != nil {
		t.Errorf("expected no err on setrev with 0 cas")
	}
	rev, err = m.GetRev("key", cas)
	if err != nil {
		t.Errorf("expected no err on GetRev with right cas")
	}
	if rev != "rev-yep" {
		t.Errorf("expected rev-yep")
	}
}

// ------------------------------------------------

func TestCfgCB(t *testing.T) {
	c, err := NewCfgCB("a bad url", "some bogus bucket")
	if err == nil || c != nil {
		t.Errorf("expected NewCfgCB to fail on bogus url")
	}

	c, err = NewCfgCB("http://fake:6666666", "some bogus bucket")
	if err == nil || c != nil {
		t.Errorf("expected NewCfgCB err on real-ish, but fake url")
	}

	c, err = NewCfgCBEx("http://fake:6666666", "some bogus bucket",
		map[string]interface{}{})
	if err == nil || c != nil {
		t.Errorf("expected NewCfgCBEx err on real-ish, but fake url")
	}

	c, err = NewCfgCBEx("http://fake:6666666", "some bogus bucket",
		map[string]interface{}{
			"keyPrefix": "foo",
		})
	if err == nil || c != nil {
		t.Errorf("expected NewCfgCBEx err fake url, with keyPrefix")
	}
}

// --------------------------------------------------------------

func compareNodeDefs(a, b *NodeDefs) bool {
	for k, v := range a.NodeDefs {
		m := b.NodeDefs[k]
		fmt.Println("nodedefs %v %v", m, v)
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
	_, err := g.Set(splitKey, val, 111)
	if err != nil {
		t.Errorf("error in setting nodedefs-wanted key to metakv")
	}
	//check if splitting happend.so take the keys directly from metakv.
	l, _ := g.getAllKeys(splitKey)
	if len(l) != 3 {
		t.Errorf("incorrect keys %v", l)
	}
	val, cas, err := g.Get(splitKey, 111)
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
	_, err = g.Set(splitKey, val, cas)
	if err != nil {
		t.Errorf("error in setting nodedefs-wanted key to metakv")
	}
	l, _ = g.getAllKeys(splitKey)
	if len(l) != 4 {
		t.Errorf("incorrect keys %v", l)
	}
	val, _, err = g.Get(splitKey, cas)
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
