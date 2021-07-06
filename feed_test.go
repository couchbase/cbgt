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
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"
)

type ErrorOnlyFeed struct {
	name string
}

func (t *ErrorOnlyFeed) Name() string {
	return t.name
}

func (t *ErrorOnlyFeed) IndexName() string {
	return t.name
}

func (t *ErrorOnlyFeed) Start() error {
	return fmt.Errorf("ErrorOnlyFeed Start() invoked")
}

func (t *ErrorOnlyFeed) Close() error {
	return fmt.Errorf("ErrorOnlyFeed Close() invoked")
}

func (t *ErrorOnlyFeed) Dests() map[string]Dest {
	return nil
}

func (t *ErrorOnlyFeed) Stats(w io.Writer) error {
	return fmt.Errorf("ErrorOnlyFeed Stats() invoked")
}

func TestParsePartitionsToVBucketIds(t *testing.T) {
	v, err := ParsePartitionsToVBucketIds(nil)
	if err != nil || v == nil || len(v) != 0 {
		t.Errorf("expected empty")
	}
	v, err = ParsePartitionsToVBucketIds(map[string]Dest{})
	if err != nil || v == nil || len(v) != 0 {
		t.Errorf("expected empty")
	}
	v, err = ParsePartitionsToVBucketIds(map[string]Dest{"123": nil})
	if err != nil || v == nil || len(v) != 1 {
		t.Errorf("expected one entry")
	}
	if v[0] != uint16(123) {
		t.Errorf("expected 123")
	}
	v, err = ParsePartitionsToVBucketIds(map[string]Dest{"!bad": nil})
	if err == nil || v != nil {
		t.Errorf("expected error")
	}
}

func TestDataSourcePartitions(t *testing.T) {
	a, err := DataSourcePartitions("a fake source type",
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err == nil || a != nil {
		t.Errorf("expected fake data source type to error")
	}

	a, err = DataSourcePartitions(SOURCE_GOCOUCHBASE,
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err == nil || a != nil {
		t.Errorf("expected couchbase source type to error on bad server url")
	}

	a, err = DataSourcePartitions(SOURCE_GOCOUCHBASE_DCP,
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err == nil || a != nil {
		t.Errorf("expected couchbase source type to error on bad server url")
	}

	a, err = DataSourcePartitions(SOURCE_GOCOUCHBASE_TAP,
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err == nil || a != nil {
		t.Errorf("expected couchbase source type to error on bad server url")
	}

	a, err = DataSourcePartitions("nil",
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err != nil || a != nil {
		t.Errorf("expected nil source type to work, but have no partitions")
	}

	a, err = DataSourcePartitions("primary",
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err == nil || a != nil {
		t.Errorf("expected dest source type to error on non-json server params")
	}

	a, err = DataSourcePartitions("primary",
		"sourceName", "sourceUUID", "", "serverURL", nil)
	if err != nil || a == nil {
		t.Errorf("expected dest source type to ok on empty server params")
	}

	a, err = DataSourcePartitions("primary",
		"sourceName", "sourceUUID", "{}", "serverURL", nil)
	if err != nil || a == nil {
		t.Errorf("expected dest source type to ok on empty JSON server params")
	}
}

func TestNilFeedStart(t *testing.T) {
	f := NewNILFeed("aaa", "bbb", nil)
	if f.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if f.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if f.Start() != nil {
		t.Errorf("expected NILFeed.Start() to work")
	}
	if f.Dests() != nil {
		t.Errorf("expected nil dests")
	}
	w := bytes.NewBuffer(nil)
	if f.Stats(w) != nil {
		t.Errorf("expected no err on nil feed stats")
	}
	if w.String() != "{}" {
		t.Errorf("expected json stats")
	}
	if f.Close() != nil {
		t.Errorf("expected nil dests")
	}
}

func TestPrimaryFeed(t *testing.T) {
	df := NewPrimaryFeed("aaa", "bbb",
		BasicPartitionFunc, map[string]Dest{})
	if df.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if df.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if df.Start() != nil {
		t.Errorf("expected PrimaryFeed start to work")
	}

	buf := make([]byte, 0, 100)
	err := df.Stats(bytes.NewBuffer(buf))
	if err != nil {
		t.Errorf("expected PrimaryFeed stats to work")
	}

	key := []byte("k")
	seq := uint64(123)
	val := []byte("v")

	if df.DataUpdate("unknown-partition", key, seq, val,
		0, DEST_EXTRAS_TYPE_NIL, nil) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.DataDelete("unknown-partition", key, seq,
		0, DEST_EXTRAS_TYPE_NIL, nil) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.SnapshotStart("unknown-partition", seq, seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.OpaqueSet("unknown-partition", val) == nil {
		t.Errorf("expected err on bad partition")
	}
	_, _, err = df.OpaqueGet("unknown-partition")
	if err == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.Rollback("unknown-partition", seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.ConsistencyWait("unknown-partition", "unknown-partition-UUID",
		"level", seq, nil) == nil {
		t.Errorf("expected err on bad partition")
	}
	df2 := NewPrimaryFeed("", "", BasicPartitionFunc, map[string]Dest{
		"some-partition": &TestDest{},
	})
	if df2.ConsistencyWait("some-partition", "some-partition-UUID",
		"level", seq, nil) != nil {
		t.Errorf("expected no err on some partition to TestDest")
	}
	_, err = df.Count(nil, nil)
	if err == nil {
		t.Errorf("expected err on counting a primary feed")
	}
	if df.Query(nil, nil, nil, nil) == nil {
		t.Errorf("expected err on querying a primary feed")
	}
}

func TestCBAuthParams(t *testing.T) {
	p := &CBAuthParams{
		AuthUser:     "au",
		AuthPassword: "ap",
	}
	a, b, c := p.GetCredentials()
	if a != "au" || b != "ap" || c != "au" {
		t.Errorf("wrong creds")
	}

	p2 := &CBAuthParamsSasl{
		CBAuthParams{
			AuthUser:     "au",
			AuthPassword: "ap",

			AuthSaslUser:     "asu",
			AuthSaslPassword: "asp",
		},
	}
	a, b, c = p2.GetCredentials()
	if a != "au" || b != "ap" || c != "au" {
		t.Errorf("wrong creds")
	}
	a, b = p2.GetSaslCredentials()
	if a != "asu" || b != "asp" {
		t.Errorf("wrong sasl creds")
	}
}

func TestVBucketIdToPartitionDest(t *testing.T) {
	var dests map[string]Dest

	pf_hi := func(partition string, key []byte, dests map[string]Dest) (Dest, error) {
		if string(key) != "hi" {
			t.Errorf("expected hi")
		}
		return nil, nil
	}
	partition, dest, err := VBucketIdToPartitionDest(pf_hi, dests, 0, []byte("hi"))
	if err != nil || dest != nil || partition != "0" {
		t.Errorf("expected no err, got: %v", err)
	}

	pf_bye := func(partition string, key []byte, dests map[string]Dest) (Dest, error) {
		if string(key) != "bye" {
			t.Errorf("expected bye")
		}
		return nil, nil
	}
	partition, dest, err = VBucketIdToPartitionDest(pf_bye, dests, 1025, []byte("bye"))
	if err != nil || dest != nil || partition != "1025" {
		t.Errorf("expected no err, got: %v", err)
	}

	pf_err := func(partition string, key []byte, dests map[string]Dest) (Dest, error) {
		return nil, fmt.Errorf("whoa_err")
	}
	partition, dest, err = VBucketIdToPartitionDest(pf_err, dests, 123, nil)
	if err == nil {
		t.Errorf("expected err")
	}
	if partition != "" {
		t.Errorf("expected empty string parition on err")
	}
	if dest != nil {
		t.Errorf("expected nil dst on err")
	}
}

func TestTAPFeedBasics(t *testing.T) {
	df, err := NewTAPFeed("aaa", "bbb",
		"url", "poolName", "bucketName", "bucketUUID", "",
		BasicPartitionFunc, map[string]Dest{}, false)
	if err != nil {
		t.Errorf("expected NewTAPFeed to work")
	}
	if df.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if df.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if df.Dests() == nil {
		t.Errorf("expected some dests")
	}
	err = df.Stats(bytes.NewBuffer(nil))
	if err != nil {
		t.Errorf("expected stats to work")
	}
}

func TestDCPFeedBasics(t *testing.T) {
	df, err := NewDCPFeed("aaa", "bbb",
		"url", "poolName", "bucketName", "bucketUUID", "",
		BasicPartitionFunc, map[string]Dest{}, false, nil)
	if err != nil {
		t.Errorf("expected NewDCPFeed to work")
	}
	if df.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if df.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if df.Dests() == nil {
		t.Errorf("expected some dests")
	}
	err = df.Stats(bytes.NewBuffer(nil))
	if err != nil {
		t.Errorf("expected stats to work")
	}
}

func TestCouchbaseParseSourceName(t *testing.T) {
	s, p, b := CouchbaseParseSourceName("s", "p", "b")
	if s != "s" ||
		p != "p" ||
		b != "b" {
		t.Errorf("expected s, p, b")
	}

	badURL := "http://a/badURL"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools/default"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools/default/buckets"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools/default/buckets/"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools//buckets/theBucket"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	bu := "http://a:8091/pools/myPool/buckets/theBucket"
	s, p, b = CouchbaseParseSourceName("s", "p", bu)
	if s != "http://a:8091" ||
		p != "myPool" ||
		b != "theBucket" {
		t.Errorf("expected theBucket, got: %s %s %s", s, p, b)
	}

	bu = "https://a:8091/pools/myPool/buckets/theBucket"
	s, p, b = CouchbaseParseSourceName("s", "p", bu)
	if s != "https://a:8091" ||
		p != "myPool" ||
		b != "theBucket" {
		t.Errorf("expected theBucket, got: %s %s %s", s, p, b)
	}

	bu = "https://%"
	s, p, b = CouchbaseParseSourceName("s", "p", bu)
	if s != "s" ||
		p != "p" ||
		b != bu {
		t.Errorf("expected spbu, got: %s %s %s", s, p, b)
	}
}

func TestCouchbasePartitions(t *testing.T) {
	tests := []struct {
		sourceType, sourceName, sourceUUID, sourceParams, serverIn string

		expErr        bool
		expPartitions []string
	}{
		{"couchbase", "", "", "", "bar", true, nil},
		{"couchbase", "", "", "{}", "bar", true, nil},
		{"couchbase", "", "", `{"authUser": "default"}`, "bar", true, nil},
		{"couchbase", "", "", `{"authUser": "baz"}`, "bar", true, nil},

		{"couchbase", "foo", "", "", "bar", true, nil},
		{"couchbase", "foo", "", "{}", "bar", true, nil},
		{"couchbase", "foo", "", `{"authUser": "default"}`, "bar", true, nil},
		{"couchbase", "foo", "", `{"authUser": "baz"}`, "bar", true, nil},

		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/default",
			"", "{}", "baz", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/default",
			"", `{"authUser": "default"}`, "baz", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/default",
			"", `{"authUser": "baz"}`, "baz", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/default",
			"", `{"authUser": "default"}`, "default", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/default",
			"", `{"authUser": "baz"}`, "default", true, nil},

		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/default",
			"", `{"authUser": "default"}`,
			"http://255.255.255.255:8091/pools/default/buckets/default", true, nil},

		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/notDefault",
			"", "{}", "baz", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/notDefault",
			"", `{"authUser": "default"}`, "baz", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/notDefault",
			"", `{"authUser": "baz"}`, "baz", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/notDefault",
			"", `{"authUser": "default"}`, "default", true, nil},
		{"couchbase", "http://255.255.255.255:8091/pools/default/buckets/notDefault",
			"", `{"authUser": "baz"}`, "default", true, nil},
	}

	for _, test := range tests {
		partitions, err := CouchbasePartitions(test.sourceType, test.sourceName,
			test.sourceUUID, test.sourceParams, test.serverIn, nil)
		if (test.expErr && err == nil) ||
			(!test.expErr && err != nil) {
			t.Errorf("test err != expErr,"+
				" err: %v, test: %#v", err, test)
		}

		if !reflect.DeepEqual(partitions, test.expPartitions) {
			t.Errorf("test partitions != expPartitions,"+
				" partitions: %v, test: %#v", partitions, test)
		}
	}
}

func TestDataSourcePrepParams(t *testing.T) {
	a, err := DataSourcePrepParams("a fake source type",
		"sourceName", "sourceUUID", "sourceParams", "serverURL", nil)
	if err == nil || a != "" {
		t.Errorf("expected fake data source type to error")
	}

	a, err = DataSourcePrepParams("primary",
		"sourceName", "sourceUUID", "", "serverURL", nil)
	if err != nil || a != "" {
		t.Errorf("expected empty data source params to ok")
	}

	a, err = DataSourcePrepParams("primary",
		"sourceName", "sourceUUID", "{}", "serverURL", nil)
	if err != nil || a != "{}" {
		t.Errorf("expected {} data source params to ok")
	}

	saw_testFeedPartitions := 0
	saw_testFeedPartitionSeqs := 0

	testFeedPartitions := func(sourceType,
		sourceName, sourceUUID, sourceParams,
		serverIn string, options map[string]string,
	) (
		partitions []string, err error,
	) {
		saw_testFeedPartitions++
		return nil, nil
	}

	testFeedPartitionSeqs := func(sourceType, sourceName, sourceUUID,
		sourceParams, serverIn string, options map[string]string,
	) (
		map[string]UUIDSeq, error,
	) {
		saw_testFeedPartitionSeqs++
		return nil, nil
	}

	RegisterFeedType("testFeed", &FeedType{
		Partitions:    testFeedPartitions,
		PartitionSeqs: testFeedPartitionSeqs,
	})

	sourceParams := `{"foo":"hoo","markPartitionSeqs":"currentPartitionSeqs"}`
	a, err = DataSourcePrepParams("testFeed",
		"sourceName", "sourceUUID", sourceParams, "serverURL", nil)
	if err != nil {
		t.Errorf("expected no err")
	}
	if a == sourceParams {
		t.Errorf("expected transformed data source params")
	}
	if saw_testFeedPartitions != 1 {
		t.Errorf("expected 1 saw_testFeedPartitions call, got: %d",
			saw_testFeedPartitions)
	}
	if saw_testFeedPartitionSeqs != 1 {
		t.Errorf("expected 1 saw_testFeedPartitionSeqs call, got: %d",
			saw_testFeedPartitionSeqs)
	}
}

func TestCouchbaseSourceVBucketLookUp(t *testing.T) {
	req, _ := http.NewRequest("POST", "/api/index/idx/pindexLookup -uAdministrator:pwd", nil)
	inDef := &IndexDef{Name: "idx", SourceName: "default", SourceType: "couchbase"}
	tests := []struct {
		documentID, serverIn string
		inDef                *IndexDef
		request              *http.Request
		expErr               bool
		expVBucketID         string
	}{
		{"test", "http://255.255.255.255:8091/pools/default/buckets/default", inDef, req, true, ""},
	}
	for _, test := range tests {
		vBucketID, err := CouchbaseSourceVBucketLookUp(test.documentID,
			test.serverIn, test.inDef, test.request)
		if (test.expErr && err == nil) ||
			(!test.expErr && err != nil) {
			t.Errorf("test err != expErr,"+
				" err: %v, test: %#v", err, test)
		}

		if !reflect.DeepEqual(vBucketID, test.expVBucketID) {
			t.Errorf("test partitions != expPartitions,"+
				" vBucketID: %v, test: %#v", vBucketID, test.expVBucketID)
		}
	}
}
