//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package monitor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestEmptyStartMonitorCluster(t *testing.T) {
	httpGet := func(url string) (resp *http.Response, err error) {
		t.Errorf("expected no get")
		return nil, nil
	}

	opt := MonitorClusterOptions{
		HttpGet: httpGet,
		MonitorNodesOptions: MonitorNodesOptions{
			HttpGet: httpGet,
		},
	}

	m, err := StartMonitorCluster(nil, nil, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	m.Stop()
}

func TestStartMonitorClusterEmptyCfg(t *testing.T) {
	var mut sync.Mutex

	httpGets := 0
	httpGet := func(url string) (resp *http.Response, err error) {
		mut.Lock()
		httpGets++
		mut.Unlock()

		if url != "url0/api/cfg" {
			t.Errorf("expected http get, url: %s", url)
		}

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewBuffer([]byte("{}"))),
		}, nil
	}

	sampleCh := make(chan MonitorSample)

	opt := MonitorClusterOptions{
		HttpGet: httpGet,
		MonitorNodesOptions: MonitorNodesOptions{
			HttpGet: httpGet,
		},
	}

	m, err := StartMonitorCluster([]string{"url0"},
		sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	s, ok := <-sampleCh
	if !ok ||
		s.Kind != "/api/cfg" ||
		s.Url != "url0" ||
		s.UUID != "" ||
		s.Error != nil ||
		string(s.Data) != "{}" {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	m.Stop()

	select {
	case <-sampleCh:
		t.Errorf("unexpected sample")
	default:
	}

	mut.Lock()
	if httpGets != 1 {
		t.Errorf("expected 1 http gets")
	}
	mut.Unlock()
}

func TestStartMonitorCluster(t *testing.T) {
	opt := MonitorClusterOptions{}
	samplesBeforeStop := 2
	expHttpGets := 3
	testStartMonitorCluster(t, "2x3",
		opt, samplesBeforeStop, expHttpGets, false)

	opt = MonitorClusterOptions{
		CfgSampleInterval: 100,
		MonitorNodesOptions: MonitorNodesOptions{
			StatsSampleInterval: 10000 * time.Second,
			DiagSampleInterval:  10000 * time.Second,
		},
	}
	samplesBeforeStop = 5
	expHttpGets = 6
	testStartMonitorCluster(t, "more cfgs",
		opt, samplesBeforeStop, expHttpGets, true)
}

func testStartMonitorCluster(t *testing.T,
	label string,
	opt MonitorClusterOptions,
	samplesBeforeStop int,
	expHttpGets int,
	cfgOk bool) {
	var mut sync.Mutex

	httpGets := 0
	httpGet := func(url string) (resp *http.Response, err error) {
		mut.Lock()
		httpGets++
		mut.Unlock()

		if url == "url0/api/cfg" {
			return &http.Response{
				StatusCode: 200,
				Body: io.NopCloser(bytes.NewBuffer([]byte(`{
"nodeDefsWanted": {
  "nodeDefs": {
    "uuid0": {
      "hostPort": "url0",
      "uuid": "uuid0"
    }
  }
}}`,
				))),
			}, nil
		}

		if url == "http://url0/api/stats?partitions=true" ||
			url == "http://url0/api/diag" {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBuffer([]byte("{}"))),
			}, nil
		}

		t.Errorf("expected http get, url: %s", url)

		return nil, fmt.Errorf("unexpected to reach here")
	}

	opt.HttpGet = httpGet
	opt.MonitorNodesOptions.HttpGet = httpGet

	sampleCh := make(chan MonitorSample)

	m, err := StartMonitorCluster([]string{"url0"},
		sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	s, ok := <-sampleCh
	if !ok ||
		s.Kind != "/api/cfg" ||
		s.Url != "url0" ||
		s.UUID != "" ||
		s.Error != nil ||
		len(s.Data) <= 0 {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	for i := 0; i < samplesBeforeStop; i++ {
		s, ok := <-sampleCh
		if !ok {
			t.Errorf("unexpected sample ok: %#v, ok: %v", s, ok)
		}

		if cfgOk && s.Kind == "/api/cfg" {
			if s.Url != "url0" ||
				s.UUID != "" ||
				s.Error != nil ||
				len(s.Data) <= 0 {
				t.Errorf("unexpected cfg sample: %#v, ok: %v", s, ok)
			}
			continue
		}

		if (s.Kind != "/api/stats?partitions=true" && s.Kind != "/api/diag") ||
			s.Url != "http://url0" ||
			s.UUID != "uuid0" ||
			s.Error != nil ||
			len(s.Data) == 0 {
			t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
		}
	}

	m.Stop()

	// NOTE: There may or may not be extra samples on sampleCh already
	// inflight when we were trying to Stop() -- racy'iness.

	mut.Lock()
	if httpGets < expHttpGets {
		t.Errorf("expected at least %d http gets, got: %d", expHttpGets, httpGets)
	}
	mut.Unlock()
}
