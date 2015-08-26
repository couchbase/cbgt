//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package monitor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestEmptyStartMonitorCluster(t *testing.T) {
	HttpGet = func(url string) (resp *http.Response, err error) {
		t.Errorf("expected no get")
		return nil, nil
	}
	defer func() {
		HttpGet = http.Get
	}()

	opt := MonitorClusterOptions{}

	m, err := StartMonitorCluster(nil, nil, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	m.Stop()
}

func TestStartMonitorClusterEmptyCfg(t *testing.T) {
	var mut sync.Mutex

	httpGets := 0
	HttpGet = func(url string) (resp *http.Response, err error) {
		mut.Lock()
		httpGets++
		mut.Unlock()

		if url != "url0/api/cfg" {
			t.Errorf("expected http get, url: %s", url)
		}

		return &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewBuffer([]byte("{}"))),
		}, nil
	}
	defer func() {
		HttpGet = http.Get
	}()

	sampleCh := make(chan MonitorSample)

	opt := MonitorClusterOptions{}

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
	HttpGet = func(url string) (resp *http.Response, err error) {
		mut.Lock()
		httpGets++
		mut.Unlock()

		if url == "url0/api/cfg" {
			return &http.Response{
				StatusCode: 200,
				Body: ioutil.NopCloser(bytes.NewBuffer([]byte(`{
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

		if url == "http://url0/api/stats" ||
			url == "http://url0/api/diag" {
			return &http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(bytes.NewBuffer([]byte("{}"))),
			}, nil
		}

		t.Errorf("expected http get, url: %s", url)

		return nil, fmt.Errorf("unexpected to reach here")
	}
	defer func() {
		HttpGet = http.Get
	}()

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

		if (s.Kind != "/api/stats" && s.Kind != "/api/diag") ||
			s.Url != "http://url0" ||
			s.UUID != "uuid0" ||
			s.Error != nil ||
			len(s.Data) == 0 {
			t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
		}
	}

	m.Stop()

	select {
	case s := <-sampleCh:
		t.Errorf("unexpected sample, s: %#v", s)
	default:
	}

	mut.Lock()
	if httpGets < expHttpGets {
		t.Errorf("expected at least %d http gets, got: %d", expHttpGets, httpGets)
	}
	mut.Unlock()
}
