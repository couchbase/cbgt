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

func TestEmptyStartMonitorNodes(t *testing.T) {
	httpGet := func(url string) (resp *http.Response, err error) {
		t.Errorf("expected no get")
		return nil, nil
	}

	opt := MonitorNodesOptions{
		HttpGet: httpGet,
	}

	m, err := StartMonitorNodes(nil, nil, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	m.Stop()
}

func Test1NodeStartMonitorNodes(t *testing.T) {
	httpGets := 0
	httpGet := func(url string) (resp *http.Response, err error) {
		httpGets++

		if url != "url0/api/stats?partitions=true" &&
			url != "url0/api/diag" {
			t.Errorf("expected stats or diag, url: %s", url)
		}

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewBuffer([]byte("{}"))),
		}, nil
	}

	sampleCh := make(chan MonitorSample)

	opt := MonitorNodesOptions{
		HttpGet: httpGet,
	}

	m, err := StartMonitorNodes([]UrlUUID{
		{"url0", "uuid0"},
	}, sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	s, ok := <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats?partitions=true" && s.Kind != "/api/diag") ||
		s.Url != "url0" ||
		s.UUID != "uuid0" ||
		s.Error != nil ||
		string(s.Data) != "{}" {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	s, ok = <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats?partitions=true" && s.Kind != "/api/diag") ||
		s.Url != "url0" ||
		s.UUID != "uuid0" ||
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

	if httpGets != 2 {
		t.Errorf("expected 2 http gets")
	}
}

func Test1NodeStartMonitorNodesAllErrors(t *testing.T) {
	httpGets := 0
	httpGet := func(url string) (resp *http.Response, err error) {
		httpGets++

		if url == "url0/api/stats?partitions=true" {
			return &http.Response{
				StatusCode: 500,
				Body:       io.NopCloser(bytes.NewBuffer([]byte("{}"))),
			}, nil
		}

		return nil, fmt.Errorf("httpGetErr")
	}

	sampleCh := make(chan MonitorSample)

	opt := MonitorNodesOptions{
		StatsSampleInterval: 10000 * time.Second,
		DiagSampleInterval:  10000 * time.Second,
		HttpGet:             httpGet,
	}

	m, err := StartMonitorNodes([]UrlUUID{
		{"url0", "uuid0"},
	}, sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	s, ok := <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats?partitions=true" && s.Kind != "/api/diag") ||
		s.Url != "url0" ||
		s.UUID != "uuid0" ||
		s.Error == nil ||
		s.Data != nil {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	s, ok = <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats?partitions=true" && s.Kind != "/api/diag") ||
		s.Url != "url0" ||
		s.UUID != "uuid0" ||
		s.Error == nil ||
		s.Data != nil {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	m.Stop()

	select {
	case <-sampleCh:
		t.Errorf("unexpected sample")
	default:
	}

	if httpGets != 2 {
		t.Errorf("expected 2 http gets, got: %d", httpGets)
	}
}

func Test1NodeStartMonitorNodesFast(t *testing.T) {
	var mut sync.Mutex
	httpGets := 0

	httpGet := func(url string) (resp *http.Response, err error) {
		mut.Lock()
		httpGets++
		mut.Unlock()

		if url != "url0/api/stats?partitions=true" &&
			url != "url0/api/diag" {
			t.Errorf("expected stats or diag, url: %s", url)
		}

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewBuffer([]byte("{}"))),
		}, nil
	}

	sampleCh := make(chan MonitorSample)

	opt := MonitorNodesOptions{
		StatsSampleInterval: 100,
		DiagSampleInterval:  100,
		HttpGet:             httpGet,
	}

	m, err := StartMonitorNodes([]UrlUUID{
		{"url0", "uuid0"},
	}, sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	go func() {
		for s := range sampleCh {
			if (s.Kind != "/api/stats?partitions=true" && s.Kind != "/api/diag") ||
				s.Url != "url0" ||
				s.UUID != "uuid0" ||
				s.Error != nil ||
				s.Data == nil {
				t.Errorf("unexpected sample: %#v", s)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)

	m.Stop()

	mut.Lock()
	if httpGets <= 20 {
		t.Errorf("expected many http gets")
	}
	mut.Unlock()
}
