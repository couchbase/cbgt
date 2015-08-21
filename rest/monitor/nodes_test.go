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
	"testing"
)

func TestEmptyStartMonitorNodes(t *testing.T) {
	HttpGet = func(url string) (resp *http.Response, err error) {
		t.Errorf("expected no get")
		return nil, nil
	}
	defer func() {
		HttpGet = http.Get
	}()

	opt := MonitorNodesOptions{}

	m, err := StartMonitorNodes(nil, nil, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	m.Stop()
}

func Test1NodeStartMonitorNodes(t *testing.T) {
	httpGets := 0
	HttpGet = func(url string) (resp *http.Response, err error) {
		httpGets++

		if url != "url0/api/stats" &&
			url != "url0/api/diag" {
			t.Errorf("expected stats or diag, url: %s", url)
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

	opt := MonitorNodesOptions{}

	m, err := StartMonitorNodes([]UrlUUID{
		UrlUUID{"url0", "uuid0"},
	}, sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	s, ok := <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats" && s.Kind != "/api/diag") ||
		s.Url != "url0" ||
		s.UUID != "uuid0" ||
		s.Error != nil ||
		string(s.Data) != "{}" {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	s, ok = <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats" && s.Kind != "/api/diag") ||
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
	HttpGet = func(url string) (resp *http.Response, err error) {
		httpGets++

		if url == "url0/api/stats" {
			return &http.Response{
				StatusCode: 500,
				Body:       ioutil.NopCloser(bytes.NewBuffer([]byte("{}"))),
			}, nil
		}

		return nil, fmt.Errorf("httpGetErr")
	}
	defer func() {
		HttpGet = http.Get
	}()

	sampleCh := make(chan MonitorSample)

	opt := MonitorNodesOptions{}

	m, err := StartMonitorNodes([]UrlUUID{
		UrlUUID{"url0", "uuid0"},
	}, sampleCh, opt)
	if err != nil || m == nil {
		t.Errorf("expected no err")
	}

	s, ok := <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats" && s.Kind != "/api/diag") ||
		s.Url != "url0" ||
		s.UUID != "uuid0" ||
		s.Error == nil ||
		s.Data != nil {
		t.Errorf("unexpected sample: %#v, ok: %v", s, ok)
	}

	s, ok = <-sampleCh
	if !ok ||
		(s.Kind != "/api/stats" && s.Kind != "/api/diag") ||
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
		t.Errorf("expected 2 http gets")
	}
}
