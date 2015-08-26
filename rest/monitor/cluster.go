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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
	"github.com/couchbaselabs/cbgt/rest"
)

// A MonitorCluster struct holds all the tracking information for the
// StartMonitorCluster operation.
type MonitorCluster struct {
	seedURLs []string // Array of seed REST URL's to try.
	sampleCh chan MonitorSample
	options  MonitorClusterOptions
	stopCh   chan struct{}

	m sync.Mutex // Protects the mutable fields that follow.

	lastCfgBytes []byte
	lastCfg      rest.RESTCfg

	monitorNodes    *MonitorNodes
	monitorNodeDefs *cbgt.NodeDefs
}

type MonitorClusterOptions struct {
	CfgSampleInterval   time.Duration // Ex: 1 * time.Second.
	MonitorNodesOptions MonitorNodesOptions

	// Optional, defaults to http.Get(); this is used, for example,
	// for unit testing.
	HttpGet func(url string) (resp *http.Response, err error)
}

// --------------------------------------------------------

// StartMonitorCluster begins monitoring a cbgt cluster (as defined by
// a REST /api/cfg endpoint), including the handling of node
// membership changes.
//
// TODO: Allow a monitoring of other MonitorCluster's in a hierarchy
// to support the scaling out to large number of cbgt nodes.
func StartMonitorCluster(
	seedURLs []string,
	sampleCh chan MonitorSample,
	options MonitorClusterOptions,
) (*MonitorCluster, error) {
	m := &MonitorCluster{
		seedURLs: seedURLs,
		sampleCh: sampleCh,
		options:  options,
		stopCh:   make(chan struct{}),
	}

	go m.run()

	return m, nil
}

func (m *MonitorCluster) Stop() {
	close(m.stopCh)
}

// --------------------------------------------------------

func (m *MonitorCluster) run() {
	cfgSampleInterval := m.options.CfgSampleInterval
	if cfgSampleInterval <= 0 {
		cfgSampleInterval = DEFAULT_CFG_SAMPLE_INTERVAL_SECS * time.Second
	}

	httpGet := m.options.HttpGet
	if httpGet == nil {
		httpGet = http.Get
	}

	for {
		seedURL, start, duration, cfgBytes, errs :=
			httpGetBytes(httpGet, m.seedURLs, "/api/cfg")
		if len(errs) > 0 {
			log.Printf("run: httpGetBytes, errs: %+v", errs)

			m.sampleCh <- MonitorSample{
				Kind:     "/api/cfg",
				Url:      seedURL,
				UUID:     "",
				Start:    start,
				Duration: duration,
				Error:    errs[0],
				Data:     cfgBytes,
			}

			continue
		}

		if seedURL == "" {
			return
		}

		m.m.Lock()
		cfgBytesSame := bytes.Equal(m.lastCfgBytes, cfgBytes)
		m.m.Unlock()

		if !cfgBytesSame {
			rc := rest.RESTCfg{}
			err := json.Unmarshal(cfgBytes, &rc)
			if err != nil {
				log.Printf("run: json.Unmarshal, cfgBytes: %q, err: %v",
					cfgBytes, err)
				continue
			}

			m.m.Lock()

			m.lastCfg = rc

			if m.monitorNodes != nil &&
				!SameNodeDefs(m.monitorNodeDefs, m.lastCfg.NodeDefsWanted) {
				m.monitorNodes.Stop()
				m.monitorNodes = nil
				m.monitorNodeDefs = nil
			}

			m.m.Unlock()

			m.sampleCh <- MonitorSample{
				Kind:     "/api/cfg",
				Url:      seedURL,
				UUID:     "",
				Start:    start,
				Duration: duration,
				Error:    nil,
				Data:     cfgBytes,
			}
		}

		m.m.Lock()
		if m.monitorNodes == nil {
			monitorNodes, err := StartMonitorNodes(
				NodeDefsUrlUUIDs(m.lastCfg.NodeDefsWanted),
				m.sampleCh,
				m.options.MonitorNodesOptions)
			if err != nil {
				m.monitorNodeDefs = m.lastCfg.NodeDefsWanted
				m.monitorNodes = monitorNodes
			}
		}
		m.m.Unlock()

		// Wait until it's time for the next sampling.
		select {
		case <-m.stopCh:
			return

		case <-time.After(cfgSampleInterval):
			// NO-OP.
		}
	}
}

// ------------------------------------------------------

func SameNodeDefs(a, b *cbgt.NodeDefs) bool {
	return reflect.DeepEqual(a, b)
}

func NodeDefsUrlUUIDs(nodeDefs *cbgt.NodeDefs) (r []UrlUUID) {
	if nodeDefs == nil {
		return nil
	}

	for _, nodeDef := range nodeDefs.NodeDefs {
		// TODO: Security/auth.
		r = append(r, UrlUUID{"http://" + nodeDef.HostPort, nodeDef.UUID})
	}

	return r
}

// ------------------------------------------------------

func httpGetBytes(
	httpGet func(url string) (resp *http.Response, err error),
	baseURLs []string, suffix string) (
	baseURL string,
	start time.Time,
	duration time.Duration,
	body []byte,
	errs []error) {
	errs = make([]error, 0, len(baseURLs))

	for _, baseURL = range baseURLs {
		url := baseURL + "/api/cfg"

		start = time.Now()
		resp, err := httpGet(url)
		duration = time.Now().Sub(start)

		if err != nil {
			errs = append(errs, fmt.Errorf("httpGetBytes,"+
				" httpGet, url: %s, err: %v", url, err))
			continue // Try next url.
		}

		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			errs = append(errs, fmt.Errorf("httpGetBytes,"+
				" url: %s, resp: %#v", url, resp))
			continue // Try next url.
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errs = append(errs, fmt.Errorf("httpGetBytes,"+
				" ioutil.ReadAll, url: %s, err: %v", url, err))
			continue // Try next url.
		}

		return baseURL, start, duration, body, nil
	}

	return "", time.Time{}, time.Duration(0), nil, errs
}
