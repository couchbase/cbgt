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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
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
				log.Warnf("run: json.Unmarshal, cfgBytes: %q, err: %v",
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
		hostPortUrl := "http://" + nodeDef.HostPort
		if u, err := nodeDef.HttpsURL(); err == nil {
			hostPortUrl = u
		}

		r = append(r, UrlUUID{
			Url:  hostPortUrl,
			UUID: nodeDef.UUID,
		})
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

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			errs = append(errs, fmt.Errorf("httpGetBytes,"+
				" io.ReadAll, url: %s, err: %v", url, err))
			continue // Try next url.
		}

		return baseURL, start, duration, body, nil
	}

	return "", time.Time{}, time.Duration(0), nil, errs
}
