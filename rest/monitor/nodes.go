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
	"io/ioutil"
	"time"
)

// A MonitorNodes struct holds all the tracking information for the
// StartMonitorNodes operation.
type MonitorNodes struct {
	nodes    []string // Array of base REST URL's to monitor.
	sampleCh chan MonitorSample
	options  MonitorNodesOptions
	stopCh   chan struct{}
}

type MonitorNodesOptions struct {
	StatsSampleInterval time.Duration // Ex: 1 * time.Second.
	DiagSampleInterval  time.Duration
}

// StartMonitorNodes begins REST stats and diag sampling from a fixed
// set of cbgt nodes.  Higher level parts (like StartMonitorCluster)
// should handle situations of node membership changes by stopping and
// restarting StartMonitorNodes() as needed.
//
// The cbgt REST URL endpoints that are monitored are [node]/api/stats
// and [node]/api/diag.
func StartMonitorNodes(
	nodes []string,
	sampleCh chan MonitorSample,
	options MonitorNodesOptions,
) (*MonitorNodes, error) {
	m := &MonitorNodes{
		nodes:    nodes,
		sampleCh: sampleCh,
		options:  options,
		stopCh:   make(chan struct{}),
	}

	for _, node := range nodes {
		go m.runNode(node)
	}

	return m, nil
}

func (m *MonitorNodes) Stop() {
	close(m.stopCh)
}

func (m *MonitorNodes) runNode(node string) {
	statsSampleInterval := m.options.StatsSampleInterval
	if statsSampleInterval <= 0 {
		statsSampleInterval =
			DEFAULT_STATS_SAMPLE_INTERVAL_SECS * time.Second
	}

	diagSampleInterval := m.options.StatsSampleInterval
	if diagSampleInterval <= 0 {
		diagSampleInterval =
			DEFAULT_DIAG_SAMPLE_INTERVAL_SECS * time.Second
	}

	statsTicker := time.NewTicker(statsSampleInterval)
	defer statsTicker.Stop()

	diagTicker := time.NewTicker(diagSampleInterval)
	defer diagTicker.Stop()

	for {
		select {
		case <-m.stopCh:
			return

		case t, ok := <-statsTicker.C:
			if !ok {
				return
			}

			m.sample(node, "/api/stats", t)

		case t, ok := <-diagTicker.C:
			if !ok {
				return
			}

			m.sample(node, "/api/diag", t)
		}
	}
}

func (m *MonitorNodes) sample(node string, kind string,
	start time.Time) {
	res, err := HttpGet(node + kind)

	duration := time.Now().Sub(start)

	data := []byte(nil)
	if res.StatusCode == 200 {
		var dataErr error

		data, dataErr = ioutil.ReadAll(res.Body)
		if err == nil && dataErr != nil {
			err = dataErr
		}
	}

	res.Body.Close()

	m.sampleCh <- MonitorSample{
		Kind:     kind,
		Node:     node,
		Start:    start,
		Duration: duration,
		Error:    err,
		Data:     data,
	}
}
