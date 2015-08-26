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
	"time"
)

const DEFAULT_STATS_SAMPLE_INTERVAL_SECS = 1
const DEFAULT_DIAG_SAMPLE_INTERVAL_SECS = 60
const DEFAULT_CFG_SAMPLE_INTERVAL_SECS = 60

// MonitorSample represents the information collected during
// monitoring and sampling a node.
type MonitorSample struct {
	Kind     string // Ex: "/api/cfg", "/api/stats", "/api/diag".
	Url      string // Ex: "http://10.0.0.1:8095".
	UUID     string
	Start    time.Time     // When we started to get this sample.
	Duration time.Duration // How long it took to get this sample.
	Error    error
	Data     []byte
}

// UrlUUID associates a URL with a UUID.
type UrlUUID struct {
	Url  string
	UUID string
}
