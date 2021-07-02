//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
