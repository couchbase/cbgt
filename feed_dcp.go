//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"encoding/json"
	"fmt"
)

// DCPFeedPrefix should be immutable after process init()'ialization.
var DCPFeedPrefix string

// DCPFeedBufferSizeBytes is representative of connection_buffer_size
// for DCP to enable flow control, defaults at 20MB.
var DCPFeedBufferSizeBytes = uint32(20 * 1024 * 1024)

// DCPFeedBufferAckThreshold is representative of the percentage of
// the connection_buffer_size when the consumer will ack back to
// the producer.
var DCPFeedBufferAckThreshold = float32(0.8)

// DCPNoopTimeIntervalSecs is representative of set_noop_interval
// for DCP to enable no-op messages, defaults at 2min.
var DCPNoopTimeIntervalSecs = uint32(120)

// DCPFeedParams are DCP data-source/feed specific connection
// parameters that may be part of a sourceParams JSON and is a
// superset of CBAuthParams.  DCPFeedParams holds the information used
// to populate a cbdatasource.BucketDataSourceOptions on calls to
// cbdatasource.NewBucketDataSource().  DCPFeedParams also implements
// the couchbase.AuthHandler interface.
type DCPFeedParams struct {
	AuthUser     string `json:"authUser,omitempty"` // May be "" for no auth.
	AuthPassword string `json:"authPassword,omitempty"`

	AuthSaslUser     string `json:"authSaslUser,omitempty"` // May be "" for no auth.
	AuthSaslPassword string `json:"authSaslPassword,omitempty"`

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a cluster manager node.
	ClusterManagerBackoffFactor float32 `json:"clusterManagerBackoffFactor,omitempty"`

	// Initial sleep time (millisecs) before first retry to cluster manager.
	ClusterManagerSleepInitMS int `json:"clusterManagerSleepInitMS,omitempty"`

	// Maximum sleep time (millisecs) between retries to cluster manager.
	ClusterManagerSleepMaxMS int `json:"clusterManagerSleepMaxMS,omitempty"`

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a data manager node.
	DataManagerBackoffFactor float32 `json:"dataManagerBackoffFactor,omitempty"`

	// Initial sleep time (millisecs) before first retry to data manager.
	DataManagerSleepInitMS int `json:"dataManagerSleepInitMS,omitempty"`

	// Maximum sleep time (millisecs) between retries to data manager.
	DataManagerSleepMaxMS int `json:"dataManagerSleepMaxMS,omitempty"`

	// Buffer size in bytes provided for UPR flow control.
	FeedBufferSizeBytes uint32 `json:"feedBufferSizeBytes,omitempty"`

	// Used for UPR flow control and buffer-ack messages when this
	// percentage of FeedBufferSizeBytes is reached.
	FeedBufferAckThreshold float32 `json:"feedBufferAckThreshold,omitempty"`

	// Time interval in seconds of NO-OP messages for UPR flow control,
	// needs to be set to a non-zero value to enable no-ops.
	NoopTimeIntervalSecs uint32 `json:"noopTimeIntervalSecs,omitempty"`

	// Used to specify whether the applications are interested
	// in receiving the xattrs information in a dcp stream.
	IncludeXAttrs bool `json:"includeXAttrs,omitempty"`

	// Used to specify whether the applications are not interested
	// in receiving the value for mutations in a dcp stream.
	NoValue bool `json:"noValue,omitempty"`

	// Scope within the bucket to stream data from.
	Scope string `json:"scope,omitempty"`

	// Collections within the scope that the feed would cover.
	Collections []string `json:"collections,omitempty"`
}

// NewDCPFeedParams returns a DCPFeedParams initialized with default
// values.
func NewDCPFeedParams() *DCPFeedParams {
	return &DCPFeedParams{}
}

// -------------------------------------------------------

// The FeedEx interface will be used to represent extended functionality
// for a DCP Feed. These functions will be invoked by the manager's error
// handlers to decide on the course of the feed.
type FeedEx interface {
	VerifyBucketNotExists() (bool, error)
	GetBucketDetails() (string, string)
}

// -------------------------------------------------------

type VBucketMetaData struct {
	FailOverLog [][]uint64 `json:"failOverLog"`
}

func ParseOpaqueToUUID(b []byte) string {
	vmd := &VBucketMetaData{}
	err := json.Unmarshal(b, &vmd)
	if err != nil {
		return ""
	}

	flogLen := len(vmd.FailOverLog)
	if flogLen < 1 || len(vmd.FailOverLog[flogLen-1]) < 1 {
		return ""
	}

	return fmt.Sprintf("%d", vmd.FailOverLog[flogLen-1][0])
}
