//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

const defaultOSOBackfillMode = false
const defaultInitialBootstrapNonTLS = true

// GocbcoreConnectTimeout and GocbcoreKVConnectTimeout are timeouts used
// by gocbcore to connect to the cluster manager and KV
var GocbcoreConnectTimeout = time.Duration(60 * time.Second)
var GocbcoreKVConnectTimeout = time.Duration(7 * time.Second)

// GocbcoreAgentSetupTimeout is the time alloted for completing setup of
// a gocbcore.Agent or a gocbcore.DCPAgent, two factors ..
//   - cluster state to be online
//   - memcached service to be ready
var GocbcoreAgentSetupTimeout = time.Duration(60 * time.Second)

// GocbcoreStatsTimeout is the time alloted to obtain a response from
// the server for a stats request.
var GocbcoreStatsTimeout = time.Duration(60 * time.Second)

var errAgentSetupFailed = fmt.Errorf("agent setup failed")

// ----------------------------------------------------------------

// gocbcore's Agent it's AgentConfig

func setupAgentConfig(name, sourceName string,
	auth gocbcore.AuthProvider, options map[string]string) *gocbcore.AgentConfig {
	initialBootstrapNonTLS := defaultInitialBootstrapNonTLS
	if options["feedInitialBootstrapNonTLS"] == "true" {
		initialBootstrapNonTLS = true
	} else if options["feedInitialBootstrapNonTLS"] == "false" {
		initialBootstrapNonTLS = false
	}

	useCollections := true
	if options["disableCollectionsSupport"] == "true" {
		useCollections = false
	}

	return &gocbcore.AgentConfig{
		UserAgent:  name,
		BucketName: sourceName,
		SecurityConfig: gocbcore.SecurityConfig{
			NoTLSSeedNode: initialBootstrapNonTLS,
			Auth:          auth,
		},
		IoConfig: gocbcore.IoConfig{
			NetworkType:    "default",
			UseCollections: useCollections,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectTimeout: GocbcoreKVConnectTimeout,
		},
		HTTPConfig: gocbcore.HTTPConfig{
			MaxIdleConns:        HttpTransportMaxIdleConns,
			MaxIdleConnsPerHost: HttpTransportMaxIdleConnsPerHost,
			ConnectTimeout:      GocbcoreConnectTimeout,
		},
	}
}

func setupGocbcoreAgent(config *gocbcore.AgentConfig) (
	*gocbcore.Agent, error) {
	agent, err := gocbcore.CreateAgent(config)
	if err != nil {
		return nil, fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState:  gocbcore.ClusterStateOnline,
		ServiceTypes:  []gocbcore.ServiceType{gocbcore.MemdService},
		RetryStrategy: &retryStrategy{},
	}

	signal := make(chan error, 1)
	_, err = agent.WaitUntilReady(time.Now().Add(GocbcoreAgentSetupTimeout),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		})

	if err == nil {
		err = <-signal
	}

	if err != nil {
		go agent.Close()
		return nil, fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	log.Printf("gocbcore_utils: CreateAgent succeeded"+
		" (agent: %p, bucket: %s, name: %s)",
		agent, config.BucketName, config.UserAgent)

	return agent, nil
}

// ----------------------------------------------------------------

// gocbcore's DCPAgent & it's DCPAgentConfig

func setupDCPAgentConfig(
	name, bucketName string,
	auth gocbcore.AuthProvider,
	agentPriority gocbcore.DcpAgentPriority,
	options map[string]string) *gocbcore.DCPAgentConfig {

	config := &gocbcore.DCPAgentConfig{
		UserAgent:  name,
		BucketName: bucketName,
		SecurityConfig: gocbcore.SecurityConfig{
			NoTLSSeedNode: defaultInitialBootstrapNonTLS,
			Auth:          auth,
		},
		IoConfig: gocbcore.IoConfig{
			NetworkType:    "default",
			UseCollections: true,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectTimeout: GocbcoreKVConnectTimeout,
			// FIXME ServerWaitBackoff: ?,
			// FIXME PoolSize: ?,
			// FIXME ConnectionBufferSize: ?,
		},
		HTTPConfig: gocbcore.HTTPConfig{
			MaxIdleConns:        HttpTransportMaxIdleConns,
			MaxIdleConnsPerHost: HttpTransportMaxIdleConnsPerHost,
			ConnectTimeout:      GocbcoreConnectTimeout,
		},
		DCPConfig: gocbcore.DCPConfig{
			AgentPriority:  agentPriority,
			UseStreamID:    true,
			UseOSOBackfill: defaultOSOBackfillMode,
			BackfillOrder:  gocbcore.DCPBackfillOrderRoundRobin,
			BufferSize:     int(DCPFeedBufferSizeBytes),
		},
	}

	if options["useOSOBackfill"] == "true" {
		config.DCPConfig.UseOSOBackfill = true
	} else if options["useOSOBackfill"] == "false" {
		config.DCPConfig.UseOSOBackfill = false
	}

	if options["feedInitialBootstrapNonTLS"] == "true" {
		config.SecurityConfig.NoTLSSeedNode = true
	} else if options["feedInitialBootstrapNonTLS"] == "false" {
		config.SecurityConfig.NoTLSSeedNode = false
	}

	if options["disableCollectionsSupport"] == "true" {
		config.IoConfig.UseCollections = false
	}

	if options["disableStreamIDs"] == "true" {
		config.DCPConfig.UseStreamID = false
	}

	if val, exists := options["kvConnectionWaitBackoff"]; exists {
		// kvConnectionWaitBackoff is the period of time that the SDK will wait before
		// reattempting connection to a node after bootstrap fails against that node.
		if valDuration, err := time.ParseDuration(val); err == nil {
			config.KVConfig.ServerWaitBackoff = valDuration
		}
	}

	if val, exists := options["kvConnectionPoolSize"]; exists {
		// kvConnectionPoolSize is the number of connections to create to each node.
		if valInt, err := strconv.Atoi(val); err == nil {
			config.KVConfig.PoolSize = valInt
		}
	}

	if val, exists := options["kvConnectionBufferSize"]; exists {
		// kvConnectionBufferSize is the buffer size to use for connections.
		if valInt, err := strconv.Atoi(val); err == nil {
			config.KVConfig.ConnectionBufferSize = uint(valInt)
		}
	}

	return config
}

func setupGocbcoreDCPAgent(config *gocbcore.DCPAgentConfig,
	connName string, flags memd.DcpOpenFlag) (
	*gocbcore.DCPAgent, error) {
	agent, err := gocbcore.CreateDcpAgent(config, connName, flags)
	if err != nil {
		return nil, fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState: gocbcore.ClusterStateOnline,
		ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService},
	}

	signal := make(chan error, 1)
	_, err = agent.WaitUntilReady(time.Now().Add(GocbcoreAgentSetupTimeout),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		})

	if err == nil {
		err = <-signal
	}

	if err != nil {
		log.Warnf("feed_dcp_gocbcore: CreateDcpAgent, err: %v (close DCPAgent: %p)",
			err, agent)
		go agent.Close()
		return nil, fmt.Errorf("%w, err: %v", errAgentSetupFailed, err)
	}

	log.Printf("feed_dcp_gocbcore: CreateDcpAgent succeeded"+
		" (agent: %p, bucketName: %s, name: %s)",
		agent, config.BucketName, config.UserAgent)

	return agent, nil
}
