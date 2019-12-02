//  Copyright (c) 2018 Couchbase, Inc.
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
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocbcore"
)

// ----------------------------------------------------------------

type gocbAgentMap struct {
	// Mutex to serialize access to entries
	m sync.Mutex
	// Map of gocbcore.Agent instances by bucket <name>:<uuid>
	entries map[string]*gocbcore.Agent
}

var agentMap *gocbAgentMap

func init() {
	agentMap = &gocbAgentMap{
		entries: make(map[string]*gocbcore.Agent),
	}
}

// Fetches a gocbcore agent instance for the bucket (name:uuid),
// if not found creates a new instance and stashes it in the map.
func (am *gocbAgentMap) fetchAgent(name, uuid, params, server string,
	options map[string]string) (*gocbcore.Agent, error) {
	am.m.Lock()
	defer am.m.Unlock()

	key := name + ":" + uuid

	if _, exists := am.entries[key]; !exists {
		agent, err := newAgent(name, uuid, params, server, options)
		if err != nil {
			return nil, err
		}

		am.entries[key] = agent
	}

	return am.entries[key], nil
}

// Closes and removes the gocbcore Agent instance with the uuid.
func (am *gocbAgentMap) closeAgent(name, uuid string) {
	am.m.Lock()
	defer am.m.Unlock()

	key := name + ":" + uuid

	if _, exists := am.entries[key]; exists {
		am.entries[key].Close()
		delete(am.entries, key)
	}
}

// ----------------------------------------------------------------

func newAgent(sourceName, sourceUUID, sourceParams, serverIn string,
	options map[string]string) (*gocbcore.Agent, error) {
	server, _, bucketName :=
		CouchbaseParseSourceName(serverIn, "default", sourceName)

	auth, err := gocbAuth(sourceParams, options)
	if err != nil {
		return nil, fmt.Errorf("gocb_helper: newAgent, gocbAuth,"+
			" bucketName: %s, err: %v", bucketName, err)
	}

	config := setupAgentConfig()
	config.UserString = "stats"
	config.BucketName = bucketName
	config.Auth = auth

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return nil, fmt.Errorf("gocb_helper: newAgent, no servers provided")
	}

	err = config.FromConnStr(svrs[0])
	if err != nil {
		return nil, err
	}

	return gocbcore.CreateAgent(config)
}

// ----------------------------------------------------------------

// CBPartitions parses a sourceParams for a couchbase
// data-source/feed.
func CBPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (
	partitions []string, err error) {
	agent, err := agentMap.fetchAgent(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	numVBuckets := agent.NumVbuckets()
	rv := make([]string, numVBuckets)
	for i := 0; i < numVBuckets; i++ {
		rv[i] = strconv.Itoa(i)
	}

	return rv, nil
}

// ----------------------------------------------------------------

// CBPartitionSeqs returns a map keyed by partition/vbucket ID
// with values of each vbucket's UUID / high_seqno. It implements the
// FeedPartitionsFunc func signature.
func CBPartitionSeqs(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string) (
	map[string]UUIDSeq, error) {
	agent, err := agentMap.fetchAgent(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	rv := map[string]UUIDSeq{}

	signal := make(chan error, 1)
	op, err := agent.StatsEx(gocbcore.StatsOptions{Key: "vbucket-details"},
		func(resp *gocbcore.StatsResult, er error) {
			if resp == nil || er != nil {
				signal <- er
				return
			}

			stats := resp.Servers
			for _, nodeStats := range stats {
				if nodeStats.Error != nil || len(nodeStats.Stats) <= 0 {
					continue
				}

				for _, vbid := range vbucketIdStrings {
					stateVal, ok := nodeStats.Stats["vb_"+vbid]
					if !ok || stateVal != "active" {
						continue
					}

					uuid, ok := nodeStats.Stats["vb_"+vbid+":uuid"]
					if !ok {
						continue
					}

					seqStr, ok := nodeStats.Stats["vb_"+vbid+":high_seqno"]
					if !ok {
						continue
					}

					seq, err := strconv.ParseUint(seqStr, 10, 64)
					if err == nil {
						rv[vbid] = UUIDSeq{
							UUID: uuid,
							Seq:  seq,
						}
					}
				}
			}

			signal <- nil
		})

	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(GocbStatsTimeout)
	select {
	case err := <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return rv, err
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			err := <-signal
			return rv, err
		}
		return nil, gocbcore.ErrTimeout
	}
}

// ----------------------------------------------------------------

// CBStats returns a map of aggregated ("aggStats") and
// per-node stats ("nodesStats"). It implements the FeedStatsFunc
// func signature.
func CBStats(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string, statsKind string) (
	map[string]interface{}, error) {
	agent, err := agentMap.fetchAgent(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	signal := make(chan error, 1)
	var rv map[string]interface{}
	op, err := agent.StatsEx(gocbcore.StatsOptions{Key: statsKind},
		func(resp *gocbcore.StatsResult, er error) {
			if resp == nil || er != nil {
				signal <- er
				return
			}

			stats := resp.Servers
			aggStats := map[string]int64{} // Calculate aggregates.
			for _, nodeStats := range stats {
				if nodeStats.Error != nil {
					continue
				}

				for k, v := range nodeStats.Stats {
					iv, err := strconv.ParseInt(v, 10, 64)
					if err == nil {
						aggStats[k] += iv
					}
				}
			}

			rv = map[string]interface{}{
				"aggStats":   aggStats,
				"nodesStats": stats,
			}

			if statsKind == "" {
				rv["docCount"] = aggStats["curr_items"]
			}

			signal <- nil
		})

	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(GocbStatsTimeout)
	select {
	case err := <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return rv, err
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			err := <-signal
			return rv, err
		}
		return nil, gocbcore.ErrTimeout
	}
}

// ----------------------------------------------------------------

// CBVBucketLookUp looks up the source vBucketID for a given
// document ID and index.
func CBVBucketLookUp(docID, serverIn string,
	sourceDetails *IndexDef, req *http.Request) (string, error) {
	var config gocbcore.AgentConfig
	agent, err := gocbcore.CreateAgent(&config)
	if err != nil {
		return "", err
	}
	defer agent.Close()

	vb := agent.KeyToVbucket([]byte(docID))

	return strconv.Itoa(int(vb)), nil
}

// ----------------------------------------------------------------

type AuthParams struct {
	AuthUser     string `json:"authUser"`
	AuthPassword string `json:"authPassword"`

	AuthSaslUser     string `json:"authSaslUser"`
	AuthSaslPassword string `json:"authSaslPassword"`
}

func (a *AuthParams) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	return []gocbcore.UserPassPair{{
		Username: a.AuthUser,
		Password: a.AuthPassword,
	}}, nil
}

type AuthParamsSasl struct {
	AuthParams
}

func (a *AuthParamsSasl) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	return []gocbcore.UserPassPair{{
		Username: a.AuthSaslUser,
		Password: a.AuthSaslPassword,
	}}, nil
}

type CBAuthenticator struct{}

func (a *CBAuthenticator) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	endpoint := req.Endpoint

	// get rid of the http:// or https:// prefix from the endpoint
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	username, password, err := cbauth.GetMemcachedServiceAuth(endpoint)
	if err != nil {
		return []gocbcore.UserPassPair{{}}, err
	}

	return []gocbcore.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

func gocbAuth(sourceParams string, options map[string]string) (
	auth gocbcore.AuthProvider, err error) {
	params := &AuthParams{}

	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), params)
		if err != nil {
			return nil, fmt.Errorf("gocb_helper: gocbAuth" +
				" failed to parse sourceParams JSON to CBAuthParams")
		}
	}

	auth = params

	if params.AuthSaslUser != "" {
		auth = &AuthParamsSasl{*params}
	}

	authType := ""
	if options != nil {
		authType = options["authType"]
	}

	if authType == "cbauth" {
		auth = &CBAuthenticator{}
	}

	return auth, nil
}
