//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

// ----------------------------------------------------------------

type retryStrategy struct{}

func (rs *retryStrategy) RetryAfter(req gocbcore.RetryRequest,
	reason gocbcore.RetryReason) gocbcore.RetryAction {
	if reason == gocbcore.BucketNotReadyReason {
		return &gocbcore.WithDurationRetryAction{
			WithDuration: gocbcore.ControlledBackoff(req.RetryAttempts()),
		}
	}

	return &gocbcore.NoRetryRetryAction{}
}

func setupAgentConfig(name, bucketName string,
	auth gocbcore.AuthProvider) *gocbcore.AgentConfig {
	return &gocbcore.AgentConfig{
		UserAgent:        name,
		BucketName:       bucketName,
		Auth:             auth,
		ConnectTimeout:   GocbcoreConnectTimeout,
		KVConnectTimeout: GocbcoreKVConnectTimeout,
		UseCollections:   true,
	}
}

var errAgentSetupFailed = fmt.Errorf("agent setup failed")

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

type gocbcoreClient struct {
	agent    *gocbcore.Agent
	dcpAgent *gocbcore.DCPAgent
}

type gocbcoreAgentsMap struct {
	// mutex to serialize access to entries
	m sync.Mutex
	// map of gocbcore.Agent, gocbcore.DCPAgent instances by bucket <name>:<uuid>
	entries map[string]*gocbcoreClient
}

var statsAgentsMap *gocbcoreAgentsMap

func init() {
	statsAgentsMap = &gocbcoreAgentsMap{
		entries: make(map[string]*gocbcoreClient),
	}
}

// Fetches gocbcore agent+dcpAgent instance for the bucket (name:uuid),
// if not found creates a new instance and stashes it in the map,
// before returning it.
func (am *gocbcoreAgentsMap) fetchClient(sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (*gocbcoreClient, error) {
	am.m.Lock()
	defer am.m.Unlock()

	key := sourceName + ":" + sourceUUID
	if _, exists := am.entries[key]; exists {
		return am.entries[key], nil
	}

	server, _, bucketName :=
		CouchbaseParseSourceName(serverIn, "default", sourceName)

	auth, err := gocbAuth(sourceParams, options["authType"])
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: fetchClient, gocbAuth,"+
			" bucketName: %s, err: %v", bucketName, err)
	}

	config := setupAgentConfig("stats", bucketName, auth)
	dcpConnName := fmt.Sprintf("stats-%s-%x", key, rand.Int31())
	dcpConfig := setupDCPAgentConfig(dcpConnName, bucketName, auth,
		gocbcore.DcpAgentPriorityLow, false /* no need for streamID */, nil)

	svrs := strings.Split(server, ";")
	if len(svrs) == 0 {
		return nil, fmt.Errorf("gocbcore_utils: fetchClient, no servers provided")
	}

	connStr := svrs[0]
	if connURL, err := url.Parse(svrs[0]); err == nil {
		if strings.HasPrefix(connURL.Scheme, "http") {
			// tack on an option: bootstrap_on=http for gocbcore SDK
			// connections to force HTTP config polling
			if ret, err := connURL.Parse("?bootstrap_on=http"); err == nil {
				connStr = ret.String()
			}
		}
	}

	err = config.FromConnStr(connStr)
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: fetchClient, unable to build"+
			" agent config from connStr: %s, err: %v", connStr, err)
	}
	err = dcpConfig.FromConnStr(connStr)
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: fetchClient, unable to build"+
			" dcpAgent config from connStr: %s, err: %v", connStr, err)
	}

	agent, err := setupGocbcoreAgent(config)
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: fetchClient (1), setup err: %w", err)
	}

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		log.Warnf("gocbcore_utils: fetchClient, ConfigSnapshot err: %v (close Agent: %p)",
			err, agent)
		go agent.Close()
		return nil, err
	}

	// if sourceUUID is provided, ensure that it matches with the bucket's UUID
	if len(sourceUUID) > 0 && sourceUUID != snapshot.BucketUUID() {
		go agent.Close()
		return nil, fmt.Errorf("gocbcore_utils: mismatched sourceUUID for"+
			" bucket `%v`", sourceName)
	}

	dcpAgent, err := setupGocbcoreDCPAgent(dcpConfig, dcpConnName, memd.DcpOpenFlagProducer)
	if err != nil {
		log.Warnf("gocbcore_utils: fetchClient, setupGocbcoreDCPAgent err: %v"+
			" (close Agent: %p)", err, agent)
		go agent.Close()
		return nil, fmt.Errorf("gocbcore_utils: fetchClient (2), setup err: %w", err)
	}

	am.entries[key] = &gocbcoreClient{
		agent:    agent,
		dcpAgent: dcpAgent,
	}

	log.Printf("gocbcore_utils: fetchClient, new agents setup (Agent: %p, DCPAgent: %p)",
		agent, dcpAgent)

	return am.entries[key], nil
}

// Closes and removes the gocbcore Agent instance with the name:uuid.
func (am *gocbcoreAgentsMap) closeClient(sourceName, sourceUUID string) {
	key := sourceName + ":" + sourceUUID
	am.m.Lock()
	if _, exists := am.entries[key]; exists {
		log.Printf("gocbcore_utils: closeClient, closing agents (Agent: %p, DCPAgent: %p)",
			am.entries[key].agent, am.entries[key].dcpAgent)
		go am.entries[key].agent.Close()
		go am.entries[key].dcpAgent.Close()
		delete(am.entries, key)
	}
	am.m.Unlock()
}

// ----------------------------------------------------------------

// CBPartitions parses a sourceParams for a couchbase
// data-source/feed.
func CBPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (
	partitions []string, err error) {
	client, err := statsAgentsMap.fetchClient(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	snapshot, err := client.agent.ConfigSnapshot()
	if err != nil {
		return nil, fmt.Errorf("CBPartitions, ConfigSnapshot err: %v", err)
	}

	numVBuckets, err := snapshot.NumVbuckets()
	if err != nil {
		return nil, fmt.Errorf("CBPartitions, NumVbuckets err: %v", err)
	}

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
	client, err := statsAgentsMap.fetchClient(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, fmt.Errorf("CBPartitionSeqs, fetchClient err: %v", err)
	}

	signal := make(chan error, 1)

	var manifest gocbcore.Manifest
	op, err := client.agent.GetCollectionManifest(
		gocbcore.GetCollectionManifestOptions{},
		func(res *gocbcore.GetCollectionManifestResult, er error) {
			if er == nil && res == nil {
				er = fmt.Errorf("manifest not retrieved")
			}

			if er == nil {
				er = manifest.UnmarshalJSON(res.Manifest)
			}

			signal <- er
		})

	if err != nil {
		return nil,
			fmt.Errorf("CBPartitionSeqs, GetCollectionManifest err: %v", err)
	}
	if err = waitForResponse(signal, nil, op, GocbcoreStatsTimeout); err != nil {
		return nil,
			fmt.Errorf("CBPartitionSeqs, GetCollectionManifest failed, err: %v", err)
	}

	collectionsIDtoName := map[uint32]string{}
	for _, manifestScope := range manifest.Scopes {
		for _, coll := range manifestScope.Collections {
			collectionsIDtoName[coll.UID] = manifestScope.Name + ":" + coll.Name
		}
	}

	rv := map[string]UUIDSeq{}
	addSeqnoToRV := func(vbID uint16, collID uint32, seqNo uint64) {
		rv[vbucketIdStrings[vbID]+":"+collectionsIDtoName[collID]] = UUIDSeq{
			UUID: fmt.Sprintf("%v", collID),
			Seq:  seqNo,
		}
	}

	vbucketSeqnoOptions := gocbcore.GetVbucketSeqnoOptions{
		FilterOptions: &gocbcore.GetVbucketSeqnoFilterOptions{},
	}

	for collID := range collectionsIDtoName {
		vbucketSeqnoOptions.FilterOptions.CollectionID = collID
		op, err := client.dcpAgent.GetVbucketSeqnos(
			0,                       // serverIdx (leave at 0 for now)
			memd.VbucketStateActive, // active vbuckets only
			vbucketSeqnoOptions,     // contains collectionID
			func(entries []gocbcore.VbSeqNoEntry, er error) {
				if er == nil {
					for _, entry := range entries {
						addSeqnoToRV(entry.VbID, collID, uint64(entry.SeqNo))
					}
				}

				signal <- er
			})

		if err != nil {
			return nil, fmt.Errorf("CBPartitionSeqs, GetVbucketSeqnos err: %v", err)
		}

		if err = waitForResponse(signal, nil, op, GocbcoreStatsTimeout); err != nil {
			return nil, fmt.Errorf("CBPartitionSeqs, GetVbucketSeqnos callback err: %v", err)
		}
	}

	return rv, nil
}

// ----------------------------------------------------------------

// CBStats returns a map of aggregated ("aggStats") and
// per-node stats ("nodesStats"). It implements the FeedStatsFunc
// func signature.
func CBStats(sourceType, sourceName, sourceUUID,
	sourceParams, serverIn string,
	options map[string]string, statsKind string) (
	map[string]interface{}, error) {
	client, err := statsAgentsMap.fetchClient(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	signal := make(chan error, 1)
	var rv map[string]interface{}
	op, err := client.agent.Stats(gocbcore.StatsOptions{Key: statsKind},
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

			rv["docCount"] = aggStats["curr_items"]

			signal <- nil
		})

	if err != nil {
		return nil, err
	}

	err = waitForResponse(signal, nil, op, GocbcoreStatsTimeout)
	return rv, err
}

// ----------------------------------------------------------------

// CBVBucketLookUp looks up the source vBucketID for a given
// document ID and index.
func CBVBucketLookUp(docID, serverIn string,
	sourceDetails *IndexDef, req *http.Request) (string, error) {
	var config gocbcore.AgentConfig
	agent, err := setupGocbcoreAgent(&config)
	if err != nil {
		return "", err
	}

	defer func() {
		log.Printf("gocbcore_utils: Closing Agent (%p)", agent)
		go agent.Close()
	}()

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return "", err
	}

	vb, err := snapshot.KeyToVbucket([]byte(docID))
	if err != nil {
		return "", err
	}

	return strconv.Itoa(int(vb)), nil
}

// ----------------------------------------------------------------

// CBSourceUUIDLookUp fetches the sourceUUID for the provided sourceName.
func CBSourceUUIDLookUp(sourceName, sourceParams, serverIn string,
	options map[string]string) (string, error) {
	server, _, bucketName :=
		CouchbaseParseSourceName(serverIn, "default", sourceName)

	auth, err := gocbAuth(sourceParams, options["authType"])
	if err != nil {
		return "", fmt.Errorf("gocbcore_utils: CBSourceUUIDLookUp, gocbAuth,"+
			" bucketName: %s, err: %v", bucketName, err)
	}

	config := setupAgentConfig("cb-source-uuid", bucketName, auth)

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return "", fmt.Errorf("gocbcore_utils: CBSourceUUIDLookUp," +
			" no servers provided")
	}

	connStr := svrs[0]
	if connURL, err := url.Parse(svrs[0]); err == nil {
		if strings.HasPrefix(connURL.Scheme, "http") {
			// tack on an option: bootstrap_on=http for gocbcore SDK
			// connections to force HTTP config polling
			if ret, err := connURL.Parse("?bootstrap_on=http"); err == nil {
				connStr = ret.String()
			}
		}
	}

	err = config.FromConnStr(connStr)
	if err != nil {
		return "", fmt.Errorf("gocbcore_utils: CBSourceUUIDLookUp,"+
			" unable to build config from connStr: %s, err: %v", connStr, err)
	}

	agent, err := setupGocbcoreAgent(config)
	if err != nil {
		return "", fmt.Errorf("gocbcore_utils: CBSourceUUIDLookUp,"+
			" unable to create agent, err: %w", err)
	}

	defer func() {
		log.Printf("gocbcore_utils: Closing Agent (%p)", agent)
		go agent.Close()
	}()

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return "", fmt.Errorf("gocbcore_utils: CBSourceUUIDLookUp,"+
			" unable to fetch agent's config snapshot, err: %v", err)
	}

	return snapshot.BucketUUID(), nil
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

func (a *AuthParams) Certificate(req gocbcore.AuthCertRequest) (
	*tls.Certificate, error) {
	return nil, nil
}

func (a *AuthParams) SupportsTLS() bool {
	return true
}

func (a *AuthParams) SupportsNonTLS() bool {
	return true
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

func (a *AuthParamsSasl) Certificate(req gocbcore.AuthCertRequest) (
	*tls.Certificate, error) {
	return nil, nil
}

func (a *AuthParamsSasl) SupportsTLS() bool {
	return true
}

func (a *AuthParamsSasl) SupportsNonTLS() bool {
	return true
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

func (a *CBAuthenticator) Certificate(req gocbcore.AuthCertRequest) (
	*tls.Certificate, error) {
	return nil, nil
}

func (a *CBAuthenticator) SupportsTLS() bool {
	return true
}

func (a *CBAuthenticator) SupportsNonTLS() bool {
	return true
}

func gocbAuth(sourceParams string, authType string) (
	auth gocbcore.AuthProvider, err error) {
	params := &AuthParams{}

	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), params)
		if err != nil {
			return nil, fmt.Errorf("gocbcore_utils: gocbAuth" +
				" failed to parse sourceParams JSON to CBAuthParams")
		}
	}

	auth = params

	if params.AuthSaslUser != "" {
		auth = &AuthParamsSasl{*params}
	}

	if authType == "cbauth" {
		auth = &CBAuthenticator{}
	}

	return auth, nil
}

// -------------------------------------------------------

type GocbcoreLogger struct {
}

var gocbcoreLogger GocbcoreLogger

func (l GocbcoreLogger) Log(level gocbcore.LogLevel, offset int, format string,
	args ...interface{}) error {
	prefixedFormat := "(GOCBCORE) " + format
	switch level {
	case gocbcore.LogError:
		log.Errorf(prefixedFormat, args...)
	case gocbcore.LogWarn:
		log.Warnf(prefixedFormat, args...)
	case gocbcore.LogInfo:
		log.Printf(prefixedFormat, args...)
	case gocbcore.LogDebug:
		log.Debugf(prefixedFormat, args...)
	default:
		// not logging LogTrace
	}

	return nil
}

func init() {
	gocbcore.SetLogger(gocbcoreLogger)
}
