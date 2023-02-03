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
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/couchbase/cbauth"
	log "github.com/couchbase/clog"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
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

// ----------------------------------------------------------------

type gocbcoreClient struct {
	agent    *gocbcore.Agent
	dcpAgent *gocbcore.DCPAgent

	ref int
}

type gocbcoreAgentsMap struct {
	// mutex to serialize access to entries
	m sync.Mutex
	// map of gocbcore.Agent, gocbcore.DCPAgent instances by bucket <name>:<uuid>
	entries map[string]*gocbcoreClient
	// stat to track number of live agents (connections)
	numAgents uint64
}

var statsAgentsMap *gocbcoreAgentsMap

func init() {
	statsAgentsMap = &gocbcoreAgentsMap{
		entries: make(map[string]*gocbcoreClient),
	}
}

func NumStatsAgents() uint64 {
	if statsAgentsMap != nil {
		return atomic.LoadUint64(&statsAgentsMap.numAgents)
	}

	return 0
}

type certProvider func() *x509.CertPool

// RootCAsProvider supports override capability when authType isn't "cbauth"
// and the application wants to use unique TLS config for feeds.
var RootCAsProvider func(bucketName, bucketUUID string) func() *x509.CertPool

// setupConfigParams sets up the following parameters needed to set up
// gocbcore AgentConfig/DCPAgentConfig ..
//   - connection string
//   - useTLS flag
//   - TLSRootCAProvider
func setupConfigParams(bucketName, bucketUUID, server string, options map[string]string) (
	connStr string, useTLS bool, caProvider certProvider) {
	if options["authType"] == "cbauth" {
		caProvider = FetchRootCAs
	} else {
		if RootCAsProvider != nil {
			caProvider = RootCAsProvider(bucketName, bucketUUID)
		}
	}

	connStr = server
	if connURL, err := url.Parse(server); err == nil {
		if strings.HasPrefix(connURL.Scheme, "http") {
			// tack on an option: bootstrap_on=http for gocbcore SDK
			// connections to force HTTP config polling
			if ret, err := connURL.Parse("?bootstrap_on=http"); err == nil {
				connStr = ret.String()
			}
		}
	}

	if caProvider != nil {
		useTLS = true
	}

	return connStr, useTLS, caProvider
}

// ----------------------------------------------------------------

// Obtains gocbcore agent, dcpAgent instances for the bucket,
// if not found creates a new instance and stashes it in the map,
// before returning it.
func (am *gocbcoreAgentsMap) obtainAgents(sourceName, sourceUUID, sourceParams,
	server string, options map[string]string) (
	*gocbcore.Agent, *gocbcore.DCPAgent, error) {
	am.m.Lock()
	defer am.m.Unlock()

	server, _, sourceName =
		CouchbaseParseSourceName(server, "default", sourceName)

	if entry, exists := am.entries[sourceName]; exists {
		return entry.agent, entry.dcpAgent, nil
	}

	agent, dcpAgent, err := am.createAgentsLOCKED(sourceName, sourceUUID,
		sourceParams, server, options)

	if err == nil {
		log.Printf("gocbcore_utils: obtainAgents, new agents setup"+
			" (Agent: %p, DCPAgent: %p) for sourceName: `%v`, sourceUUID: %v",
			agent, dcpAgent, sourceName, sourceUUID)
	}

	return agent, dcpAgent, err
}

// Increase reference count to gocbcore Agent, DCPAgent instances
// for the sourceName, create new instances if unavailable.
func (am *gocbcoreAgentsMap) registerAgents(sourceName, sourceUUID,
	sourceParams, server string, options map[string]string) {
	am.m.Lock()
	defer am.m.Unlock()

	server, _, sourceName =
		CouchbaseParseSourceName(server, "default", sourceName)

	if entry, exists := am.entries[sourceName]; exists {
		entry.ref++
		log.Printf("gocbcore_utils: registerAgents, ref: %v"+
			" (Agent: %p, DCPAgent: %p) for sourceName: `%v`, sourceUUID: %v",
			entry.ref, entry.agent, entry.dcpAgent, sourceName, sourceUUID)
		return
	}

	agent, dcpAgent, err := am.createAgentsLOCKED(sourceName, sourceUUID, sourceParams,
		server, options)

	if err == nil {
		log.Printf("gocbcore_utils: registerAgents, new agents setup"+
			" (Agent: %p, DCPAgent: %p) for sourceName: `%v`, sourceUUID: %v",
			agent, dcpAgent, sourceName, sourceUUID)
	}
}

// Release a reference to the gocbcore clients, if reference count is down to
// zero, close the gocbcore Agent and DCPAgent instances.
func (am *gocbcoreAgentsMap) releaseAgents(sourceName string) {
	am.m.Lock()
	if entry, exists := am.entries[sourceName]; exists {
		entry.ref--
		if entry.ref > 0 {
			log.Printf("gocbcore_utils: releaseAgents (Agent: %p, DCPAgent: %p), ref: %v,"+
				" sourceName: %v", entry.agent, entry.dcpAgent, entry.ref, sourceName)
		} else {
			log.Printf("gocbcore_utils: releaseAgents, closing (Agent: %p, DCPAgent: %p),"+
				" sourceName: %v", entry.agent, entry.dcpAgent, sourceName)
			go func() {
				entry.agent.Close()
				entry.dcpAgent.Close()
			}()
			delete(am.entries, sourceName)
			atomic.AddUint64(&am.numAgents, ^uint64(1)) // decrement by 2
		}
	}
	am.m.Unlock()
}

func (am *gocbcoreAgentsMap) forceReconnectAgents() {
	am.m.Lock()
	for _, entry := range am.entries {
		go entry.agent.ForceReconnect()
		go entry.dcpAgent.ForceReconnect()
	}
	am.m.Unlock()
}

// ----------------------------------------------------------------

func (am *gocbcoreAgentsMap) createAgentsLOCKED(sourceName, sourceUUID,
	sourceParams, server string, options map[string]string) (
	*gocbcore.Agent, *gocbcore.DCPAgent, error) {
	auth, err := gocbAuth(sourceParams, options["authType"])
	if err != nil {
		return nil, nil, fmt.Errorf("gocbcore_utils: createAgents, gocbAuth,"+
			" sourceName: %s, err: %v", sourceName, err)
	}

	config := setupAgentConfig("stats", sourceName, auth, options)
	dcpConnName := fmt.Sprintf("stats-%s-%x", sourceName, rand.Int31())
	dcpConfig := setupDCPAgentConfig(dcpConnName, sourceName, auth,
		gocbcore.DcpAgentPriorityLow, options)

	svrs := strings.Split(server, ";")
	if len(svrs) == 0 {
		return nil, nil,
			fmt.Errorf("gocbcore_utils: createAgents, no servers provided")
	}

	connStr, useTLS, caProvider := setupConfigParams(sourceName, sourceUUID, svrs[0], options)
	err = config.FromConnStr(connStr)
	if err != nil {
		return nil, nil, fmt.Errorf("gocbcore_utils: createAgents, unable to build"+
			" agent config from connStr: %s, err: %v", connStr, err)
	}
	config.SecurityConfig.UseTLS = useTLS
	config.SecurityConfig.TLSRootCAProvider = caProvider

	err = dcpConfig.FromConnStr(connStr)
	if err != nil {
		return nil, nil, fmt.Errorf("gocbcore_utils: createAgents, unable to build"+
			" dcpAgent config from connStr: %s, err: %v", connStr, err)
	}
	dcpConfig.SecurityConfig.UseTLS = useTLS
	dcpConfig.SecurityConfig.TLSRootCAProvider = caProvider

	agent, err := setupGocbcoreAgent(config)
	if err != nil {
		return nil, nil,
			fmt.Errorf("gocbcore_utils: createAgents (1), setup err: %w", err)
	}

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		log.Warnf("gocbcore_utils: createAgents, ConfigSnapshot err: %v"+
			" (close Agent: %p)",
			err, agent)
		go agent.Close()
		return nil, nil, err
	}

	// if sourceUUID is provided, ensure that it matches with the bucket's UUID
	if len(sourceUUID) > 0 && sourceUUID != snapshot.BucketUUID() {
		go agent.Close()
		return nil, nil, fmt.Errorf("gocbcore_utils: mismatched sourceUUID for"+
			" bucket `%v`", sourceName)
	}

	dcpAgent, err := setupGocbcoreDCPAgent(dcpConfig, dcpConnName, memd.DcpOpenFlagProducer)
	if err != nil {
		log.Warnf("gocbcore_utils: createAgents, setupGocbcoreDCPAgent err: %v"+
			" (close Agent: %p)", err, agent)
		go agent.Close()
		return nil, nil,
			fmt.Errorf("gocbcore_utils: createAgents (2), setup err: %w", err)
	}

	am.entries[sourceName] = &gocbcoreClient{
		agent:    agent,
		dcpAgent: dcpAgent,
		ref:      1,
	}
	atomic.AddUint64(&am.numAgents, 2)

	return agent, dcpAgent, nil
}

// ----------------------------------------------------------------

// CBPartitions parses a sourceParams for a couchbase
// data-source/feed.
func CBPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string, options map[string]string) (
	partitions []string, err error) {
	agent, _, err := statsAgentsMap.obtainAgents(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitions, ConfigSnapshot err: %v", err)
	}

	numVBuckets, err := snapshot.NumVbuckets()
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitions, NumVbuckets err: %v", err)
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
	if options["disableCollectionsSupport"] == "true" {
		return nil,
			fmt.Errorf("CBPartitionSeqs supported only with collections")
	}

	agent, dcpAgent, err := statsAgentsMap.obtainAgents(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil,
			fmt.Errorf("gocbcore_utils: CBPartitionSeqs, fetchClient err: %v", err)
	}

	signal := make(chan error, 1)

	var manifest gocbcore.Manifest
	op, err := agent.GetCollectionManifest(
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
		return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
			" GetCollectionManifest err: %v", err)
	}
	if err = waitForResponse(signal, nil, op, GocbcoreStatsTimeout); err != nil {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
			" GetCollectionManifest failed, err: %v", err)
	}

	configSnapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
			" ConfigSnapshot err: %v", err)
	}

	numServers, err := configSnapshot.NumServers()
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
			" Couldn't determine number of servers in target cluster, err: %v", err)
	}

	numVBuckets, err := configSnapshot.NumVbuckets()
	if err != nil {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
			" Couldn't determine number of vbuckets in target cluster, err: %v", err)
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

	for serverIdx := 1; serverIdx < numServers+1; serverIdx++ {
		for collID := range collectionsIDtoName {
			vbucketSeqnoOptions.FilterOptions.CollectionID = collID
			op, err := dcpAgent.GetVbucketSeqnos(
				serverIdx,               // server index (as per KV)
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
				return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
					" GetVbucketSeqnos err: %v", err)
			}

			if err = waitForResponse(signal, nil, op, GocbcoreStatsTimeout); err != nil {
				return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs,"+
					" GetVbucketSeqnos callback err: %v", err)
			}
		}
	}

	if len(rv) != len(collectionsIDtoName)*numVBuckets {
		return nil, fmt.Errorf("gocbcore_utils: CBPartitionSeqs," +
			" Could not obtain high sequence numbers for all vbuckets")
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
	agent, _, err := statsAgentsMap.obtainAgents(sourceName, sourceUUID,
		sourceParams, serverIn, options)
	if err != nil {
		return nil, err
	}

	signal := make(chan error, 1)
	var rv map[string]interface{}
	op, err := agent.Stats(gocbcore.StatsOptions{Key: statsKind},
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
	config.UserAgent = "CBVBucketLookUp"
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
	agent, _, err := statsAgentsMap.obtainAgents(sourceName, "",
		sourceParams, serverIn, options)
	if err != nil {
		return "", fmt.Errorf("gocbcore_utils: CBSourceUUIDLookUp,"+
			" obtainAgents err: %v", err)
	}

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

	ClientCertPath string `json:"clientCertPath"`
	ClientKeyPath  string `json:"clientKeyPath"`
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

type AuthParamsCert struct {
	AuthParams
}

func (a *AuthParamsCert) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	return []gocbcore.UserPassPair{{}}, nil
}

func (a *AuthParamsCert) Certificate(req gocbcore.AuthCertRequest) (
	*tls.Certificate, error) {
	if len(a.ClientCertPath) > 0 && len(a.ClientKeyPath) > 0 {
		cert, err := tls.LoadX509KeyPair(a.ClientCertPath, a.ClientKeyPath)
		if err != nil {
			return nil, err
		}

		return &cert, nil
	}

	return nil, fmt.Errorf("AuthParamsCert: certPath/keyPath unavailable")
}

func (a *AuthParamsCert) SupportsTLS() bool {
	return true
}

func (a *AuthParamsCert) SupportsNonTLS() bool {
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

	if len(params.ClientCertPath) > 0 && len(params.ClientKeyPath) > 0 {
		auth = &AuthParamsCert{*params}
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
