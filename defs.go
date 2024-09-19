//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/blance"
	log "github.com/couchbase/clog"
)

// JSON/struct definitions of what the Manager stores in the Cfg.
// NOTE: You *must* update VERSION if you change these
// definitions or the planning algorithms change.

// An IndexDefs is zero or more index definitions.
type IndexDefs struct {
	// IndexDefs.UUID changes whenever any child IndexDef changes.
	UUID        string               `json:"uuid"`        // Like a revision id.
	IndexDefs   map[string]*IndexDef `json:"indexDefs"`   // Key is IndexDef.Name.
	ImplVersion string               `json:"implVersion"` // See VERSION.
}

// An IndexDef is a logical index definition.
type IndexDef struct {
	Type            string     `json:"type"` // Ex: "blackhole", etc.
	Name            string     `json:"name"`
	UUID            string     `json:"uuid"` // Like a revision id.
	Params          string     `json:"params"`
	SourceType      string     `json:"sourceType"`
	SourceName      string     `json:"sourceName,omitempty"`
	SourceUUID      string     `json:"sourceUUID,omitempty"`
	SourceParams    string     `json:"sourceParams,omitempty"` // Optional connection info.
	PlanParams      PlanParams `json:"planParams,omitempty"`
	HibernationPath string     `json:"hibernationPath,omitempty"`

	// NOTE: Any auth credentials to access datasource, if any, may be
	// stored as part of SourceParams.
}

// An indexDefBase defines the stable, "non-envelopable" fields of an
// IndexDef.
//
// IMPORTANT!  This must be manually kept in sync with the IndexDef
// struct definition.  If you change IndexDef struct, you must change
// this indexDefBase definition, too; and also see defs_json.go.
type indexDefBase struct {
	Type            string     `json:"type"` // Ex: "blackhole", etc.
	Name            string     `json:"name"`
	UUID            string     `json:"uuid"` // Like a revision id.
	SourceType      string     `json:"sourceType"`
	SourceName      string     `json:"sourceName,omitempty"`
	SourceUUID      string     `json:"sourceUUID,omitempty"`
	PlanParams      PlanParams `json:"planParams,omitempty"`
	HibernationPath string     `json:"hibernationPath,omitempty"`
}

// A PlanParams holds input parameters to the planner, that control
// how the planner should split an index definition into one or more
// index partitions, and how the planner should assign those index
// partitions to nodes.
type PlanParams struct {
	// MaxPartitionsPerPIndex controls the maximum number of source
	// partitions the planner can assign to or clump into a PIndex (or
	// index partition).
	MaxPartitionsPerPIndex int `json:"maxPartitionsPerPIndex,omitempty"`

	// IndexPartitions controls the number of partitions to split
	// the entire index into.
	// IndexPartitions will have higher precedence over MaxPartitionsPerPIndex,
	// as in if both are set to non-zero values, IndexPartitions will
	// override MaxPartitionsPerPIndex with ..
	// SourcePartitions / IndexPartitions.
	IndexPartitions int `json:"indexPartitions,omitempty"`

	// NumReplicas controls the number of replicas for a PIndex, over
	// the first copy.  The first copy is not counted as a replica.
	// For example, a NumReplicas setting of 2 means there should be a
	// primary and 2 replicas... so 3 copies in total.  A NumReplicas
	// of 0 means just the first, primary copy only.
	NumReplicas int `json:"numReplicas,omitempty"`

	// HierarchyRules defines the policy the planner should follow
	// when assigning PIndexes to nodes, especially for replica
	// placement.  Through the HierarchyRules, a user can specify, for
	// example, that the first replica should be not on the same rack
	// and zone as the first copy.  Some examples:
	// Try to put the first replica on the same rack...
	// {"replica":[{"includeLevel":1,"excludeLevel":0}]}
	// Try to put the first replica on a different rack...
	// {"replica":[{"includeLevel":2,"excludeLevel":1}]}
	HierarchyRules blance.HierarchyRules `json:"hierarchyRules,omitempty"`

	// NodePlanParams allows users to specify per-node input to the
	// planner, such as whether PIndexes assigned to different nodes
	// can be readable or writable.  Keyed by node UUID.  Value is
	// keyed by planPIndex.Name or indexDef.Name.  The empty string
	// ("") is used to represent any node UUID and/or any planPIndex
	// and/or any indexDef.
	NodePlanParams map[string]map[string]*NodePlanParam `json:"nodePlanParams,omitempty"`

	// PIndexWeights allows users to specify an optional weight for a
	// PIndex, where weights default to 1.  In a range-partitioned
	// index, for example, some index partitions (or PIndexes) may
	// have more entries (higher weight) than other index partitions.
	PIndexWeights map[string]int `json:"pindexWeights,omitempty"`

	// PlanFrozen means the planner should not change the previous
	// plan for an index, even if as nodes join or leave and even if
	// there was no previous plan.  Defaults to false (allow
	// re-planning).
	PlanFrozen bool `json:"planFrozen,omitempty"`
}

// A NodePlanParam defines whether a particular node can service a
// particular index definition.
type NodePlanParam struct {
	CanRead  bool `json:"canRead"`
	CanWrite bool `json:"canWrite"`
}

// ------------------------------------------------------------------------

// A NodeDefs is comprised of zero or more node definitions.
type NodeDefs struct {
	// NodeDefs.UUID changes whenever any child NodeDef changes.
	UUID        string              `json:"uuid"`        // Like a revision id.
	NodeDefs    map[string]*NodeDef `json:"nodeDefs"`    // Key is NodeDef.UUID.
	ImplVersion string              `json:"implVersion"` // See VERSION.
}

// A NodeDef is a node definition.
type NodeDef struct {
	HostPort    string   `json:"hostPort"`
	UUID        string   `json:"uuid"`
	ImplVersion string   `json:"implVersion"` // See VERSION.
	Tags        []string `json:"tags"`
	Container   string   `json:"container"`
	Weight      int      `json:"weight"`
	Extras      string   `json:"extras"`

	m            sync.Mutex
	extrasParsed map[string]interface{}
}

func (n *NodeDef) GetFromParsedExtras(key string) (interface{}, error) {
	var ret interface{}
	var err error

	n.m.Lock()
	if n.extrasParsed != nil {
		ret = n.extrasParsed[key]
	} else {
		extrasParsed := make(map[string]interface{})

		err = UnmarshalJSON([]byte(n.Extras), &extrasParsed)
		if err == nil {
			n.extrasParsed = extrasParsed
			ret = n.extrasParsed[key]
		}
	}
	n.m.Unlock()

	return ret, err
}

func (n *NodeDef) HttpsURL() (string, error) {
	hostnameDelimiter := strings.LastIndex(n.HostPort, ":")
	if hostnameDelimiter < 0 || hostnameDelimiter > len(n.HostPort) {
		return "", fmt.Errorf("unable to locate hostname")
	}

	bindHTTPS, err := n.GetFromParsedExtras("bindHTTPS")
	if err != nil {
		return "", err
	}

	if bindHTTPS == nil {
		return "", fmt.Errorf("no extras entry for bindHTTPS")
	}

	bindHTTPSstr, ok := bindHTTPS.(string)
	if !ok {
		return "", fmt.Errorf("bindHTTPS not a string")
	}

	portPos := strings.LastIndex(bindHTTPSstr, ":") + 1
	if portPos < 0 || portPos > len(bindHTTPSstr) {
		return "", fmt.Errorf("unable to locate port")
	}

	address := "https://" + n.HostPort[:hostnameDelimiter] +
		":" + bindHTTPSstr[portPos:]

	return address, nil
}

// ------------------------------------------------------------------------

// A PlanPIndexes is comprised of zero or more planPIndexes.
type PlanPIndexes struct {
	// PlanPIndexes.UUID changes whenever any child PlanPIndex changes.
	UUID         string                 `json:"uuid"`         // Like a revision id.
	PlanPIndexes map[string]*PlanPIndex `json:"planPIndexes"` // Key is PlanPIndex.Name.
	ImplVersion  string                 `json:"implVersion"`  // See VERSION.
	Warnings     map[string][]string    `json:"warnings"`     // Key is IndexDef.Name.
}

// A PlanPIndex represents the plan for a particular index partition,
// including on what nodes that the index partition is assigned to.
// An index partition might be assigned to more than one node if the
// "plan params" has a replica count > 0.
type PlanPIndex struct {
	Name             string `json:"name,omitempty"` // Stable & unique cluster wide.
	UUID             string `json:"uuid"`
	IndexType        string `json:"indexType"`             // See IndexDef.Type.
	IndexName        string `json:"indexName"`             // See IndexDef.Name.
	IndexUUID        string `json:"indexUUID"`             // See IndefDef.UUID.
	IndexParams      string `json:"indexParams,omitempty"` // See IndexDef.Params.
	SourceType       string `json:"sourceType"`
	SourceName       string `json:"sourceName,omitempty"`
	SourceUUID       string `json:"sourceUUID,omitempty"`
	SourceParams     string `json:"sourceParams,omitempty"` // Optional connection info.
	SourcePartitions string `json:"sourcePartitions"`
	HibernationPath  string `json:"hibernationPath,omitempty"`
	PlannerVersion   string `json:"plannerVersion,omitempty"`

	Nodes map[string]*PlanPIndexNode `json:"nodes"` // Keyed by NodeDef.UUID.
}

// A planPIndexBase defines the stable, "non-envelopable" fields of a
// PlanPIndex.
//
// IMPORTANT!  This must be manually kept in sync with the PlanPIndex
// struct definition.  If you change PlanPIndex struct, you must change
// this planPIndexBase definition, too; and also see defs_json.go.
type planPIndexBase struct {
	Name             string `json:"name,omitempty"` // Stable & unique cluster wide.
	UUID             string `json:"uuid"`
	IndexType        string `json:"indexType"` // See IndexDef.Type.
	IndexName        string `json:"indexName"` // See IndexDef.Name.
	IndexUUID        string `json:"indexUUID"` // See IndefDef.UUID.
	SourceType       string `json:"sourceType"`
	SourceName       string `json:"sourceName,omitempty"`
	SourceUUID       string `json:"sourceUUID,omitempty"`
	SourcePartitions string `json:"sourcePartitions"`
	HibernationPath  string `json:"hibernationPath,omitempty"`
	PlannerVersion   string `json:"plannerVersion,omitempty"`

	Nodes map[string]*PlanPIndexNode `json:"nodes"` // Keyed by NodeDef.UUID.
}

// A PlanPIndexNode represents the kind of service a node has been
// assigned to provide for an index partition.
type PlanPIndexNode struct {
	CanRead  bool `json:"canRead"`
	CanWrite bool `json:"canWrite"`
	Priority int  `json:"priority"` // Lower is higher priority, 0 is highest.
}

// PlanPIndexNodeCanRead returns true if PlanPIndexNode.CanRead is
// true; it's useful as a filter arg for Manager.CoveringPIndexes().
func PlanPIndexNodeCanRead(p *PlanPIndexNode) bool {
	return p != nil && p.CanRead
}

// PlanPIndexNodeCanWrite returns true if PlanPIndexNode.CanWrite is
// true; it's useful as a filter arg for Manager.CoveringPIndexes().
func PlanPIndexNodeCanWrite(p *PlanPIndexNode) bool {
	return p != nil && p.CanWrite
}

// PlanPIndexNodeOk always returns true; it's useful as a filter arg
// for Manager.CoveringPIndexes().
func PlanPIndexNodeOk(p *PlanPIndexNode) bool {
	return true
}

// ------------------------------------------------------------------------

// INDEX_DEFS_KEY is the key used for Cfg access.
const INDEX_DEFS_KEY = "indexDefs"

// Returns an intiialized IndexDefs.
func NewIndexDefs(version string) *IndexDefs {
	return &IndexDefs{
		UUID:        NewUUID(),
		IndexDefs:   make(map[string]*IndexDef),
		ImplVersion: version,
	}
}

// Returns index definitions from a Cfg provider.
func CfgGetIndexDefs(cfg Cfg) (*IndexDefs, uint64, error) {
	v, cas, err := cfg.Get(INDEX_DEFS_KEY, 0)
	if err != nil {
		return nil, cas, err
	}
	if v == nil {
		return nil, cas, nil
	}
	rv := &IndexDefs{}
	err = UnmarshalJSON(v, rv)
	if err != nil {
		return nil, cas, err
	}
	return rv, cas, nil
}

// Updates index definitions on a Cfg provider.
func CfgSetIndexDefs(cfg Cfg, indexDefs *IndexDefs, cas uint64) (uint64, error) {
	buf, err := MarshalJSON(indexDefs)
	if err != nil {
		return 0, err
	}
	return cfg.Set(INDEX_DEFS_KEY, buf, cas)
}

// ------------------------------------------------------------------------

// GetNodePlanParam returns a relevant NodePlanParam for a given node
// from a nodePlanParams, defaulting to a less-specific NodePlanParam
// if needed.
func GetNodePlanParam(nodePlanParams map[string]map[string]*NodePlanParam,
	nodeUUID, indexDefName, planPIndexName string) *NodePlanParam {
	var nodePlanParam *NodePlanParam
	if nodePlanParams != nil {
		m := nodePlanParams[nodeUUID]
		if m == nil {
			m = nodePlanParams[""]
		}
		if m != nil {
			nodePlanParam = m[indexDefName]
			if nodePlanParam == nil {
				nodePlanParam = m[planPIndexName]
			}
			if nodePlanParam == nil {
				nodePlanParam = m[""]
			}
		}
	}
	return nodePlanParam
}

// ------------------------------------------------------------------------

const NODE_DEFS_KEY = "nodeDefs"  // NODE_DEFS_KEY is used for Cfg access.
const NODE_DEFS_KNOWN = "known"   // NODE_DEFS_KNOWN is used for Cfg access.
const NODE_DEFS_WANTED = "wanted" // NODE_DEFS_WANTED is used for Cfg access.

// Returns an initialized NodeDefs.
func NewNodeDefs(version string) *NodeDefs {
	return &NodeDefs{
		UUID:        NewUUID(),
		NodeDefs:    make(map[string]*NodeDef),
		ImplVersion: version,
	}
}

// CfgGetVersion returns the Cfg version
func CfgGetVersion(cfg Cfg) string {
	v, _, err := cfg.Get(VERSION_KEY, 0)
	if err != nil || v == nil {
		log.Warnf("defs: err: %v, Cfg version: %v, using VERSION", err, v)
		return VERSION
	}
	return string(v)
}

// CfgGetClusterOptions returns the cluster level options
func CfgGetClusterOptions(cfg Cfg) (*ClusterOptions, uint64, error) {
	v, cas, err := cfg.Get(MANAGER_CLUSTER_OPTIONS_KEY, 0)
	if err != nil || v == nil {
		return nil, cas, err
	}
	rv := &ClusterOptions{}
	err = UnmarshalJSON(v, rv)
	if err != nil {
		return nil, cas, err
	}

	return rv, cas, nil
}

// CfgSetClusterOptions sets the cluster level options
func CfgSetClusterOptions(cfg Cfg, options *ClusterOptions,
	cas uint64) (uint64, error) {
	buf, err := MarshalJSON(options)
	if err != nil {
		return 0, err
	}
	return cfg.Set(MANAGER_CLUSTER_OPTIONS_KEY, buf, cas)
}

// CfgNodeDefsKey returns the Cfg access key for a NodeDef kind.
func CfgNodeDefsKey(kind string) string {
	return NODE_DEFS_KEY + "-" + kind
}

// Retrieves node definitions from a Cfg provider.
func CfgGetNodeDefs(cfg Cfg, kind string) (*NodeDefs, uint64, error) {
	v, cas, err := cfg.Get(CfgNodeDefsKey(kind), 0)
	if err != nil {
		return nil, cas, err
	}
	if v == nil {
		return nil, cas, nil
	}
	rv := &NodeDefs{}
	err = UnmarshalJSON(v, rv)
	if err != nil {
		return nil, cas, err
	}
	return rv, cas, nil
}

// Updates node definitions on a Cfg provider.
func CfgSetNodeDefs(cfg Cfg, kind string, nodeDefs *NodeDefs,
	cas uint64) (uint64, error) {
	buf, err := MarshalJSON(nodeDefs)
	if err != nil {
		return 0, err
	}
	return cfg.Set(CfgNodeDefsKey(kind), buf, cas)
}

// CfgRemoveNodeDef removes a NodeDef with the given uuid from the Cfg.
func CfgRemoveNodeDef(cfg Cfg, kind, uuid, version string) error {
	return removeNodeDef(cfg, kind, uuid, version, false)
}

// CfgRemoveNodeDefForce removes a NodeDef with the given uuid
// from the Cfg ignoring the cas checks.
func CfgRemoveNodeDefForce(cfg Cfg, kind, uuid, version string) error {
	return removeNodeDef(cfg, kind, uuid, version, true)
}

func removeNodeDef(cfg Cfg, kind, uuid, version string, force bool) error {
	nodeDefs, cas, err := CfgGetNodeDefs(cfg, kind)
	if err != nil {
		return err
	}

	if nodeDefs == nil {
		return nil
	}

	nodeDefPrev, exists := nodeDefs.NodeDefs[uuid]
	if !exists || nodeDefPrev == nil {
		return nil
	}

	delete(nodeDefs.NodeDefs, uuid)

	nodeDefs.UUID = NewUUID()
	nodeDefs.ImplVersion = version

	if force {
		cas = CFG_CAS_FORCE
	}

	_, err = CfgSetNodeDefs(cfg, kind, nodeDefs, cas)

	return err
}

// ------------------------------------------------------------------------

// UnregisterNodes removes the given nodes (by their UUID) from the
// nodes wanted & known cfg entries.
func UnregisterNodes(cfg Cfg, version string, nodeUUIDs []string) error {
	return UnregisterNodesWithRetries(cfg, version, nodeUUIDs, 10)
}

// UnregisterNodesWithRetries removes the given nodes (by their UUID)
// from the nodes wanted & known cfg entries, and performs retries a
// max number of times if there were CAS conflict errors.
func UnregisterNodesWithRetries(cfg Cfg, version string, nodeUUIDs []string,
	maxTries int) error {
	for _, nodeUUID := range nodeUUIDs {
		for _, kind := range []string{NODE_DEFS_WANTED, NODE_DEFS_KNOWN} {
		LOOP_TRIES:
			for tries := 0; tries < maxTries; tries++ {
				err := CfgRemoveNodeDef(cfg, kind, nodeUUID, version)
				if err != nil {
					if _, ok := err.(*CfgCASError); ok {
						continue LOOP_TRIES
					}

					return fmt.Errorf("defs: UnregisterNodes,"+
						" nodeUUID: %s, kind: %s, tries; %d, err: %v",
						nodeUUID, kind, tries, err)
				}

				break LOOP_TRIES // Success.
			}
		}
	}

	return nil
}

// ------------------------------------------------------------------------

// PLAN_PINDEXES_KEY is used for Cfg access.
const PLAN_PINDEXES_KEY = "planPIndexes"

const PLAN_PINDEXES_DIRECTORY_STAMP = "curMetaKvPlanKey"

// Check pointing of the rebalance status
// To be updated by rebalance orchestrator
// To be consumed by all nodes to cache the status of last rebalance operation
//
// This is helpful in case of nodes dieing during rebalance (orchestrator
// and non-orchestrator). In such a case, ns_server sends the CancelTask request
// to the nodes that are still alive. After which it will periodically send
// GetCurrentTopology request to all nodes to check if there is a need of rebalance.
// To answer which, the nodes can use the cached status of last rebalance operation.
const LAST_REBALANCE_STATUS_KEY = "lastRebalanceStatusKey"

// Values for LAST_REBALANCE_STATUS_KEY
type LastRebalanceStatus uint

const (
	RebNoRecord LastRebalanceStatus = iota

	RebStarted
	RebCompleted // Rebalance completed successfully (without any errors)
)

func CfgGetLastRebalanceStatus(cfg Cfg) (LastRebalanceStatus, uint64, error) {
	v, cas, err := cfg.Get(LAST_REBALANCE_STATUS_KEY, 0)
	if err != nil || v == nil {
		return RebNoRecord, cas, err
	}
	var rv LastRebalanceStatus
	err = UnmarshalJSON(v, &rv)
	return rv, cas, err
}

func CfgSetLastRebalanceStatus(cfg Cfg, status LastRebalanceStatus,
	cas uint64) (uint64, error) {
	buf, err := MarshalJSON(status)
	if err != nil {
		return 0, err
	}
	return cfg.Set(LAST_REBALANCE_STATUS_KEY, buf, cas)
}

// Returns the current(i.e. the most recent) planner version.
func GetPlannerVersion() string {
	return plannerVersion
}

// Returns an initialized PlanPIndexes.
func NewPlanPIndexes(version string) *PlanPIndexes {
	return &PlanPIndexes{
		UUID:         NewUUID(),
		PlanPIndexes: make(map[string]*PlanPIndex),
		ImplVersion:  version,
		Warnings:     make(map[string][]string),
	}
}

// CopyPlanPIndexes returns a copy of the given planPIndexes, albeit
// with a new UUID and given version.
func CopyPlanPIndexes(planPIndexes *PlanPIndexes,
	version string) *PlanPIndexes {
	r := NewPlanPIndexes(version)
	j, _ := MarshalJSON(planPIndexes)
	UnmarshalJSON(j, r)
	r.UUID = NewUUID()
	r.ImplVersion = version
	return r
}

// Copy plans iff their planner version is the current.
func CopyPlanPIndexesWithSameVersion(planPIndexes *PlanPIndexes,
	version, plannerVersion string) *PlanPIndexes {
	r := NewPlanPIndexes(version)
	for name, plan := range planPIndexes.PlanPIndexes {
		if plan.PlannerVersion == plannerVersion {
			r.PlanPIndexes[name] = plan
		}
	}
	r.UUID = planPIndexes.UUID
	r.ImplVersion = planPIndexes.ImplVersion
	r.Warnings = planPIndexes.Warnings
	return r
}

// Retrieves PlanPIndexes from a Cfg provider.
func CfgGetPlanPIndexes(cfg Cfg) (*PlanPIndexes, uint64, error) {
	v, cas, err := cfg.Get(PLAN_PINDEXES_KEY, 0)
	if err != nil {
		return nil, cas, err
	}
	if v == nil {
		return nil, cas, nil
	}
	rv := &PlanPIndexes{}
	err = UnmarshalJSON(v, rv)
	if err != nil {
		return nil, cas, err
	}
	return rv, cas, nil
}

// Updates PlanPIndexes on a Cfg provider.
func CfgSetPlanPIndexes(cfg Cfg, planPIndexes *PlanPIndexes, cas uint64) (
	uint64, error) {
	buf, err := MarshalJSON(planPIndexes)
	if err != nil {
		return 0, err
	}
	return cfg.Set(PLAN_PINDEXES_KEY, buf, cas)
}

// Returns true if both PlanPIndexes are the same, where we ignore any
// differences in UUID, ImplVersion or PlannerVersion.
func SamePlanPIndexes(a, b *PlanPIndexes) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a.PlanPIndexes) != len(b.PlanPIndexes) {
		return false
	}
	return SubsetPlanPIndexes(a, b) && SubsetPlanPIndexes(b, a)
}

// Returns true if PlanPIndex children in a are a subset of those in
// b, using SamePlanPIndex() for sameness comparion.
func SubsetPlanPIndexes(a, b *PlanPIndexes) bool {
	for name, av := range a.PlanPIndexes {
		bv, exists := b.PlanPIndexes[name]
		if !exists {
			return false
		}
		if !SamePlanPIndex(av, bv) {
			return false
		}
	}
	return true
}

// Returns true if both PlanPIndex are the same, ignoring PlanPIndex.UUID.
func SamePlanPIndex(a, b *PlanPIndex) bool {
	// Of note, we don't compare UUID's.
	if a.Name != b.Name ||
		a.IndexType != b.IndexType ||
		a.IndexName != b.IndexName ||
		a.IndexUUID != b.IndexUUID ||
		a.IndexParams != b.IndexParams ||
		a.SourceType != b.SourceType ||
		a.SourceName != b.SourceName ||
		a.SourceUUID != b.SourceUUID ||
		a.SourceParams != b.SourceParams ||
		a.SourcePartitions != b.SourcePartitions ||
		!reflect.DeepEqual(a.Nodes, b.Nodes) {
		return false
	}
	return true
}

// Returns true if both the PIndex meets the PlanPIndex, ignoring UUID.
func PIndexMatchesPlan(pindex *PIndex, planPIndex *PlanPIndex) bool {
	same := pindex.Name == planPIndex.Name &&
		pindex.IndexType == planPIndex.IndexType &&
		pindex.IndexName == planPIndex.IndexName &&
		pindex.IndexUUID == planPIndex.IndexUUID &&
		pindex.IndexParams == planPIndex.IndexParams &&
		pindex.SourceType == planPIndex.SourceType &&
		pindex.SourceName == planPIndex.SourceName &&
		pindex.SourceUUID == planPIndex.SourceUUID &&
		pindex.SourceParams == planPIndex.SourceParams &&
		pindex.SourcePartitions == planPIndex.SourcePartitions
	return same
}

// ------------------------------------------------------------------------

func NewPlanParams(mgr *Manager) PlanParams {
	ps := IndexPartitionSettings(mgr)

	return PlanParams{
		MaxPartitionsPerPIndex: ps.MaxPartitionsPerPIndex,
		IndexPartitions:        ps.IndexPartitions,
	}
}

// ------------------------------------------------------------------------

// DefaultMaxPartitionsPerPIndex retrieves "defaultMaxPartitionsPerPIndex"
// from the manager options, if available.
func DefaultMaxPartitionsPerPIndex(mgr *Manager) int {
	maxPartitionsPerPIndex := 20

	options := mgr.Options()
	if options != nil {
		v, ok := options["defaultMaxPartitionsPerPIndex"]
		if ok {
			i, err := strconv.Atoi(v)
			if err == nil && i >= 0 {
				maxPartitionsPerPIndex = i
			}
		}
	}

	return maxPartitionsPerPIndex
}

type PartitionSettings struct {
	MaxPartitionsPerPIndex int
	IndexPartitions        int
}

// IndexPartitionSettings returns the settings to be used for
// MaxPartitionsPerPIndex and IndexPartitions.
func IndexPartitionSettings(mgr *Manager) *PartitionSettings {
	maxPartitionsPerPIndex := DefaultMaxPartitionsPerPIndex(mgr)

	var indexPartitions int
	var sourcePartitions int

	options := mgr.Options()
	if options != nil {
		v, ok := options["indexPartitions"]
		if ok {
			i, err := strconv.Atoi(v)
			if err == nil && i > 0 {
				indexPartitions = i
			}
		}
		v, ok = options["sourcePartitions"]
		if ok {
			i, err := strconv.Atoi(v)
			if err == nil && i >= 0 {
				sourcePartitions = i
			}
		}
	}

	if indexPartitions > 0 && sourcePartitions > 0 {
		maxPartitionsPerPIndex =
			int(math.Ceil(float64(sourcePartitions) / float64(indexPartitions)))
	}

	return &PartitionSettings{
		MaxPartitionsPerPIndex: maxPartitionsPerPIndex,
		IndexPartitions:        indexPartitions,
	}
}

// ------------------------------------------------------------------------

// IsFeatureSupportedByCluster checks whether the given feature is
// supported across the cluster/given NodeDefs
func IsFeatureSupportedByCluster(feature string, nodeDefs *NodeDefs) bool {
	if nodeDefs == nil ||
		(nodeDefs != nil && len(nodeDefs.NodeDefs) == 0) {
		return false
	}
	for _, v1 := range nodeDefs.NodeDefs {
		featureEnabled := false
		if v1.Extras != "" {
			extras := map[string]string{}
			err := UnmarshalJSON([]byte(v1.Extras), &extras)
			if err != nil {
				return false
			}

			features := strings.Split(extras["features"], ",")
			for _, f := range features {
				if f == feature {
					featureEnabled = true
					break
				}
			}
		}
		if !featureEnabled {
			return false
		}
	}
	return true
}
