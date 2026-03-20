//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	log "github.com/couchbase/clog"
	fsutil "github.com/couchbase/tools-common/fs/util"
)

const PINDEX_META_FILENAME string = "PINDEX_META"
const pindexPathSuffix string = ".pindex"

// A PIndex represents a partition of an index, or an "index
// partition".  A logical index definition will be split into one or
// more pindexes.
type PIndex struct {
	Name             string     `json:"name"`
	UUID             string     `json:"uuid"`
	IndexType        string     `json:"indexType"`
	IndexName        string     `json:"indexName"`
	IndexUUID        string     `json:"indexUUID"`
	IndexParams      string     `json:"indexParams"`
	SourceType       string     `json:"sourceType"`
	SourceName       string     `json:"sourceName"`
	SourceUUID       string     `json:"sourceUUID"`
	SourceParams     string     `json:"sourceParams"`
	SourcePartitions string     `json:"sourcePartitions"`
	HibernationPath  string     `json:"hibernationPath"`
	Path             string     `json:"-"` // Transient, not persisted.
	Impl             PIndexImpl `json:"-"` // Transient, not persisted.
	Dest             Dest       `json:"-"` // Transient, not persisted.

	sourcePartitionsMap map[string]bool // Non-persisted memoization.

	m      sync.Mutex
	closed bool
}

// Note that these callbacks are invoked within the manager's sync mutex
// context, it is the responsibility of the user to ensure that they do NOT
// reacquire the manager mutex or any api that does within the callbacks.
type PIndexCallbacks struct {
	OnCreate  func(name string)
	OnDelete  func(name string)
	OnRefresh func()
}

var RegisteredPIndexCallbacks = PIndexCallbacks{}

// Close down a pindex, optionally removing its stored files.
func (p *PIndex) Close(remove bool) error {
	p.m.Lock()
	if p.closed {
		p.m.Unlock()
		return nil
	}

	p.closed = true
	p.m.Unlock()

	log.Printf("pindex: %s Close started with remove: %v", p.Name, remove)

	if p.Dest != nil {
		err := p.Dest.Close(remove)
		if err != nil {
			log.Errorf("pindex: %s Close failed, err: %v", p.Name, err)
			return err
		}
	}

	log.Printf("pindex: %s Close completed successfully", p.Name)
	return nil
}

// IsFeedable checks whether the pindex is ready to
// ingest data from a feed.
func (p *PIndex) IsFeedable() (bool, error) {
	if pa, ok := p.Dest.(Feedable); ok {
		return pa.IsFeedable()
	}
	return true, nil
}

// Clone clones the current PIndex
func (p *PIndex) Clone() *PIndex {
	if p != nil {
		p.m.Lock()
		pi := &PIndex{
			Name:                p.Name,
			UUID:                p.UUID,
			IndexName:           p.IndexName,
			IndexParams:         p.IndexParams,
			IndexType:           p.IndexType,
			IndexUUID:           p.IndexUUID,
			SourceType:          p.SourceType,
			SourceName:          p.SourceName,
			SourceUUID:          p.SourceUUID,
			SourceParams:        p.SourceParams,
			SourcePartitions:    p.SourcePartitions,
			sourcePartitionsMap: p.sourcePartitionsMap,
			Path:                p.Path,
			Impl:                p.Impl,
			Dest:                p.Dest,
			closed:              p.closed,
		}
		p.m.Unlock()
		return pi
	}
	return nil
}

var ErrTerminatedDownload = fmt.Errorf("pindex: case of abruptly terminated download")

func createNewPIndex(mgr *Manager, name, uuid, indexType, indexName, indexUUID, indexParams,
	sourceType, sourceName, sourceUUID, sourceParams, sourcePartitions string,
	path string, createPIndexCB func(indexType, indexParams, sourceParams,
		path string, mgr *Manager, restart func()) (PIndexImpl, Dest, error)) (*PIndex, error) {

	var pindex *PIndex

	rollback := func() {
		log.Printf("pindex: rollbackPIndex starts for pindex: %s", pindex.Name)
		go mgr.JanitorRollbackKick("rollback:"+pindex.Name, pindex)
	}

	params := IndexPrepParams{SourceName: sourceName, IndexName: indexName, Params: indexParams}

	pBytes, err := MarshalJSON(&params)
	if err != nil {
		return nil, fmt.Errorf("pindex: RollbackPIndex, json marshal err: %v", err)
	}

	if mgr != nil && len(mgr.dataDir) > 0 {
		// Creating a directory to store the PINDEX_META file.
		log.Printf("pindex: creating directory at %s", path)
		err = fsutil.Mkdir(path, 0700, true, true)
		if err != nil {
			return nil, fmt.Errorf("pindex: could not create path %s: %#v",
				path, err)
		}
	}

	err = verifySourceExists(mgr, sourceName, sourceUUID, sourceParams)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSourceDoesNotExist, err)
	}

	impl, dest, err := createPIndexCB(indexType, string(pBytes), sourceParams, path, mgr, rollback)
	if err != nil && err != ErrTerminatedDownload {
		os.RemoveAll(path)
		return nil, fmt.Errorf("pindex: new indexType: %s, indexParams: %s,"+
			" path: %s, err: %w", indexType, indexParams, path, err)
	}

	pindex = &PIndex{
		Name:             name,
		UUID:             uuid,
		IndexType:        indexType,
		IndexName:        indexName,
		IndexUUID:        indexUUID,
		IndexParams:      indexParams,
		SourceType:       sourceType,
		SourceName:       sourceName,
		SourceUUID:       sourceUUID,
		SourceParams:     sourceParams,
		SourcePartitions: sourcePartitions,
		Path:             path,
		Impl:             impl,
		Dest:             dest,
	}
	pindex.sourcePartitionsMap = map[string]bool{}
	for _, partition := range strings.Split(sourcePartitions, ",") {
		pindex.sourcePartitionsMap[partition] = true
	}

	// persist PINDEX_META only if manager's dataDir is set
	if mgr != nil && len(mgr.dataDir) > 0 {
		buf, err := MarshalJSON(pindex)
		if err != nil {
			dest.Close(true)
			return nil, err
		}

		_, err = mgr.EncryptAndWriteFile(
			path+string(os.PathSeparator)+PINDEX_META_FILENAME, buf, 0600)
		if err != nil {
			dest.Close(true)
			return nil, fmt.Errorf("pindex: could not save PINDEX_META_FILENAME,"+
				" path: %s, err: %v", path, err)
		}
	}

	return pindex, nil
}

// Creates a pindex, including its backend implementation structures,
// and its files.
func NewPIndex(mgr *Manager, name, uuid,
	indexType, indexName, indexUUID, indexParams,
	sourceType, sourceName, sourceUUID, sourceParams, sourcePartitions string,
	path string) (*PIndex, error) {

	return createNewPIndex(mgr, name, uuid, indexType, indexName, indexUUID, indexParams,
		sourceType, sourceName, sourceUUID, sourceParams, sourcePartitions, path, NewPIndexImplEx)
}

var ErrSourceDoesNotExist = errors.New("source does not exist")

// OpenPIndex reopens a previously created pindex
// The path argument must be a directory for the pindex
// If options has an updated mapping, an index update is attempted
func OpenPIndex(mgr *Manager, path string, options map[string]interface{}) (pindex *PIndex, err error) {
	pindex = &PIndex{}
	// load PINDEX_META only if manager's dataDir is set
	if mgr != nil && len(mgr.dataDir) > 0 {
		buf, _, err := mgr.DecryptAndReadFile(
			path + string(os.PathSeparator) + PINDEX_META_FILENAME)
		if err != nil {
			return nil, fmt.Errorf("pindex: could not load PINDEX_META_FILENAME,"+
				" path: %s, err: %v", path, err)
		}

		err = UnmarshalJSON(buf, pindex)
		if err != nil {
			return nil, fmt.Errorf("pindex: could not parse pindex json,"+
				" path: %s, err: %v", path, err)
		}
	}

	rollback := func() {
		log.Printf("pindex: rollbackPIndex starts for pindex: %s", pindex.Name)
		go mgr.JanitorRollbackKick("rollback:"+pindex.Name, pindex)
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pindex: OpenPIndex panic msg: %v \n, %v",
				r, ReadableStackTrace())
		}
	}()

	err = verifySourceExists(mgr, pindex.SourceName, pindex.SourceUUID, pindex.SourceParams)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSourceDoesNotExist, err)
	}

	if options == nil {
		options = make(map[string]interface{})
	}
	options["indexParams"] = pindex.IndexParams
	impl, dest, err := OpenPIndexImplUsing(pindex.IndexType, path, rollback, options)
	if err != nil {
		return nil, fmt.Errorf("pindex: could not open indexType: %s,"+
			" path: %s, err: %v", pindex.IndexType, path, err)
	}

	pindex.Path = path
	pindex.Impl = impl
	pindex.Dest = dest

	pindex.sourcePartitionsMap = map[string]bool{}
	for _, partition := range strings.Split(pindex.SourcePartitions, ",") {
		pindex.sourcePartitionsMap[partition] = true
	}

	return pindex, nil
}

type ScopeParams struct {
	Name        string             `json:"name"`
	Collections []CollectionParams `json:"collections"`
}

type CollectionParams struct {
	Name string `json:"name"`
	Uid  string `json:"uid"`
}

func verifySourceExists(mgr *Manager, sourceName, sourceUUID, sourceParams string) error {
	if mgr == nil {
		return nil
	}

	resp, err := GetPoolsDefaultForBucket(mgr.Server(), sourceName, false)
	if err != nil {
		log.Warnf("unable to fetch bucket manifest %s, err: %v", sourceName, err)
		return nil
	}

	if isResponseEquivalentToResourceNotFound(string(resp)) {
		return fmt.Errorf("bucket %s not found", sourceName)
	}

	if sourceUUID == "" {
		return nil
	}

	uuidResp := struct {
		UUID string `json:"uuid"`
	}{}
	err = UnmarshalJSON(resp, &uuidResp)
	if err != nil {
		log.Warnf("unable to parse bucket UUID from response %s, err: %v", resp, err)
		return nil
	}

	if sourceUUID != uuidResp.UUID {
		return fmt.Errorf("bucket %s UUID mismatch, expected %s, got %s",
			sourceName, uuidResp.UUID, sourceUUID)
	}

	resp, err = GetPoolsDefaultForBucket(mgr.Server(), sourceName, true)
	if err != nil {
		log.Warnf("unable to fetch bucket manifest %s, err: %v", sourceName, err)
		return nil
	}

	if isResponseEquivalentToResourceNotFound(string(resp)) {
		return fmt.Errorf("bucket %s not found", sourceName)
	}

	if sourceParams == "" || sourceParams == "null" {
		return nil
	}

	var scopeParams *ScopeParams
	err = UnmarshalJSON([]byte(sourceParams), &scopeParams)
	if err != nil {
		return fmt.Errorf("pindex: could not parse sourceParams: %s, err: %v", sourceParams, err)
	}

	if scopeParams.Name == "" {
		return nil
	}

	var m manifest
	if err = m.UnmarshalJSON(resp); err != nil {
		log.Warnf("unable to parse bucket manifest %s, err: %v", sourceName, err)
		return nil
	}

	found := false
	for _, s := range m.scopes {
		if s.name == scopeParams.Name {
			found = true

		OUTER:
			for i := range scopeParams.Collections {
				for _, coll := range s.collections {
					if coll.name == scopeParams.Collections[i].Name {
						collectionUID, err := strconv.ParseUint(scopeParams.Collections[i].Uid, 16, 32)
						if err != nil {
							return err
						}
						if coll.uid != uint32(collectionUID) {
							return fmt.Errorf("collection %s UID mismatch in scope %s in bucket %s, expected %d, got %d",
								scopeParams.Collections[i], scopeParams.Name, sourceName, collectionUID, coll.uid)
						}
						continue OUTER
					}
				}

				return fmt.Errorf("collection %s not found in scope %s in bucket %s",
					scopeParams.Collections[i], scopeParams.Name, sourceName)
			}
		}
	}
	if !found {
		return fmt.Errorf("scope %s not found in bucket %s", scopeParams.Name, sourceName)
	}

	return nil
}

// Computes the storage path for a pindex.
func PIndexPath(dataDir, pindexName string) string {
	// TODO: Need path security checks / mapping here; ex: "../etc/pswd"
	return dataDir + string(os.PathSeparator) + pindexName + pindexPathSuffix
}

// Computes the PIndex name from the storage path.
func PIndexNameFromPath(path string) string {
	path = filepath.Base(path)
	return path[0 : len(path)-len(pindexPathSuffix)]
}

// Retrieves a pindex name from a pindex path.
func ParsePIndexPath(dataDir, pindexPath string) (string, bool) {
	if !strings.HasSuffix(pindexPath, pindexPathSuffix) {
		return "", false
	}
	prefix := dataDir + string(os.PathSeparator)
	if !strings.HasPrefix(pindexPath, prefix) {
		return "", false
	}
	pindexName := pindexPath[len(prefix):]
	pindexName = pindexName[0 : len(pindexName)-len(pindexPathSuffix)]
	return pindexName, true
}

func ParsePIndexPathUUID(dataDir, pindexPath string) (string, bool) {
	prefix := dataDir + string(os.PathSeparator)
	if !strings.HasPrefix(pindexPath, prefix) {
		return "", false
	}
	path := pindexPath[len(prefix):]
	if len(path) != 16 {
		return "", false
	}
	return path, true
}

// ---------------------------------------------------------

// RemotePlanPIndex associations are returned by CoveringPIndexes().
type RemotePlanPIndex struct {
	PlanPIndex *PlanPIndex
	NodeDef    *NodeDef
}

// PlanPIndexFilter is used to filter out nodes being considered by
// CoveringPIndexes().
type PlanPIndexFilter func(*PlanPIndexNode) bool

// CoveringPIndexesSpec represent the arguments for computing the
// covering pindexes for an index.  See also CoveringPIndexesEx().
type CoveringPIndexesSpec struct {
	IndexName            string
	IndexUUID            string
	PlanPIndexFilterName string // See PlanPIndexesFilters.
	PartitionSelection   string // See QueryCtl.
}

// CoveringPIndexes represents a non-overlapping, disjoint set of
// PIndexes that cover all the partitions of an index.
type CoveringPIndexes struct {
	LocalPIndexes      []*PIndex
	RemotePlanPIndexes []*RemotePlanPIndex
	MissingPIndexNames []string
}

// PlanPIndexFilters represent registered PlanPIndexFilter func's, and
// should only be modified during process init()'ialization.
var PlanPIndexFilters = map[string]PlanPIndexFilter{
	"ok":      PlanPIndexNodeOk,
	"canRead": PlanPIndexNodeCanRead,
}

// ---------------------------------------------------------

// CoveringPIndexes returns a non-overlapping, disjoint set (or cut)
// of PIndexes (either local or remote) that cover all the partitons
// of an index so that the caller can perform scatter/gather queries,
// etc.  Only PlanPIndexes on wanted nodes that pass the
// planPIndexFilter filter will be returned.
//
// TODO: Perhaps need a tighter check around indexUUID, as the current
// implementation might have a race where old pindexes with a matching
// (but outdated) indexUUID might be chosen.
//
// TODO: This implementation currently always favors the local node's
// pindex, but should it?  Perhaps a remote node is more up-to-date
// than the local pindex?
//
// TODO: We should favor the most up-to-date node rather than
// the first one that we run into here?  But, perhaps the most
// up-to-date node is also the most overloaded?  Or, perhaps
// the planner may be trying to rebalance away the most
// up-to-date node and hitting it with load just makes the
// rebalance take longer?
func (mgr *Manager) CoveringPIndexes(indexName, indexUUID string,
	planPIndexFilter PlanPIndexFilter, wantKind string) (
	localPIndexes []*PIndex,
	remotePlanPIndexes []*RemotePlanPIndex,
	err error) {
	var missingPIndexNames []string

	localPIndexes, remotePlanPIndexes, missingPIndexNames, err =
		mgr.CoveringPIndexesEx(CoveringPIndexesSpec{
			IndexName: indexName,
			IndexUUID: indexUUID,
		}, planPIndexFilter, false)
	if err == nil && len(missingPIndexNames) > 0 {
		return nil, nil, fmt.Errorf("pindex:"+
			" %s may have been disabled; no nodes are enabled/allocated"+
			" to serve %s for the index partition(s)",
			wantKind, wantKind)
	}

	return localPIndexes, remotePlanPIndexes, err
}

// CoveringPIndexesBestEffort is similar to CoveringPIndexes, but does
// not error if there are missing/disabled nodes for some of the
// pindexes.
func (mgr *Manager) CoveringPIndexesBestEffort(indexName, indexUUID string,
	planPIndexFilter PlanPIndexFilter, wantKind string) (
	localPIndexes []*PIndex,
	remotePlanPIndexes []*RemotePlanPIndex,
	missingPIndexNames []string,
	err error) {
	return mgr.CoveringPIndexesEx(CoveringPIndexesSpec{
		IndexName: indexName,
		IndexUUID: indexUUID,
	}, planPIndexFilter, false)
}

// CoveringPIndexesEx returns a non-overlapping, disjoint set (or cut)
// of PIndexes (either local or remote) that cover all the partitons
// of an index so that the caller can perform scatter/gather queries.
//
// If the planPIndexFilter param is nil, then the
// spec.PlanPIndexFilterName is used.
func (mgr *Manager) CoveringPIndexesEx(spec CoveringPIndexesSpec,
	planPIndexFilter PlanPIndexFilter, noCache bool) (
	[]*PIndex, []*RemotePlanPIndex, []string, error) {
	ppf := planPIndexFilter
	if ppf == nil {
		if !noCache {
			var cp *CoveringPIndexes

			mgr.m.RLock()
			if mgr.coveringCache != nil {
				cp = mgr.coveringCache[spec]
			}
			mgr.m.RUnlock()

			if cp != nil {
				return cp.LocalPIndexes, cp.RemotePlanPIndexes, cp.MissingPIndexNames, nil
			}
		}

		ppf = PlanPIndexFilters[spec.PlanPIndexFilterName]
	}

	localPIndexes, remotePlanPIndexes, missingPIndexNames, err :=
		mgr.coveringPIndexesEx(spec.IndexName, spec.IndexUUID, ppf)
	if err != nil {
		return nil, nil, nil, err
	}

	if planPIndexFilter == nil && !noCache {
		cp := &CoveringPIndexes{
			LocalPIndexes:      localPIndexes,
			RemotePlanPIndexes: remotePlanPIndexes,
			MissingPIndexNames: missingPIndexNames,
		}

		mgr.m.Lock()
		if mgr.coveringCache == nil {
			mgr.coveringCache = map[CoveringPIndexesSpec]*CoveringPIndexes{}
		}
		mgr.coveringCache[spec] = cp
		mgr.m.Unlock()
	}

	return localPIndexes, remotePlanPIndexes, missingPIndexNames, err
}

func (mgr *Manager) coveringPIndexesEx(indexName, indexUUID string,
	planPIndexFilter PlanPIndexFilter) (
	localPIndexes []*PIndex,
	remotePlanPIndexes []*RemotePlanPIndex,
	missingPIndexNames []string,
	err error) {
	nodeDefs, err := mgr.GetNodeDefs(NODE_DEFS_WANTED, false)
	if err != nil {
		return nil, nil, nil,
			fmt.Errorf("pindex: could not get wanted nodeDefs,"+
				" err: %v", err)
	}

	_, allPlanPIndexes, err := mgr.GetPlanPIndexes(false)
	if err != nil {
		return nil, nil, nil,
			fmt.Errorf("pindex: could not retrieve allPlanPIndexes,"+
				" err: %v", err)
	}

	planPIndexes, exists := allPlanPIndexes[indexName]
	if !exists || len(planPIndexes) <= 0 {
		return nil, nil, nil,
			fmt.Errorf("pindex: no planPIndexes for indexName: %s",
				indexName)
	}

	return mgr.ClassifyPIndexes(indexName, indexUUID,
		planPIndexes, nodeDefs, planPIndexFilter)
}

func (mgr *Manager) ClassifyPIndexes(indexName, indexUUID string,
	planPIndexes []*PlanPIndex, nodeDefs *NodeDefs,
	planPIndexFilter PlanPIndexFilter) (
	localPIndexes []*PIndex, remotePlanPIndexes []*RemotePlanPIndex,
	missingPIndexNames []string, err error) {
	// Returns true if the node has the "pindex" tag.
	nodeDoesPIndexes := func(nodeUUID string) (*NodeDef, bool) {
		nodeDef, ok := nodeDefs.NodeDefs[nodeUUID]
		if ok && nodeDef.UUID == nodeUUID {
			if len(nodeDef.Tags) <= 0 {
				return nodeDef, true
			}
			for _, tag := range nodeDef.Tags {
				if tag == "pindex" {
					return nodeDef, true
				}
			}
		}
		return nil, false
	}

	localPIndexes = make([]*PIndex, 0, len(planPIndexes))
	remotePlanPIndexes = make([]*RemotePlanPIndex, 0, len(planPIndexes))
	missingPIndexNames = make([]string, 0)

	_, pindexes := mgr.CurrentMaps()

	selfUUID := mgr.UUID()

	for _, planPIndex := range planPIndexes {
		lowestNodePriority := math.MaxInt64
		var lowestNode *NodeDef

		// look through each of the nodes
		for nodeUUID, planPIndexNode := range planPIndex.Nodes {
			// if node is local, do additional checks
			nodeLocal := nodeUUID == selfUUID
			nodeLocalOK := false
			if nodeLocal {
				localPIndex, exists := pindexes[planPIndex.Name]
				if exists &&
					localPIndex != nil &&
					localPIndex.Name == planPIndex.Name &&
					localPIndex.IndexName == indexName &&
					(indexUUID == "" || localPIndex.IndexUUID == indexUUID) {
					nodeLocalOK = true
				}
			}

			// node does pindexes and it is wanted
			if nodeDef, ok := nodeDoesPIndexes(nodeUUID); ok &&
				planPIndexFilter(planPIndexNode) {
				if planPIndexNode.Priority < lowestNodePriority {
					// candidate node has lower priority
					if !nodeLocal || (nodeLocal && nodeLocalOK) {
						lowestNode = nodeDef
						lowestNodePriority = planPIndexNode.Priority
					}
				} else if planPIndexNode.Priority == lowestNodePriority {
					if nodeLocal && nodeLocalOK {
						// same priority, but prefer local nodes
						lowestNode = nodeDef
						lowestNodePriority = planPIndexNode.Priority
					}
				}
			}
		}

		// now add the node we found to the correct list
		if lowestNode == nil {
			// couldn't find anyone with this pindex
			missingPIndexNames = append(missingPIndexNames, planPIndex.Name)
		} else if lowestNode.UUID == selfUUID {
			// lowest priority is local
			localPIndex := pindexes[planPIndex.Name]
			localPIndexes = append(localPIndexes, localPIndex)
		} else {
			// lowest priority is remote
			remotePlanPIndexes =
				append(remotePlanPIndexes, &RemotePlanPIndex{
					PlanPIndex: planPIndex,
					NodeDef:    lowestNode,
				})
		}
	}

	return localPIndexes, remotePlanPIndexes, missingPIndexNames, nil
}

// coveringCacheVerLOCKED computes a CAS-like number that can be
// quickly compared to see if any inputs to the covering pindexes
// computation have changed.
func (mgr *Manager) coveringCacheVerLOCKED() uint64 {
	return mgr.stats.TotRefreshLastNodeDefs +
		mgr.stats.TotRefreshLastPlanPIndexes +
		mgr.stats.TotRegisterPIndex +
		mgr.stats.TotUnregisterPIndex
}

const PINDEX_NAMES = "pIndexNames"

// ----------------------------------------------------------------------------

// obtain the pindex name for a given pindex UUID path, optionally refreshing from
// cfg if not found in cache
func (mgr *Manager) GetPIndexName(pIndexPath string, refresh bool) (string, error) {
	mgr.namesMutex.RLock()
	name, _ := mgr.pIndexNames[pIndexPath]
	mgr.namesMutex.RUnlock()

	if name == "" && refresh {
		namesMap, err := CfgGetPIndexNames(mgr.Cfg())
		if err != nil {
			return "", fmt.Errorf("pindex: could not get pindex names from cfg, err: %v", err)
		}
		name, _ = namesMap[pIndexPath]
		if name == "" {
			return "", fmt.Errorf("pindex: no name found for pindexPath: %s", pIndexPath)
		}
		mgr.namesMutex.Lock()
		mgr.pIndexNames[pIndexPath] = name
		mgr.namesMutex.Unlock()
	}

	return name, nil
}

// obtain a UUID path for a given pindex name, creating one if it does
// not already exist, and persist the mapping to cfg
func (mgr *Manager) GetPIndexPath(pIndexName string) (string, error) {
	mgr.namesMutex.RLock()
	for path, name := range mgr.pIndexNames {
		if name == pIndexName {
			mgr.namesMutex.RUnlock()
			return path, nil
		}
	}
	mgr.namesMutex.RUnlock()

	path := convertOldPIndexPath(pIndexName)
	err := CfgSetPIndexPaths(mgr.Cfg(), path, pIndexName)
	if err != nil {
		return "", fmt.Errorf("pindex: could not set pindex path in cfg, err: %v", err)
	}
	mgr.namesMutex.Lock()
	mgr.pIndexNames[path] = pIndexName
	mgr.namesMutex.Unlock()
	return path, nil
}

// set a pindex name for a given pindex path, and persist the mapping to cfg
func (mgr *Manager) SetPIndexName(path, name string) error {
	err := CfgSetPIndexPaths(mgr.Cfg(), path, name)
	if err != nil {
		return err
	}
	mgr.namesMutex.Lock()
	mgr.pIndexNames[path] = name
	mgr.namesMutex.Unlock()
	return nil
}

// obtain all pindex name to path mappings from cfg
func CfgGetPIndexNames(cfg Cfg) (map[string]string, error) {
	// silently return if cfg is nil, which can
	// happen in some unit tests
	if cfg == nil {
		return map[string]string{}, nil
	}

	v, _, err := cfg.Get(PINDEX_NAMES, 0)
	if err != nil {
		return nil, err
	}
	var rv map[string]string
	err = UnmarshalJSON(v, &rv)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

// set a pindex name for a given pindex path, and persist the
// mapping to cfg, with retry on CAS mismatch
func CfgSetPIndexPaths(cfg Cfg, path, name string) error {
	// silently return if cfg is nil, which can
	// happen in some unit tests
	if cfg == nil {
		return nil
	}

	setFunc := func() error {
		v, cas, err := cfg.Get(PINDEX_NAMES, 0)
		if err != nil {
			return err
		}

		var rv map[string]string
		if v != nil {
			err = UnmarshalJSON(v, &rv)
			if err != nil {
				return err
			}
		} else {
			rv = make(map[string]string)
		}

		rv[path] = name

		buf, err := MarshalJSON(rv)
		if err != nil {
			return err
		}

		_, err = cfg.Set(PINDEX_NAMES, buf, cas)
		return err
	}

	return RetryOnCASMismatch(setFunc, 100)
}

// Converts old pindex paths to new randomly generated UUID paths
func convertOldPIndexPath(name string) string {
	// check if the path does not have the .pindex suffix and is 16 characters
	// long since that is the length of the new UUID path.
	if !strings.HasSuffix(name, pindexPathSuffix) && len(name) == 16 {
		return name
	}

	return NewUUID()
}
