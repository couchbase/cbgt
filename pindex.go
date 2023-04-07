//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	log "github.com/couchbase/clog"
	"github.com/couchbase/tools-common/fsutil"
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
		err := p.Dest.Close()
		if err != nil {
			log.Errorf("pindex: %s Close failed, err: %v", p.Name, err)
			return err
		}
	}

	if remove {
		os.RemoveAll(p.Path)
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

func rollbackPIndex(mgr *Manager, pindex *PIndex) {
	log.Printf("pindex: rollbackPIndex starts for pindex: %s", pindex.Name)

	syncWorkReq(mgr.janitorCh, JANITOR_ROLLBACK_PINDEX,
		"rollback:"+pindex.Name, pindex)
}

var ErrTerminatedDownload = fmt.Errorf("pindex: case of abruptly terminated download")

func createNewPIndex(mgr *Manager, name, uuid, indexType, indexName, indexUUID, indexParams,
	sourceType, sourceName, sourceUUID, sourceParams, sourcePartitions string,
	path string, createPIndexFunc func(indexType, indexParams, sourceParams,
		path string, mgr *Manager, restart func()) (PIndexImpl, Dest, error)) (*PIndex, error) {

	var pindex *PIndex

	rollback := func() {
		go rollbackPIndex(mgr, pindex)
	}

	params := IndexPrepParams{SourceName: sourceName, IndexName: indexName,
		Params: indexParams}

	pBytes, err := json.Marshal(&params)
	if err != nil {
		return nil, fmt.Errorf("pindex: RollbackPIndex, json marshal err: %v", err)
	}

	impl, dest, err := createPIndexFunc(indexType, string(pBytes), sourceParams,
		path, mgr, rollback)
	if err != nil && err != ErrTerminatedDownload {
		os.RemoveAll(path)
		return nil, fmt.Errorf("pindex: new indexType: %s, indexParams: %s,"+
			" path: %s, err: %s", indexType, indexParams, path, err)
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
		buf, err := json.Marshal(pindex)
		if err != nil {
			dest.Close()
			os.RemoveAll(path)
			return nil, err
		}

		// Creating a directory to store the PINDEX_META file.
		log.Printf("pindex: creating directory at %s", path)
		err = fsutil.Mkdir(path, 0700, true, true)
		if err != nil {
			return nil, fmt.Errorf("pindex: could not create path %s: %#v",
				path, err)
		}

		err = os.WriteFile(path+string(os.PathSeparator)+PINDEX_META_FILENAME,
			buf, 0600)
		if err != nil {
			dest.Close()
			os.RemoveAll(path)
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

// OpenPIndex reopens a previously created pindex.  The path argument
// must be a directory for the pindex.
func OpenPIndex(mgr *Manager, path string) (pindex *PIndex, err error) {
	pindex = &PIndex{}
	// load PINDEX_META only if manager's dataDir is set
	if mgr != nil && len(mgr.dataDir) > 0 {
		buf, err := os.ReadFile(path + string(os.PathSeparator) + PINDEX_META_FILENAME)
		if err != nil {
			return nil, fmt.Errorf("pindex: could not load PINDEX_META_FILENAME,"+
				" path: %s, err: %v", path, err)
		}

		err = json.Unmarshal(buf, pindex)
		if err != nil {
			return nil, fmt.Errorf("pindex: could not parse pindex json,"+
				" path: %s, err: %v", path, err)
		}
	}

	rollback := func() {
		go rollbackPIndex(mgr, pindex)
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pindex: OpenPIndex panic msg: %v \n, %v",
				r, ReadableStackTrace())
		}
	}()

	impl, dest, err := OpenPIndexImplUsing(pindex.IndexType, path,
		pindex.IndexParams, rollback)
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
