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
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
)

type BadRequestError struct {
	errMsg string
}

func (e *BadRequestError) Error() string {
	return e.errMsg
}

func NewBadRequestError(format string, args ...interface{}) error {
	return fmt.Errorf("%w", &BadRequestError{
		errMsg: fmt.Sprintf(format, args...),
	})
}

type InternalServerError struct {
	errMsg string
}

func (e *InternalServerError) Error() string {
	return e.errMsg
}

func NewInternalServerError(format string, args ...interface{}) error {
	return fmt.Errorf("%w", &InternalServerError{
		errMsg: fmt.Sprintf(format, args...),
	})
}

// cfgRefreshWaitExpiry represents the manager's config
// refresh timeout for metakv api access.
var cfgRefreshWaitExpiry = time.Second * 10

// INDEX_NAME_REGEXP is used to validate index definition names.
const INDEX_NAME_REGEXP = `^[A-Za-z][0-9A-Za-z_\-]*$`

// IndexPrepParams can be used to override any of the
// unset index parameters.
type IndexPrepParams struct {
	Params     string `json:"params,omitempty"`
	SourceName string `json:"sourceName,omitempty"`
	IndexName  string `json:"indexName,omitempty"`
}

// Creates a logical index definition.  A non-"" prevIndexUUID means
// an update to an existing index.
func (mgr *Manager) CreateIndex(sourceType,
	sourceName, sourceUUID, sourceParams,
	indexType, indexName, indexParams string, planParams PlanParams,
	prevIndexUUID string) error {
	_, _, err := mgr.CreateIndexEx(&CreateIndexPayload{
		SourceType:    sourceType,
		SourceName:    sourceName,
		SourceUUID:    sourceUUID,
		SourceParams:  sourceParams,
		IndexType:     indexType,
		IndexName:     indexName,
		IndexParams:   indexParams,
		PlanParams:    planParams,
		PrevIndexUUID: prevIndexUUID,
	})

	return err
}

type CreateIndexPayload struct {
	SourceType    string
	SourceName    string
	SourceUUID    string
	SourceParams  string
	IndexType     string
	IndexName     string
	IndexParams   string
	PlanParams    PlanParams
	PrevIndexUUID string
	ScopedPrefix  string
}

// Enforcing a maximum index name length of 209;
//
// See: MB-59858
// Linux, Windows and OSX enforce a maximum file/dir name length of 255.
// The index partitions directories created for these indexes have a
// hash suffix and here's a sample of how that'd look:
// -> <indexName>_1234567890123456_abcdefgh.pindex
// .. which can be separated into:
// - indexName (variable length)
// - '_' + hash(len=16) + '_' + hash(len=8) + '.pindex' (fixed length=33)
//
// So the max length that we can support for an index name is (255-33) = 222
// Also accommodating additional space (13) for any transient directory
// extensions - like during rebalance when a ".temp" can be suffixed and
// tar/gzip extensions.
const maxDirNameLen = 255

var (
	// Additional space reserved for transient extensions (13)
	miscExtLen         = len(TempPathPrefix) + len(".tar.gz")
	MaxIndexNameLength = maxDirNameLen - (26 + len(pindexPathSuffix) + miscExtLen)
)

func (mgr *Manager) CreateIndexEx(payload *CreateIndexPayload) (string, string, error) {
	atomic.AddUint64(&mgr.stats.TotCreateIndex, 1)

	if payload == nil {
		return "", "", NewBadRequestError("manager_api: CreateIndex," +
			" payload is nil")
	}

	adjustedIndexName := payload.ScopedPrefix + payload.IndexName

	if payload.PrevIndexUUID == "" {
		// index name validations during the fresh index creation.
		matched, err := regexp.Match(INDEX_NAME_REGEXP, []byte(payload.IndexName))
		if err != nil {
			return adjustedIndexName, "", NewBadRequestError("manager_api: CreateIndex,"+
				" indexName parsing problem,"+
				" indexName: %s, err: %v", payload.IndexName, err)
		}
		if !matched {
			return adjustedIndexName, "", NewBadRequestError("manager_api: CreateIndex,"+
				" indexName is invalid, indexName: %q", payload.IndexName)
		}
	}

	if len(adjustedIndexName) > MaxIndexNameLength {
		return "", "", NewBadRequestError("manager_api: CreateIndex,"+
			" chosen index name is too long, consider one that is less"+
			" than %v characters", MaxIndexNameLength)
	}

	indexDef := &IndexDef{
		Type:         payload.IndexType,
		Name:         adjustedIndexName,
		Params:       payload.IndexParams,
		SourceType:   payload.SourceType,
		SourceName:   payload.SourceName,
		SourceUUID:   payload.SourceUUID,
		SourceParams: payload.SourceParams,
		PlanParams:   payload.PlanParams,
	}

	pindexImplType, exists := PIndexImplTypes[payload.IndexType]
	if !exists {
		return adjustedIndexName, "", NewBadRequestError("manager_api: CreateIndex,"+
			" unknown indexType: %s", payload.IndexType)
	}

	var err error
	if pindexImplType.Prepare != nil {
		indexDef, err = pindexImplType.Prepare(mgr, indexDef)
		if err != nil {
			return adjustedIndexName, "", fmt.Errorf("manager_api: CreateIndex, Prepare failed,"+
				" err: %v", err)
		}
	}
	payload.SourceParams = indexDef.SourceParams
	payload.IndexParams = indexDef.Params
	payload.IndexName = indexDef.Name

	if pindexImplType.Validate != nil {
		err = pindexImplType.Validate(
			payload.IndexType, payload.IndexName, payload.IndexParams)
		if err != nil {
			return adjustedIndexName, "", fmt.Errorf("manager_api: CreateIndex, invalid,"+
				" err: %v", err)
		}
	}

	// First, check that the source exists.
	payload.SourceParams, err = DataSourcePrepParams(
		payload.SourceType,
		payload.SourceName,
		payload.SourceUUID,
		payload.SourceParams,
		mgr.server,
		mgr.Options(),
	)
	if err != nil {
		return adjustedIndexName, "", NewInternalServerError("manager_api: failed to connect to"+
			" or retrieve information from source,"+
			" sourceType: %s, sourceName: %s, sourceUUID: %s, err: %v",
			payload.SourceType, payload.SourceName, payload.SourceUUID, err)
	}
	indexDef.SourceParams = payload.SourceParams

	payload.SourceUUID, err = DataSourceUUID(
		payload.SourceType,
		payload.SourceName,
		payload.SourceParams,
		mgr.server, mgr.Options(),
	)
	if err != nil {
		return adjustedIndexName, "", NewInternalServerError("manager_api: failed to fetch sourceUUID"+
			" for sourceName: %s, sourceType: %s, err: %v",
			payload.SourceName, payload.SourceType, err)
	}

	if len(payload.SourceUUID) > 0 {
		// Feed's SourceUUIDLookUp is optional.
		if len(indexDef.SourceUUID) == 0 {
			// If sourceUUID is NOT available within the index def, update it.
			indexDef.SourceUUID = payload.SourceUUID
		} else if indexDef.SourceUUID != payload.SourceUUID {
			// The sourceUUID provided within the index definition does NOT match
			// the sourceUUID for the sourceName in the system.
			return adjustedIndexName, "", NewBadRequestError("manager_api: CreateIndex failed, sourceUUID"+
				" mismatched for sourceName: %s", payload.SourceName)
		}
	}

	// Validate maxReplicasAllowed here.
	maxReplicasAllowed, _ := strconv.Atoi(mgr.GetOption("maxReplicasAllowed"))
	if payload.PlanParams.NumReplicas < 0 ||
		payload.PlanParams.NumReplicas > maxReplicasAllowed {
		return adjustedIndexName, "", NewBadRequestError("manager_api: CreateIndex failed, maxReplicasAllowed:"+
			" '%v', but request for '%v'", maxReplicasAllowed, payload.PlanParams.NumReplicas)
	}

	nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg, NODE_DEFS_KNOWN)
	if err != nil {
		return adjustedIndexName, "", NewInternalServerError("manager_api: CreateIndex failed, "+
			"CfgGetNodeDefs err: %v", err)
	}
	if len(nodeDefs.NodeDefs) < payload.PlanParams.NumReplicas+1 {
		return adjustedIndexName, "", NewBadRequestError("manager_api: CreateIndex failed, cluster needs %d "+
			"search nodes to support the requested replica count of %d",
			payload.PlanParams.NumReplicas+1, payload.PlanParams.NumReplicas)
	}

	version := CfgGetVersion(mgr.cfg)

	indexCreateFunc := func() error {
		indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return NewInternalServerError("manager_api: CfgGetIndexDefs err: %v", err)
		}
		if indexDefs == nil {
			indexDefs = NewIndexDefs(version)
		}
		if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
			return NewInternalServerError("manager_api: could not create index,"+
				" indexDefs.ImplVersion: %s > mgr.version: %s",
				indexDefs.ImplVersion, mgr.version)
		}

		prevIndex, exists := indexDefs.IndexDefs[payload.IndexName]
		if payload.PrevIndexUUID == "" { // New index creation.
			if exists || prevIndex != nil {
				return NewBadRequestError("manager_api: cannot create index because"+
					" an index with the same name already exists: %s",
					payload.IndexName)
			}
		} else if payload.PrevIndexUUID == "*" {
			if exists && prevIndex != nil {
				payload.PrevIndexUUID = prevIndex.UUID
			}
		} else { // Update index definition.
			if !exists || prevIndex == nil {
				return NewBadRequestError("manager_api: index missing for update,"+
					" indexName: %s", payload.IndexName)
			}
			if prevIndex.UUID != payload.PrevIndexUUID {
				return NewBadRequestError("manager_api:"+
					" perhaps there was concurrent index definition update,"+
					" current index UUID: %s, did not match input UUID: %s",
					prevIndex.UUID, payload.PrevIndexUUID)
			}

			if prevIndex.PlanParams.PlanFrozen {
				if (prevIndex.PlanParams.MaxPartitionsPerPIndex !=
					indexDef.PlanParams.MaxPartitionsPerPIndex) ||
					(prevIndex.PlanParams.NumReplicas !=
						indexDef.PlanParams.NumReplicas) {
					return NewBadRequestError("manager_api: cannot update"+
						" partition or replica count for a planFrozen index,"+
						" indexName: %s", payload.IndexName)
				}
			}

		}

		indexUUID := NewUUID()
		indexDef.UUID = indexUUID
		indexDefs.UUID = indexUUID
		indexDefs.IndexDefs[payload.IndexName] = indexDef
		indexDefs.ImplVersion = version

		// NOTE: If our ImplVersion is still too old due to a race, we
		// expect a more modern planner to catch it later.

		_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
		return err
	}

	err = RetryOnCASMismatch(indexCreateFunc, 100)
	if err != nil {
		return adjustedIndexName, "", fmt.Errorf("manager_api: could not save indexDefs,"+
			" err: %v", err)
	}

	mgr.refreshIndexDefsWithTimeout(cfgRefreshWaitExpiry)

	mgr.PlannerKick("api/CreateIndex, indexName: " + payload.IndexName)
	atomic.AddUint64(&mgr.stats.TotCreateIndexOk, 1)

	if payload.PrevIndexUUID == "" {
		log.Printf("manager_api: index definition created,"+
			" indexType: %s, indexName: %s, indexUUID: %s",
			indexDef.Type, indexDef.Name, indexDef.UUID)
	} else {
		log.Printf("manager_api: index definition updated,"+
			" indexType: %s, indexName: %s, indexUUID: %s, prevIndexUUID: %s",
			indexDef.Type, indexDef.Name, indexDef.UUID, payload.PrevIndexUUID)
	}

	if payload.IndexType == "fulltext-index" && len(payload.SourceName) > 0 {
		// In case of index updates, reusing the existing dcp stats agent from
		// the previous version of the index, since it doesn't increase the number
		// of live indexes in the cluster
		if payload.PrevIndexUUID == "" {
			statsAgentsMap.registerAgents(payload.SourceName, payload.SourceUUID,
				payload.SourceParams, mgr.Server(), mgr.Options())
		}
	}

	event := NewSystemEvent(
		IndexCreateEventID,
		"info",
		"Index created",
		map[string]interface{}{
			"indexName":  adjustedIndexName,
			"sourceName": indexDef.SourceName,
			"indexUUID":  indexDef.UUID,
		})

	if event != nil {
		if payload.PrevIndexUUID != "" {
			event.EventID = IndexUpdateEventID
			event.Description = "Index updated"
		}
		err = PublishSystemEvent(event)
		if err != nil {
			log.Errorf("manager_api: unexpected system_event error"+
				" err: %v", err)
		}
	}

	return indexDef.Name, indexDef.UUID, nil
}

// DeleteIndex deletes a logical index definition.
func (mgr *Manager) DeleteIndex(indexName string) error {
	log.Printf("manager_api: DeleteIndex, indexname: %s", indexName)
	_, err := mgr.DeleteIndexEx(indexName, "")
	if err != nil {
		log.Errorf("manager_api: DeleteIndex, indexname: %s, err: %v",
			indexName, err)
	}
	return err
}

// DeleteIndexEx deletes a logical index definition, with an optional
// indexUUID ("" means don't care).
func (mgr *Manager) DeleteIndexEx(indexName, indexUUID string) (
	string, error) {
	atomic.AddUint64(&mgr.stats.TotDeleteIndex, 1)

	var indexDef *IndexDef
	var exists bool

	indexDeleteFunc := func() error {
		indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return err
		}
		if indexDefs == nil {
			return NewBadRequestError("manager_api: no indexes on deletion"+
				" of indexName: %s", indexName)
		}
		if !VersionGTE(mgr.version, indexDefs.ImplVersion) {
			return NewInternalServerError("manager_api: could not delete index,"+
				" indexDefs.ImplVersion: %s > mgr.version: %s",
				indexDefs.ImplVersion, mgr.version)
		}
		indexDef, exists = indexDefs.IndexDefs[indexName]
		if !exists {
			return NewBadRequestError("manager_api: index to delete missing,"+
				" indexName: %s", indexName)
		}
		if indexUUID != "" && indexDef.UUID != indexUUID {
			return NewBadRequestError("manager_api: index to delete wrong UUID,"+
				" indexName: %s", indexName)
		}

		// Associated couchbase.Bucket instances and gocbcore.Agent/DCPAgent
		// instances that are used for stats are closed by the ctl routine.

		indexDefs.UUID = NewUUID()
		delete(indexDefs.IndexDefs, indexName)
		indexDefs.ImplVersion = CfgGetVersion(mgr.cfg)

		// NOTE: if our ImplVersion is still too old due to a race, we
		// expect a more modern planner to catch it later.

		_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
		return err
	}

	err := RetryOnCASMismatch(indexDeleteFunc, 100)
	if err != nil {
		return "", fmt.Errorf("manager_api: could not save indexDefs,"+
			" err: %v", err)
	}

	mgr.refreshIndexDefsWithTimeout(cfgRefreshWaitExpiry)

	mgr.PlannerKick("api/DeleteIndex, indexName: " + indexName)
	atomic.AddUint64(&mgr.stats.TotDeleteIndexOk, 1)

	log.Printf("manager_api: index definition deleted,"+
		" indexType: %s, indexName: %s, indexUUID: %s",
		indexDef.Type, indexDef.Name, indexDef.UUID)

	err = PublishSystemEvent(NewSystemEvent(
		IndexDeleteEventID,
		"info",
		"Index deleted",
		map[string]interface{}{
			"indexName":  indexDef.Name,
			"sourceName": indexDef.SourceName,
			"indexUUID":  indexDef.UUID,
		}))
	if err != nil {
		log.Errorf("manager_api: unexpected system_event error"+
			" err: %v", err)
	}

	return indexDef.UUID, nil
}

// Index Control Default Values
//
// Applications interested in updating these defaults must do so
// at init() time.
//
// These defaults primarily serve the purpose of allowing all
// IndexDef consumers to get the value of CanRead and CanWrite
// for an index, even if the NodePlanParams are not present in
// the IndexDef (which is common for indexes that allow all nodes)
var (
	DefaultIndexCanRead  = true
	DefaultIndexCanWrite = true
)

// IndexControl is used to change runtime properties of an index
// definition.
func (mgr *Manager) IndexControl(indexName, indexUUID, readOp, writeOp,
	planFreezeOp string) error {
	atomic.AddUint64(&mgr.stats.TotIndexControl, 1)

	indexControlFunc := func() error {
		indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return err
		}
		if indexDefs == nil {
			return fmt.Errorf("manager_api: no indexes,"+
				" index read/write control, indexName: %s", indexName)
		}
		if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
			return fmt.Errorf("manager_api: index read/write control,"+
				" indexName: %s,"+
				" indexDefs.ImplVersion: %s > mgr.version: %s",
				indexName, indexDefs.ImplVersion, mgr.version)
		}
		indexDef, exists := indexDefs.IndexDefs[indexName]
		if !exists || indexDef == nil {
			return fmt.Errorf("manager_api: no index to read/write control,"+
				" indexName: %s", indexName)
		}
		if indexUUID != "" && indexDef.UUID != indexUUID {
			return fmt.Errorf("manager_api: index.UUID mismatched")
		}

		// refresh the UUID as we are updating the indexDef
		newIndexUUID := NewUUID()
		indexDef.UUID = newIndexUUID
		indexDefs.UUID = newIndexUUID

		if indexDef.PlanParams.NodePlanParams == nil {
			indexDef.PlanParams.NodePlanParams =
				map[string]map[string]*NodePlanParam{}
		}
		if indexDef.PlanParams.NodePlanParams[""] == nil {
			indexDef.PlanParams.NodePlanParams[""] =
				map[string]*NodePlanParam{}
		}
		if indexDef.PlanParams.NodePlanParams[""][""] == nil {
			indexDef.PlanParams.NodePlanParams[""][""] = &NodePlanParam{
				CanRead:  true,
				CanWrite: true,
			}
		}

		// TODO: Allow for node UUID and planPIndex.Name inputs.
		npp := indexDef.PlanParams.NodePlanParams[""][""]
		if readOp != "" {
			if readOp == "allow" || readOp == "resume" {
				npp.CanRead = true
			} else {
				npp.CanRead = false
			}
		}
		if writeOp != "" {
			if writeOp == "allow" || writeOp == "resume" {
				npp.CanWrite = true
			} else {
				npp.CanWrite = false
			}
		}

		// Remove NodePlanParams from indexDef if all nodes are allowed to read/write.
		// This is to avoid unnecessary data in the indexDef
		//
		// Readers of IndexDefs should assume that if NodePlanParams is empty,
		// all nodes are allowed to read/write
		if npp.CanRead && npp.CanWrite {
			delete(indexDef.PlanParams.NodePlanParams[""], "")
			delete(indexDef.PlanParams.NodePlanParams, "")
		}

		if planFreezeOp != "" {
			indexDef.PlanParams.PlanFrozen = planFreezeOp == "freeze"
		}

		_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
		return err
	}

	err := RetryOnCASMismatch(indexControlFunc, 100)
	if err != nil {
		return fmt.Errorf("manager_api: could not save indexDefs,"+
			" err: %v", err)
	}

	atomic.AddUint64(&mgr.stats.TotIndexControlOk, 1)
	return nil
}

// BumpIndexDefs bumps the uuid of the index defs, to force planners
// and other downstream tasks to re-run.
func (mgr *Manager) BumpIndexDefs(indexDefsUUID string) error {

	var indexDefs *IndexDefs
	var err error
	var cas uint64

	bumpIndexDefsFunc := func() error {
		indexDefs, cas, err = CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return err
		}
		if indexDefs == nil {
			return fmt.Errorf("manager_api: no indexDefs to bump")
		}
		prevIndexImplVersion := indexDefs.ImplVersion
		if VersionGTE(mgr.version, prevIndexImplVersion) == false {
			return NewInternalServerError("manager_api: could not bump indexDefs,"+
				" indexDefs.ImplVersion: %s > mgr.version: %s",
				prevIndexImplVersion, mgr.version)
		}
		if indexDefsUUID != "" && indexDefs.UUID != indexDefsUUID {
			return fmt.Errorf("manager_api: bump indexDefs wrong UUID")
		}

		indexDefs.UUID = NewUUID()
		indexDefs.ImplVersion = prevIndexImplVersion

		// NOTE: if our ImplVersion is still too old due to a race, we
		// expect a more modern cbgt to do the work instead.

		_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
		return err
	}

	err = RetryOnCASMismatch(bumpIndexDefsFunc, 100)
	if err != nil {
		return fmt.Errorf("manager_api: could not bump indexDefs,"+
			" err: %v", err)
	}

	log.Printf("manager_api: bumped indexDefs, indexDefsUUID: %s",
		indexDefs.UUID)

	return nil
}

// DeleteAllIndexFromSource deletes all indexes with a given
// sourceType and sourceName.
func (mgr *Manager) DeleteAllIndexFromSource(
	sourceType, sourceName, sourceUUID string) error {

	var deletedCount uint64
	indexDeleteFromSourceFunc := func() error {
		indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return err
		}
		if indexDefs == nil {
			return fmt.Errorf("manager_api: DeleteAllIndexFromSource, no indexDefs")
		}
		if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
			return NewInternalServerError("manager_api: DeleteAllIndexFromSource,"+
				" indexDefs.ImplVersion: %s > mgr.version: %s",
				indexDefs.ImplVersion, mgr.version)
		}

		// Close associated couchbase.Bucket instances used for stats
		statsCBBktMap.closeCouchbaseBucket(sourceName, sourceUUID)

		var deletedCount uint64
		for indexName, indexDef := range indexDefs.IndexDefs {
			if indexDef.SourceType == sourceType &&
				indexDef.SourceName == sourceName {
				if sourceUUID != "" && indexDef.SourceUUID != "" &&
					sourceUUID != indexDef.SourceUUID {
					continue
				}

				atomic.AddUint64(&mgr.stats.TotDeleteIndexBySource, 1)
				delete(indexDefs.IndexDefs, indexName)

				log.Printf("manager_api: starting index definition deletion,"+
					" indexType: %s, indexName: %s, indexUUID: %s",
					indexDef.Type, indexDef.Name, indexDef.UUID)

				deletedCount++

				// Release associated gocbcore.Agent/DCPAgent instances used for stats
				statsAgentsMap.releaseAgents(sourceName)
			}
		}
		// exit early if nothing to delete
		if deletedCount == 0 {
			return nil
		}
		// update the index definitions
		indexDefs.UUID = NewUUID()
		indexDefs.ImplVersion = CfgGetVersion(mgr.cfg)
		_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
		return err
	}

	err := RetryOnCASMismatch(indexDeleteFromSourceFunc, 100)
	if err != nil {
		atomic.AddUint64(&mgr.stats.TotDeleteIndexBySourceErr, deletedCount)
		return fmt.Errorf("manager_api: could not save indexDefs,"+
			" err: %v", err)
	}

	atomic.AddUint64(&mgr.stats.TotDeleteIndexBySourceOk, deletedCount)
	mgr.refreshIndexDefsWithTimeout(cfgRefreshWaitExpiry)
	mgr.PlannerKick("api/DeleteIndexes, for bucket: " + sourceName)

	// With MB-19117, we've seen cfg that strangely had empty
	// indexDefs, but non-empty planPIndexes.  Force bump the
	// indexDefs so the planner and other downstream tasks re-run.
	err = mgr.BumpIndexDefs("")
	if err != nil {
		return err
	}
	log.Printf("manager_api: DeleteAllIndexFromSource," +
		" index deletions completed")

	return nil
}

// DefaultCfgDebounceOffsetInMs represents the default value for
// the debounce interval for the config events.
var DefaultCfgDebounceOffsetInMs = int(500)

// DefaultNodeOffsetMultiplier represents the default value for
// of an offset multiplier for nodes.
var DefaultNodeOffsetMultiplier = int(4)

func (mgr *Manager) GetCfgDeBounceOffsetAndMultiplier() (int, int) {
	offset, found := ParseOptionsInt(mgr.options, "cfgDebounceOffsetInMs")
	if !found {
		offset = DefaultCfgDebounceOffsetInMs
	}

	nm, found := ParseOptionsInt(mgr.options, "cfgNodeOffsetMultiplier")
	if !found {
		nm = DefaultNodeOffsetMultiplier
	}

	return offset, nm
}

// refreshIndexDefsWithTimeout refresh manager's index defs cache
// to immediately reflect the latest index definitions in the system
// on the node where this request was received (the ctl routine is
// responsible for eventually  updating the cache on other nodes).
func (mgr *Manager) refreshIndexDefsWithTimeout(duration time.Duration) {
	ch := make(chan error, 1)
	refreshIndexDefCache := func() chan error {
		go func() {
			_, _, err := mgr.GetIndexDefs(true)
			ch <- err
		}()
		return ch
	}

	select {
	case <-refreshIndexDefCache():
	case <-time.After(duration):
		atomic.AddUint64(&mgr.stats.TotSlowConfigAccess, 1)
		log.Printf("manager_api: GetIndexDefs found to be slow")
	}
}
