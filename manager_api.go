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

	log "github.com/couchbase/clog"
)

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
	_, err := mgr.CreateIndexEx(sourceType, sourceName, sourceUUID,
		sourceParams, indexType, indexName, indexParams, planParams,
		prevIndexUUID)

	return err
}

func (mgr *Manager) CreateIndexEx(sourceType,
	sourceName, sourceUUID, sourceParams,
	indexType, indexName, indexParams string, planParams PlanParams,
	prevIndexUUID string) (string, error) {
	atomic.AddUint64(&mgr.stats.TotCreateIndex, 1)

	matched, err := regexp.Match(INDEX_NAME_REGEXP, []byte(indexName))
	if err != nil {
		return "", fmt.Errorf("manager_api: CreateIndex,"+
			" indexName parsing problem,"+
			" indexName: %s, err: %v", indexName, err)
	}
	if !matched {
		return "", fmt.Errorf("manager_api: CreateIndex,"+
			" indexName is invalid, indexName: %q", indexName)
	}

	indexDef := &IndexDef{
		Type:         indexType,
		Name:         indexName,
		Params:       indexParams,
		SourceType:   sourceType,
		SourceName:   sourceName,
		SourceUUID:   sourceUUID,
		SourceParams: sourceParams,
		PlanParams:   planParams,
	}

	pindexImplType, exists := PIndexImplTypes[indexType]
	if !exists {
		return "", fmt.Errorf("manager_api: CreateIndex,"+
			" unknown indexType: %s", indexType)
	}

	if pindexImplType.Prepare != nil {
		indexDef, err = pindexImplType.Prepare(mgr, indexDef)
		if err != nil {
			return "", fmt.Errorf("manager_api: CreateIndex, Prepare failed,"+
				" err: %v", err)
		}
	}
	sourceParams = indexDef.SourceParams
	indexParams = indexDef.Params

	if pindexImplType.Validate != nil {
		err = pindexImplType.Validate(indexType, indexName, indexParams)
		if err != nil {
			return "", fmt.Errorf("manager_api: CreateIndex, invalid,"+
				" err: %v", err)
		}
	}

	// First, check that the source exists.
	sourceParams, err = DataSourcePrepParams(sourceType,
		sourceName, sourceUUID, sourceParams, mgr.server, mgr.Options())
	if err != nil {
		return "", fmt.Errorf("manager_api: failed to connect to"+
			" or retrieve information from source,"+
			" sourceType: %s, sourceName: %s, sourceUUID: %s, err: %v",
			sourceType, sourceName, sourceUUID, err)
	}
	indexDef.SourceParams = sourceParams

	if len(sourceUUID) == 0 {
		// If sourceUUID isn't available, fetch the sourceUUID for
		// the sourceName by setting up a connection.
		sourceUUID, err = DataSourceUUID(sourceType, sourceName, sourceParams,
			mgr.server, mgr.Options())
		if err != nil {
			return "", fmt.Errorf("manager_api: failed to fetch sourceUUID"+
				" for sourceName: %s, sourceType: %s, err: %v",
				sourceName, sourceType, err)
		}
		indexDef.SourceUUID = sourceUUID
	}

	// Validate maxReplicasAllowed here.
	maxReplicasAllowed, _ := strconv.Atoi(mgr.Options()["maxReplicasAllowed"])
	if planParams.NumReplicas < 0 || planParams.NumReplicas > maxReplicasAllowed {
		return "", fmt.Errorf("manager_api: CreateIndex failed, maxReplicasAllowed:"+
			" '%v', but request for '%v'", maxReplicasAllowed, planParams.NumReplicas)
	}

	nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg, NODE_DEFS_KNOWN)
	if err != nil {
		return "", fmt.Errorf("manager_api: CreateIndex failed, "+
			"CfgGetNodeDefs err: %v", err)
	}
	if len(nodeDefs.NodeDefs) < planParams.NumReplicas+1 {
		return "", fmt.Errorf("manager_api: CreateIndex failed, cluster needs %d "+
			"search nodes to support the requested replica count of %d",
			planParams.NumReplicas+1, planParams.NumReplicas)
	}

	tries := 0
	version := CfgGetVersion(mgr.cfg)
	for {
		tries += 1
		if tries > 100 {
			return "", fmt.Errorf("manager_api: CreateIndex,"+
				" too many tries: %d", tries)
		}

		indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return "", fmt.Errorf("manager_api: CfgGetIndexDefs err: %v", err)
		}
		if indexDefs == nil {
			indexDefs = NewIndexDefs(version)
		}
		if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
			return "", fmt.Errorf("manager_api: could not create index,"+
				" indexDefs.ImplVersion: %s > mgr.version: %s",
				indexDefs.ImplVersion, mgr.version)
		}

		prevIndex, exists := indexDefs.IndexDefs[indexName]
		if prevIndexUUID == "" { // New index creation.
			if exists || prevIndex != nil {
				return "", fmt.Errorf("manager_api: cannot create index because"+
					" an index with the same name already exists: %s",
					indexName)
			}
		} else if prevIndexUUID == "*" {
			if exists && prevIndex != nil {
				prevIndexUUID = prevIndex.UUID
			}
		} else { // Update index definition.
			if !exists || prevIndex == nil {
				return "", fmt.Errorf("manager_api: index missing for update,"+
					" indexName: %s", indexName)
			}
			if prevIndex.UUID != prevIndexUUID {
				return "", fmt.Errorf("manager_api:"+
					" perhaps there was concurrent index definition update,"+
					" current index UUID: %s, did not match input UUID: %s",
					prevIndex.UUID, prevIndexUUID)
			}

			if prevIndex.PlanParams.PlanFrozen {
				if (prevIndex.PlanParams.MaxPartitionsPerPIndex !=
					indexDef.PlanParams.MaxPartitionsPerPIndex) ||
					(prevIndex.PlanParams.NumReplicas !=
						indexDef.PlanParams.NumReplicas) {
					return "", fmt.Errorf("manager_api: cannot update"+
						" partition or replica count for a planFrozen index,"+
						" indexName: %s", indexName)
				}
			}

		}

		indexUUID := NewUUID()
		indexDef.UUID = indexUUID
		indexDefs.UUID = indexUUID
		indexDefs.IndexDefs[indexName] = indexDef
		indexDefs.ImplVersion = version

		// NOTE: If our ImplVersion is still too old due to a race, we
		// expect a more modern planner to catch it later.

		_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
		if err != nil {
			if _, ok := err.(*CfgCASError); ok {
				continue // Retry on CAS mismatch.
			}

			return "", fmt.Errorf("manager_api: could not save indexDefs,"+
				" err: %v", err)
		}

		break // Success.
	}

	mgr.GetIndexDefs(true)
	mgr.PlannerKick("api/CreateIndex, indexName: " + indexName)
	atomic.AddUint64(&mgr.stats.TotCreateIndexOk, 1)

	if prevIndexUUID == "" {
		log.Printf("manager_api: index definition created,"+
			" indexType: %s, indexName: %s, indexUUID: %s",
			indexDef.Type, indexDef.Name, indexDef.UUID)
	} else {
		log.Printf("manager_api: index definition updated,"+
			" indexType: %s, indexName: %s, indexUUID: %s, prevIndexUUID: %s",
			indexDef.Type, indexDef.Name, indexDef.UUID, prevIndexUUID)
	}

	statsAgentsMap.registerAgents(sourceName, sourceUUID,
		sourceParams, mgr.Server(), mgr.Options())

	event := NewSystemEvent(
		IndexCreateEventID,
		"info",
		"Index created",
		map[string]interface{}{
			"indexName":  indexDef.Name,
			"sourceName": indexDef.SourceName,
			"indexUUID":  indexDef.UUID,
		})

	if event != nil {
		if prevIndexUUID != "" {
			event.EventID = IndexUpdateEventID
			event.Description = "Index updated"
		}
		err = PublishSystemEvent(event)
		if err != nil {
			log.Errorf("manager_api: unexpected system_event error"+
				" err: %v", err)
		}
	}

	return indexDef.UUID, nil
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

	mgr.m.Lock()
	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		mgr.m.Unlock()
		return "", err
	}
	if indexDefs == nil {
		mgr.m.Unlock()
		return "", fmt.Errorf("manager_api: no indexes on deletion"+
			" of indexName: %s", indexName)
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		mgr.m.Unlock()
		return "", fmt.Errorf("manager_api: could not delete index,"+
			" indexDefs.ImplVersion: %s > mgr.version: %s",
			indexDefs.ImplVersion, mgr.version)
	}
	indexDef, exists := indexDefs.IndexDefs[indexName]
	if !exists {
		mgr.m.Unlock()
		return "", fmt.Errorf("manager_api: index to delete missing,"+
			" indexName: %s", indexName)
	}
	if indexUUID != "" && indexDef.UUID != indexUUID {
		mgr.m.Unlock()
		return "", fmt.Errorf("manager_api: index to delete wrong UUID,"+
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
	if err != nil {
		mgr.m.Unlock()
		return "", fmt.Errorf("manager_api: could not save indexDefs,"+
			" err: %v", err)
	}

	mgr.m.Unlock()

	mgr.GetIndexDefs(true)
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

// IndexControl is used to change runtime properties of an index
// definition.
func (mgr *Manager) IndexControl(indexName, indexUUID, readOp, writeOp,
	planFreezeOp string) error {
	atomic.AddUint64(&mgr.stats.TotIndexControl, 1)

	mgr.m.Lock()
	defer mgr.m.Unlock()

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
	indexUUID = NewUUID()
	indexDef.UUID = indexUUID
	indexDefs.UUID = indexUUID

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

	if npp.CanRead == true && npp.CanWrite == true {
		delete(indexDef.PlanParams.NodePlanParams[""], "")
	}

	if planFreezeOp != "" {
		indexDef.PlanParams.PlanFrozen = planFreezeOp == "freeze"
	}

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
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
	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return err
	}
	if indexDefs == nil {
		return fmt.Errorf("manager_api: no indexDefs to bump")
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return fmt.Errorf("manager_api: could not bump indexDefs,"+
			" indexDefs.ImplVersion: %s > mgr.version: %s",
			indexDefs.ImplVersion, mgr.version)
	}
	if indexDefsUUID != "" && indexDefs.UUID != indexDefsUUID {
		return fmt.Errorf("manager_api: bump indexDefs wrong UUID")
	}

	indexDefs.UUID = NewUUID()
	indexDefs.ImplVersion = mgr.version

	// NOTE: if our ImplVersion is still too old due to a race, we
	// expect a more modern cbgt to do the work instead.

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
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
	mgr.m.Lock()

	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		mgr.m.Unlock()
		return err
	}
	if indexDefs == nil {
		mgr.m.Unlock()
		return fmt.Errorf("manager_api: DeleteAllIndexFromSource, no indexDefs")
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		mgr.m.Unlock()
		return fmt.Errorf("manager_api: DeleteAllIndexFromSource,"+
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
		mgr.m.Unlock()
		return nil
	}
	// update the index definitions
	indexDefs.UUID = NewUUID()
	indexDefs.ImplVersion = CfgGetVersion(mgr.cfg)
	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	mgr.m.Unlock()

	if err != nil {
		atomic.AddUint64(&mgr.stats.TotDeleteIndexBySourceErr, deletedCount)
		return fmt.Errorf("manager_api: could not save indexDefs,"+
			" err: %v", err)
	}

	atomic.AddUint64(&mgr.stats.TotDeleteIndexBySourceOk, deletedCount)
	mgr.GetIndexDefs(true)
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
