//  Copyright (c) 2017 Couchbase, Inc.
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
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth/metakv"
	log "github.com/couchbase/clog"
)

// NodeFeatureLeanPlan represents the feature flag for PlanPIndexes
// under extras
var NodeFeatureLeanPlan = "leanPlan"

var leanPlanMetaKvKeyGlue = "Lean/planPIndexesLean-"

// curMetaKvPlanKey is the source of truth meta kv path for the
// current plan
var curMetaKvPlanKey = "curMetaKvPlanKey"

var leanPlanKeyPrefix = "/fts/cbgt/cfg/planPIndexesLean/planPIndexesLean-"

var md5HashLength = 32

// GetAllMetaKvChildrenFunc defines the callback func signature that's
// invoked during supported feature check in a cluster
type GetAllMetaKvChildrenFunc func(key string,
	c *CfgMetaKv) ([]metakv.KVEntry, error)

// LeanPlanPIndexes represents the deduplicated split version of planPIndexes
// grouped by indexName
type LeanPlanPIndexes struct {
	UUID              string                            `json:"uuid"`
	ImplVersion       string                            `json:"implVersion"`
	IndexPlanPIndexes map[string]*LeanIndexPlanPIndexes `json:"indexPlanPIndexes"` // indexName => LeanPlanPIndexes
	Warnings          map[string][]string               `json:"warnings"`
}

// LeanIndexPlanPIndexes deduplicate the IndexDef parts from PlanPIndexes
type LeanIndexPlanPIndexes struct {
	IndexDef         *IndexDef                  `json:"indexDef"`
	LeanPlanPIndexes map[string]*LeanPlanPIndex `json:"leanPlanPIndexes"`
}

// LeanPlanPIndex represents the lean version of PlanPIndex
type LeanPlanPIndex struct {
	Name             string                     `json:"name"`
	UUID             string                     `json:"uuid"`
	SourcePartitions string                     `json:"sourcePartitions"`
	Nodes            map[string]*PlanPIndexNode `json:"nodes"`
}

// setLeanPlan splits a PlanPIndexes into multiple child metakv entries
// grouped under indexName and must be invoked with c.m.Lock()'ed.
// It writes the plan to a new directory everytime and upon successful
// completion of the whole subdirectory write, it updates the current
// plan key atomically and cleans up the older plan directory.
// The new directory name is suffixed with the MD5 hash of the whole plan
// contained inside and it is also used to check the sanity of the data
// while reading back.
func setLeanPlan(c *CfgMetaKv,
	key string, val []byte, cas uint64) (uint64, error) {
	hashMD5, err := computeMD5(val)
	if err != nil {
		return 0, err
	}
	// metaKv root path for planPIndexes is
	// "/fts/cbgt/cfg/planPIndexesLean/planPIndexesLean-$hashMD5-$TimeInMillisecs/"
	timeMs := time.Now().UnixNano() / 1000000
	newPath := c.keyToPath(key) + leanPlanMetaKvKeyGlue +
		hashMD5 + "-" + strconv.FormatInt(timeMs, 10) + "/"

	planPIndexes := &PlanPIndexes{}
	err = json.Unmarshal(val, planPIndexes)
	if err != nil {
		return 0, err
	}

	leanPlanPIndexes := &LeanPlanPIndexes{
		IndexPlanPIndexes: make(map[string]*LeanIndexPlanPIndexes),
		Warnings:          make(map[string][]string),
	}
	leanPlanPIndexes.ImplVersion = planPIndexes.ImplVersion
	leanPlanPIndexes.UUID = planPIndexes.UUID
	leanPlanPIndexes.Warnings = planPIndexes.Warnings
	for _, ppi := range planPIndexes.PlanPIndexes {
		indexPlanPIndexes, exists := leanPlanPIndexes.IndexPlanPIndexes[ppi.IndexName]
		if !exists {
			ipp := &LeanIndexPlanPIndexes{
				LeanPlanPIndexes: make(map[string]*LeanPlanPIndex)}
			ipp.IndexDef = &IndexDef{Name: ppi.IndexName,
				UUID:         ppi.IndexUUID,
				SourceName:   ppi.SourceName,
				SourceParams: ppi.SourceParams,
				SourceType:   ppi.SourceType,
				SourceUUID:   ppi.SourceUUID,
				Type:         ppi.IndexType,
				Params:       ppi.IndexParams,
			}
			leanPlanPIndexes.IndexPlanPIndexes[ppi.IndexName] = ipp
			indexPlanPIndexes = ipp
		}
		if _, exist := indexPlanPIndexes.LeanPlanPIndexes[ppi.Name]; !exist {
			indexPlanPIndexes.LeanPlanPIndexes[ppi.Name] = &LeanPlanPIndex{
				Name:             ppi.Name,
				UUID:             ppi.UUID,
				SourcePartitions: ppi.SourcePartitions,
				Nodes:            ppi.Nodes,
			}
		}
	}

	for name, ipp := range leanPlanPIndexes.IndexPlanPIndexes {
		childPlan := &LeanPlanPIndexes{
			UUID:              leanPlanPIndexes.UUID,
			IndexPlanPIndexes: map[string]*LeanIndexPlanPIndexes{},
			ImplVersion:       leanPlanPIndexes.ImplVersion,
			Warnings:          leanPlanPIndexes.Warnings,
		}
		childPlan.IndexPlanPIndexes[name] = ipp
		val, err = json.Marshal(childPlan)
		if err != nil {
			// clean up the incomplete plan directories
			log.Printf("cfg_metakv_lean: setLeanPlan json marshal, err: %v", err)
			metakv.RecursiveDelete(newPath)
			return 0, err
		}
		childPath := newPath + name
		log.Printf("cfg_metakv_lean: setLeanPlan, key: %v, childPath: %v",
			key, childPath)
		err = metakv.Set(childPath, val, nil)
		if err != nil {
			// clean up the incomplete plan directories
			log.Printf("cfg_metakv_lean: setLeanPlan metakv.Set, err: %v", err)
			metakv.RecursiveDelete(newPath)
			return 0, err
		}
	}

	// update the curPlanMetaKvKey to point to the latest plan directory
	curPath, _, err := getCurMetaKvPlanPath(c)
	if err != nil {
		log.Printf("cfg_metakv_lean: setLeanPlan, getCurMetaKvPlanPath,"+
			" err: %v", err)
		metakv.RecursiveDelete(newPath)
		return 0, err
	}
	err = metakv.Set(c.keyToPath(curMetaKvPlanKey), []byte(newPath), nil)
	if err != nil {
		log.Printf("cfg_metakv_lean: setLeanPlan, curMetaKvPlanKey "+
			"Set, err: %v", err)
		metakv.RecursiveDelete(newPath)
		return 0, err
	}
	// double check before deleting the older plan directory
	if strings.HasPrefix(curPath, leanPlanKeyPrefix) {
		err = metakv.RecursiveDelete(curPath)
		if err != nil {
			log.Printf("cfg_metakv_lean: setLeanPlan, "+
				"metakv.RecursiveDelete, err: %v", err)
			return 0, err
		}
	}

	casResult := c.lastSplitCAS + 1
	c.lastSplitCAS = casResult
	// purge any orphaned lean planPIndexes
	purgeOrphanedLeanPlans(c, newPath)
	return casResult, err
}

// getLeanPlan retrieves multiple child entries from the metakv and
// weaves the results back into a composite PlanPIndexes.
// getLeanPlan must be invoked with c.m.Lock()'ed
func getLeanPlan(c *CfgMetaKv,
	key string, cas uint64) ([]byte, uint64, error) {
	maxRetry := 3
	attempt := 0
	// retry helps in resolving any hash conflicts arise out of racy
	// delete/set operations happening with the planPIndex directory
RETRY:
	attempt++
	// fetch the current planPIndex path first
	path, _, err := getCurMetaKvPlanPath(c)
	if err != nil {
		return nil, 0, err
	}
	// looks like an upgrade scenario as no leanPlan path set yet,
	// hence fallback to older shared plan
	if path == "" {
		return getSharedPlan(c, key, cas)
	}

	children, err := metakv.ListAllChildren(path)
	if err != nil {
		return nil, 0, err
	}
	rv := &PlanPIndexes{
		PlanPIndexes: make(map[string]*PlanPIndex),
		Warnings:     make(map[string][]string),
	}
	leanPlanPIndexes := &LeanPlanPIndexes{
		IndexPlanPIndexes: make(map[string]*LeanIndexPlanPIndexes),
	}

	for _, v := range children {
		var childPlan LeanPlanPIndexes
		err = json.Unmarshal(v.Value, &childPlan)
		if err != nil {
			return nil, 0, err
		}
		if childPlan.Warnings != nil {
			for index, warnings := range childPlan.Warnings {
				// in order to have the [] slice to match the hash
				if len(warnings) == 0 {
					rv.Warnings[index] = make([]string, 0)
				}
				rv.Warnings[index] = append(rv.Warnings[index], warnings...)
			}
		}

		for _, ipp := range childPlan.IndexPlanPIndexes {
			for _, lpp := range ipp.LeanPlanPIndexes {
				planPIndex := &PlanPIndex{
					Name:             lpp.Name,
					UUID:             lpp.UUID,
					IndexName:        ipp.IndexDef.Name,
					IndexType:        ipp.IndexDef.Type,
					IndexUUID:        ipp.IndexDef.UUID,
					IndexParams:      ipp.IndexDef.Params,
					SourceName:       ipp.IndexDef.SourceName,
					SourceType:       ipp.IndexDef.SourceType,
					SourceUUID:       ipp.IndexDef.SourceUUID,
					SourceParams:     ipp.IndexDef.SourceParams,
					SourcePartitions: lpp.SourcePartitions,
					Nodes:            lpp.Nodes,
				}
				rv.PlanPIndexes[lpp.Name] = planPIndex
			}
			leanPlanPIndexes.IndexPlanPIndexes[ipp.IndexDef.Name] = ipp
		}

		if rv.ImplVersion == "" ||
			VersionGTE(childPlan.ImplVersion, rv.ImplVersion) {
			rv.ImplVersion = childPlan.ImplVersion
		}
		if rv.UUID == "" {
			rv.UUID = childPlan.UUID
		}
	}
	if rv.ImplVersion == "" {
		rv.ImplVersion = VERSION
	}

	data, err := json.Marshal(rv)
	if err != nil {
		return nil, 0, err
	}
	// compare the hash of the fetched metakv content and
	// that from the the directory name stamp.
	hashMD5, err := computeMD5(data)
	if err != nil {
		log.Printf("cfg_metakv_lean: getLeanPlan, computeMD5, err: %v", err)
		return nil, 0, err
	}
	hashStart := len(leanPlanKeyPrefix)
	hashFromName := path[hashStart : hashStart+32]
	if hashFromName != hashMD5 {
		if attempt < maxRetry {
			goto RETRY
		}
		log.Printf("cfg_metakv_lean: getLeanPlan, hash mismatch between"+
			" plan contents: %s, and directory stamp: %s", hashMD5, hashFromName)
		return nil, 0, fmt.Errorf("cfg_metakv_lean: getLeanPlan, hash mismatch between"+
			" plan contents: %s, and directory stamp: %s", hashMD5, hashFromName)
	}

	casResult := c.lastSplitCAS + 1
	c.lastSplitCAS = casResult
	return data, casResult, nil
}

func delLeanPlan(
	c *CfgMetaKv, key string, cas uint64) error {
	// Check for any sharedPlan left overs and clean it.
	buf, _, _ := c.getRawLOCKED(key, cas)
	if len(buf) > 0 {
		delSharedPlan(c, key, cas)
	}
	// fetch the current planPIndex path first
	path, _, err := getCurMetaKvPlanPath(c)
	if err != nil {
		return err
	}

	return metakv.RecursiveDelete(path)
}

// purgeOrphanedLeanPlans purges all those planPIndexes directories
// which are left orphaned due to those rare metakv race scenarios
func purgeOrphanedLeanPlans(c *CfgMetaKv, curPath string) error {
	children, err := metakv.ListAllChildren(c.keyToPath("planPIndexesLean") + "/")
	if err != nil {
		log.Printf("cfg_metakv_lean: purgeOrphanedLeanPlans, err: %v", err)
		return err
	}

	// deduplicate the directory entries under the
	// "/fts/cbgt/cfg/planPIndexesLean/"
	// as ListAllChildren traverse recursively
	planDirPaths := make(map[string]bool, len(children))
	for _, v := range children {
		dirPath, _ := filepath.Split(v.Path)
		if _, seen := planDirPaths[dirPath]; !seen && curPath != dirPath {
			planDirPaths[dirPath] = true
		}
	}

	curTimeMs := time.Now().UnixNano() / 1000000
	// purge all those orphan lean planPIndex directory paths
	// which are older than 10 min
	for orphanPath := range planDirPaths {
		bornTimeStr := orphanPath[len(leanPlanKeyPrefix)+
			md5HashLength+1 : len(orphanPath)-1]
		// check if older than a min
		bornTimeMs, _ := strconv.Atoi(bornTimeStr)
		age := curTimeMs - int64(bornTimeMs)
		if age >= 600000 {
			err = metakv.RecursiveDelete(orphanPath)
			if err != nil {
				// errs are logged and ignored except the last one
				log.Printf("cfg_metakv_lean: purgeOrphanedLeanPlans, "+
					"err: %v", err)
			}
		}
	}
	return err
}

func getCurMetaKvPlanPath(c *CfgMetaKv) (string, interface{}, error) {
	path := c.keyToPath(curMetaKvPlanKey)
	v, cas, err := metakv.Get(path)
	if err != nil {
		log.Printf("cfg_metakv_lean: getCurMetaKvPlanPath, err: %v", err)
		return "", 0, err
	}
	return string(v), cas, nil
}

func computeMD5(payload []byte) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, bytes.NewReader(payload)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
