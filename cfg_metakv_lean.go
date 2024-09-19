//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
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

// curMetaKvPlanKey is the source of truth for the meta kv data for the
// current plan
var curMetaKvPlanKey = "curMetaKvPlanKey"

var leanPlanKeyPrefix = "planPIndexesLean/planPIndexesLean-"

var md5HashLength = 32

var PlanPurgeTimeout = int64(90000) // 15 min default

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
	PlannerVersion   string                     `json:"plannerVersion"`
}

// planMeta represents the json contents of curMetaKvPlanKey
type planMeta struct {
	Path        string `json:"path"`
	UUID        string `json:"uuid"`
	ImplVersion string `json:"implVersion"`
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

	log.Printf("cfg_metakv_lean: setLeanPlan, val: %s", val)

	planPIndexes := &PlanPIndexes{}
	err = UnmarshalJSON(val, planPIndexes)
	if err != nil {
		return 0, err
	}

	// check whether adv meta encoding is supported.
	if isAdvMetaEncodingSupported(c) {
		_, err = setSplitPlan(c, key, planPIndexes, newPath)
		if err != nil {
			return 0, err
		}

	} else {
		_, err = setLeanPlanUtil(c, key, planPIndexes, newPath)
		if err != nil {
			return 0, err
		}
	}

	// update the curPlanMetaKvKey to point to the latest plan directory
	meta := &planMeta{UUID: planPIndexes.UUID,
		ImplVersion: planPIndexes.ImplVersion,
		Path:        newPath,
	}
	metaJSON, err := MarshalJSON(meta)
	if err != nil {
		metakv.RecursiveDelete(newPath)
		return 0, err
	}

	metaJSON, err = c.compressLocked(metaJSON)
	if err != nil {
		return 0, err
	}

	err = metakv.Set(c.keyToPath(curMetaKvPlanKey), metaJSON, nil)
	if err != nil {
		log.Printf("cfg_metakv_lean: setLeanPlan, curMetaKvPlanKey "+
			"set, err: %v", err)
		metakv.RecursiveDelete(newPath)
		return 0, err
	}

	log.Printf("cfg_metakv_lean: setLeanPlan, curMetaKvPlanKey "+
		"set, val: %s", metaJSON)

	casResult := c.lastSplitCAS + 1
	c.lastSplitCAS = casResult
	// purge any orphaned, old enough lean planPIndexes
	purgeOrphanedLeanPlans(c, newPath)
	return casResult, err
}

// setLeanPlan writes the lean plan split by the index name.
func setLeanPlanUtil(c *CfgMetaKv, key string,
	planPIndexes *PlanPIndexes, path string) (uint64, error) {
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
				PlannerVersion:   ppi.PlannerVersion,
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
		val, err := MarshalJSON(childPlan)
		if err != nil {
			// clean up the incomplete plan directories
			log.Errorf("cfg_metakv_lean: setLeanPlan json marshal, err: %v", err)
			metakv.RecursiveDelete(path)
			return 0, err
		}
		childPath := path + name

		val, err = c.compressLocked(val)
		if err != nil {
			return 0, err
		}

		err = metakv.Set(childPath, val, nil)
		if err != nil {
			// clean up the incomplete plan directories
			log.Errorf("cfg_metakv_lean: setLeanPlan metakv.Set, err: %v", err)
			metakv.RecursiveDelete(path)
			return 0, err
		}
		log.Printf("cfg_metakv_lean: setLeanPlan, key: %v, childPath: %v",
			key, childPath)
	}

	return 0, nil
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
	// fetch the current planPIndex meta first
	planMeta, err := getCurMetaKvPlanMeta(c)
	if err != nil {
		return nil, 0, err
	}
	// looks like an upgrade scenario as no leanPlan meta set yet,
	// hence fallback to older shared plan
	if planMeta == nil {
		return getSharedPlan(c, key, cas)
	}
	// looks like no active plan exists
	if planMeta != nil && planMeta.Path == "" {
		return nil, 0, nil
	}

	children, err := metakv.ListAllChildren(planMeta.Path)
	if err != nil {
		return nil, 0, err
	}

	var rv *PlanPIndexes
	var data []byte
	var hashMD5 string

	// check for split plans.
	if isSplitPlan(children) {
		data, hashMD5, err = getSplitPlan(c, planMeta, children)
		if err != nil {
			return nil, 0, err
		}
	} else {
		rv, err = getLeanPlanUtil(c, planMeta, children)
		if err != nil {
			return nil, 0, err
		}
		data, err = MarshalJSON(rv)
		if err != nil {
			return nil, 0, err
		}
		hashMD5, err = computeMD5(data)
		if err != nil {
			log.Errorf("cfg_metakv_lean: getLeanPlan, computeMD5, err: %v", err)
			return nil, 0, err
		}
	}

	// compare the hash of the fetched metakv content and
	// that from the the directory name stamp.
	hashStart := len(leanPlanKeyPrefix)
	hashFromName := planMeta.Path[hashStart : hashStart+32]
	if hashFromName != hashMD5 {
		if attempt < maxRetry {
			goto RETRY
		}
		err = fmt.Errorf("cfg_metakv_lean: getLeanPlan, hash mismatch"+
			" between plan hash: %s contents: %s, and directory stamp: %s",
			hashMD5, data, hashFromName)
		log.Printf("%+v", err)
		return data, 0, nil
	}

	casResult := c.lastSplitCAS + 1
	c.lastSplitCAS = casResult
	return data, casResult, nil
}

func getLeanPlanUtil(c *CfgMetaKv, planMeta *planMeta,
	children []metakv.KVEntry) (*PlanPIndexes, error) {
	rv := &PlanPIndexes{
		PlanPIndexes: make(map[string]*PlanPIndex),
		Warnings:     make(map[string][]string),
	}
	rv.UUID = planMeta.UUID
	rv.ImplVersion = planMeta.ImplVersion

	leanPlanPIndexes := &LeanPlanPIndexes{
		IndexPlanPIndexes: make(map[string]*LeanIndexPlanPIndexes),
	}

	for _, v := range children {
		var childPlan LeanPlanPIndexes

		value, err := c.uncompressLocked(v.Value)
		if err != nil {
			return nil, err
		}

		err = UnmarshalJSON(value, &childPlan)
		if err != nil {
			return nil, err
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
					PlannerVersion:   lpp.PlannerVersion,
				}
				rv.PlanPIndexes[lpp.Name] = planPIndex
			}
			leanPlanPIndexes.IndexPlanPIndexes[ipp.IndexDef.Name] = ipp

			if childPlan.Warnings != nil {
				if warnings, exists := childPlan.Warnings[ipp.IndexDef.Name]; exists {
					if len(warnings) == 0 {
						rv.Warnings[ipp.IndexDef.Name] = make([]string, 0)
					}
					rv.Warnings[ipp.IndexDef.Name] = append(rv.Warnings[ipp.IndexDef.Name], warnings...)
				}
			}
		}

		// use the lowest version among childPlans
		if rv.ImplVersion == "" ||
			!VersionGTE(childPlan.ImplVersion, rv.ImplVersion) {
			rv.ImplVersion = childPlan.ImplVersion
		}
		if rv.UUID == "" {
			rv.UUID = childPlan.UUID
		}
	}

	if rv.ImplVersion == "" {
		version := VERSION
		v, _, err := c.getRawLOCKED(VERSION_KEY, 0)
		if err == nil {
			version = string(v)
		}
		rv.ImplVersion = version
	}

	return rv, nil
}

func delLeanPlan(
	c *CfgMetaKv, key string, cas uint64) error {
	// Check for any sharedPlan left overs and clean it.
	buf, _, _ := c.getRawLOCKED(key, cas)
	if len(buf) > 0 {
		delSharedPlan(c, key, cas)
	}
	// fetch the current planPIndex path
	meta, err := getCurMetaKvPlanMeta(c)
	if err != nil || meta == nil {
		return err
	}
	err = metakv.RecursiveDelete(meta.Path)
	if err != nil {
		log.Printf("cfg_metakv_lean: delLeanPlan, RecursiveDelete,"+
			" err: %v", err)
		return err
	}
	log.Printf("cfg_metakv_lean: delLeanPlan, RecursiveDelete,"+
		" path: %s", meta.Path)

	// set the plan meta Path to empty, to prevent the fallback
	// to the sharedPlan once upgraded
	meta.Path = ""
	metaJSON, err := MarshalJSON(meta)
	if err != nil {
		return err
	}

	metaJSON, err = c.compressLocked(metaJSON)
	if err != nil {
		return err
	}

	err = metakv.Set(c.keyToPath(curMetaKvPlanKey), metaJSON, nil)
	if err != nil {
		log.Printf("cfg_metakv_lean: delLeanPlan, curMetaKvPlanKey "+
			"Set, err: %v", err)
		return err
	}
	log.Printf("cfg_metakv_lean: delLeanPlan, curMetaKvPlanKey,"+
		" val: %s", metaJSON)
	return nil
}

// purgeOrphanedLeanPlans purges all those planPIndexes directories
// which are left orphaned due to those rare metakv race scenarios
func purgeOrphanedLeanPlans(c *CfgMetaKv, curPath string) error {
	children, err := metakv.ListAllChildren(c.keyToPath("planPIndexesLean") + "/")
	if err != nil {
		log.Errorf("cfg_metakv_lean: purgeOrphanedLeanPlans, err: %v", err)
		return err
	}

	// deduplicate the directory entries under the
	// "/fts/cbgt/cfg/planPIndexesLean/"
	// as ListAllChildren traverse recursively
	curDir, _ := filepath.Split(curPath)
	planDirPaths := make(map[string]bool, len(children))
	for _, v := range children {
		dirPath, _ := filepath.Split(v.Path)
		if _, seen := planDirPaths[dirPath]; !seen && curPath != dirPath &&
			curDir != dirPath {
			planDirPaths[dirPath] = true
		}
	}

	curTimeMs := time.Now().UnixNano() / 1000000
	// purge all those orphan lean planPIndex directory paths
	// which are older than PlanPurgeTimeout value
	for orphanPath := range planDirPaths {
		if strings.HasPrefix(orphanPath, leanPlanKeyPrefix) {
			bornTimeStr := orphanPath[len(leanPlanKeyPrefix)+
				md5HashLength+1 : len(orphanPath)-1]
			// check if older than PlanPurgeTimeout
			bornTimeMs, _ := strconv.Atoi(bornTimeStr)
			age := curTimeMs - int64(bornTimeMs)
			if age >= PlanPurgeTimeout {
				err = metakv.RecursiveDelete(orphanPath)
				if err != nil {
					// errs are logged and ignored except the last one
					log.Printf("cfg_metakv_lean: purgeOrphanedLeanPlans, "+
						"err: %v", err)
				}
				log.Printf("cfg_metakv_lean: purgeOrphanedLeanPlans, "+
					" purged path: %s", orphanPath)
			}
		}
	}
	return err
}

func getCurMetaKvPlanMeta(c *CfgMetaKv) (*planMeta, error) {
	path := c.keyToPath(curMetaKvPlanKey)
	v, _, err := metakv.Get(path)
	if err != nil {
		log.Errorf("cfg_metakv_lean: getCurMetaKvPlanMeta, err: %v", err)
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}

	v, err = c.uncompressLocked(v)
	if err != nil {
		return nil, err
	}

	meta := &planMeta{}
	err = UnmarshalJSON(v, meta)
	if err != nil {
		log.Errorf("cfg_metakv_lean: getCurMetaKvPlanMeta, json err: %v", err)
		return nil, err
	}

	return meta, nil
}

func computeMD5(payload []byte) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, bytes.NewReader(payload)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
