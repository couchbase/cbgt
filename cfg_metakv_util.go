//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/couchbase/cbauth/metakv"
	log "github.com/couchbase/clog"
)

// splitPlanKeySuffix serves as a suffix to identify
// the new plan keys writtern as per the split plan.
const splitPlanKeySuffix = "-$#$"

// defaultMaxSizePerKey acts as the maximum size threshold for the
// amount of data writtern per key.
const defaultMaxSizePerKey = int(100000) // 100KB.

// gzip header constants for checking the header presence.
const (
	gzipID1     = 0x1f
	gzipID2     = 0x8b
	gzipDeflate = 8
	osValue     = 0xff
)

// compressLocked apply compression to the given value if the feature
// is supported in the cluster.
func (c *CfgMetaKv) compressLocked(val []byte) ([]byte, error) {
	// check whether the advanced meta encoding feature is supported.
	if len(val) == 0 || !isAdvMetaEncodingSupported(c) {
		return val, nil
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(val)
	if err != nil {
		return nil, err
	}

	zw.Close()
	return buf.Bytes(), nil
}

// uncompressLocked deflate the given value if needed.
func (c *CfgMetaKv) uncompressLocked(val []byte) ([]byte, error) {
	// if no gzip header values are found, then return the value.
	if len(val) < 10 || (val[0] != gzipID1 || val[1] != gzipID2 ||
		val[2] != gzipDeflate || val[9] != osValue) {
		return val, nil
	}

	zr, err := gzip.NewReader(bytes.NewBuffer(val))
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	result, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// setSplitPlan writes the plan in sequential chunks of max size
// defaultMaxSizePerKey.
func setSplitPlan(c *CfgMetaKv, key string, planPIndexes *PlanPIndexes,
	path string) (uint64, error) {
	val, err := json.Marshal(planPIndexes)
	if err != nil {
		return 0, err
	}

	val, err = c.compressLocked(val)
	if err != nil {
		return 0, err
	}

	var childID int
	for len(val) > 0 {
		var chunk []byte
		if len(val) > defaultMaxSizePerKey {
			chunk = val[:defaultMaxSizePerKey]
			val = val[defaultMaxSizePerKey:]
		} else {
			chunk = val
			val = val[:0]
		}

		childPath := path + fmt.Sprintf("%d%s", childID, splitPlanKeySuffix)
		err = metakv.Set(childPath, chunk, nil)
		if err != nil {
			// clean up the incomplete plan directories
			log.Errorf("cfg_metakv_utils: setSplitPlan metakv.Set, err: %v", err)
			metakv.RecursiveDelete(path)
			return 0, err
		}
		log.Printf("cfg_metakv_utils: setSplitPlan, key: %v, childPath: %v",
			key, childPath)

		childID++
	}

	return 0, nil
}

// getSplitPlan fetches the split plan.
func getSplitPlan(c *CfgMetaKv, planMeta *planMeta,
	children []metakv.KVEntry) ([]byte, string, error) {
	// iterate the children in the numeric order to weave back the plan.
	sort.Slice(children, func(i, j int) bool {
		path := children[i].Path
		strID := path[strings.LastIndex(path, "/")+1 : len(path)-4]
		child1ID, err := strconv.ParseInt(strID, 10, 64)
		if err != nil {
			return children[i].Path < children[j].Path
		}

		path = children[j].Path
		strID = path[strings.LastIndex(path, "/")+1 : len(path)-4]
		child2ID, err := strconv.ParseInt(strID, 10, 64)
		if err != nil {
			return children[i].Path < children[j].Path
		}

		return child1ID < child2ID
	})

	var res []byte
	for _, v := range children {
		res = append(res, v.Value...)
	}

	val, err := c.uncompressLocked(res)
	if err != nil {
		return nil, "", err
	}

	hashMD5, err := computeMD5(val)
	if err != nil {
		log.Errorf("cfg_metakv_util: getSplitPlan, computeMD5, err: %v", err)
		return nil, "", err
	}

	return val, hashMD5, nil
}

// isAdvMetaEncodingSupported checks whether the feature is
// supported by the cluster.
func isAdvMetaEncodingSupported(c *CfgMetaKv) bool {
	if atomic.LoadInt32(&c.advMetaEncodingSupported) != 1 &&
		!c.isFeatureSupported(AdvMetaEncodingFeatureVersion,
			NodeFeatureAdvMetaEncoding) {
		return false
	}
	atomic.StoreInt32(&c.advMetaEncodingSupported, 1)
	return true
}

// isSplitPlan checks whether the given entries points to split plan keys.
func isSplitPlan(children []metakv.KVEntry) bool {
	for _, v := range children {
		if !strings.HasSuffix(v.Path, splitPlanKeySuffix) {
			return false
		}
	}
	return true
}

// isFeatureSupported checks whether the given feature flag is supported
// by the nodes in the cluster as well as the effective cluster version
// is greater than or equal to the given minimum version requirement.
func (c *CfgMetaKv) isFeatureSupported(minVersion string,
	featureFlag string) bool {
	b, _, err := c.getRawLOCKED(VERSION_KEY, 0)
	if err != nil {
		return false
	}
	// if the Cfg version is lower than minVersion
	// then return false.
	if VersionGTE(string(b), minVersion) == false {
		return false
	}

	// extra check to see if the cluster contains any nodes which
	// dont support the given feature flag.
	for _, k := range []string{NODE_DEFS_KNOWN, NODE_DEFS_WANTED} {
		key := CfgNodeDefsKey(k)
		handler := cfgMetaKvAdvancedKeys[key]
		if handler == nil {
			return false
		}

		v, _, err := handler.get(c, key, 0)
		if err != nil {
			log.Printf("cfg_metakv_util: isFeatureSupported, flag: %s,"+
				" err: %v", featureFlag, err)
			return false
		}
		if v == nil {
			return false
		}
		rv := &NodeDefs{}
		err = json.Unmarshal(v, rv)
		if err != nil {
			log.Printf("cfg_metakv_util: isFeatureSupported, flag: %s,"+
				" json unmarshal, err: %v", featureFlag, err)
			return false
		}

		if !IsFeatureSupportedByCluster(featureFlag, rv) {
			return false
		}
	}
	return true
}
