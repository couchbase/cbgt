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

	log "github.com/couchbase/clog"
)

// The cbgt.VERSION tracks persistence versioning (schema/format of
// persisted data and configuration).  The main.VERSION from "git
// describe" that's part of an executable command, in contrast, is an
// overall "product" version.  For example, we might introduce new
// UI-only features or fix a UI typo, in which case we'd bump the
// main.VERSION number; but, if the persisted data/config format was
// unchanged, then the cbgt.VERSION number should remain unchanged.
//
// NOTE: You *must* update cbgt.VERSION if you change what's stored in
// the Cfg (such as the JSON/struct definitions or the planning
// algorithms).
const VERSION = "5.5.0"
const VERSION_KEY = "version"

// Returns true if a given version is modern enough to modify the Cfg.
// Older versions (which are running with older JSON/struct definitions
// or planning algorithms) will see false from their CheckVersion()'s.
func CheckVersion(cfg Cfg, myVersion string) (bool, error) {
	tries := 0
	for cfg != nil {
		tries += 1
		if tries > 100 {
			return false,
				fmt.Errorf("version: CheckVersion too many tries")
		}

		clusterVersion, cas, err := cfg.Get(VERSION_KEY, 0)
		if err != nil {
			return false, err
		}

		if clusterVersion == nil {
			// First time initialization, so save myVersion to cfg and
			// retry in case there was a race.
			_, err = cfg.Set(VERSION_KEY, []byte(myVersion), cas)
			if err != nil {
				if _, ok := err.(*CfgCASError); ok {
					// Retry if it was a CAS mismatch due to
					// multi-node startup races.
					continue
				}
				return false, fmt.Errorf("version:"+
					" could not save VERSION to cfg, err: %v", err)
			}
			log.Printf("version: CheckVersion, Cfg version updated %s",
				myVersion)
			continue
		}

		// this check is retained to keep the same behaviour of
		// preventing the older versions to override the newer
		// version Cfgs. Now a Cfg version bump happens only when
		// all nodes in cluster are on a given homogeneous version.
		if VersionGTE(myVersion, string(clusterVersion)) == false {
			return false, nil
		}

		if myVersion != string(clusterVersion) {
			bumpVersion, err := VerifyEffectiveClusterVersion(cfg, myVersion)
			if err != nil {
				return false, err
			}
			// CheckVersion passes even if no bump version is required
			if !bumpVersion {
				log.Printf("version: CheckVersion, no bump for current Cfg"+
					" verion: %s", clusterVersion)
				return true, nil
			}

			// Found myVersion is higher than the clusterVersion and
			// all cluster nodes are on the same myVersion, so save
			// myVersion to cfg and retry in case there was a race.
			_, err = cfg.Set(VERSION_KEY, []byte(myVersion), cas)
			if err != nil {
				if _, ok := err.(*CfgCASError); ok {
					// Retry if it was a CAS mismatch due to
					// multi-node startup races.
					continue
				}
				return false, fmt.Errorf("version:"+
					" could not update VERSION in cfg, err: %v", err)
			}
			log.Printf("version: CheckVersion, Cfg version updated %s",
				myVersion)
			continue
		}

		return true, nil
	}

	return false, nil
}

// VerifyEffectiveClusterVersion checks the cluster version values, and
// if the cluster contains any node which is lower than the given
// myVersion, then return false
func VerifyEffectiveClusterVersion(cfg interface{}, myVersion string) (bool, error) {
	// first check with the ns_server for clusterCompatibility value
	// On any errors in retrieving the values there, fallback to
	// nodeDefinitions level version checks
	if rsc, ok := cfg.(VersionReader); ok {
		ccVersion, err := retry(3, rsc.ClusterVersion)
		if err != nil {
			log.Printf("version: RetrieveNsServerCompatibility, err: %v", err)
			goto NODEDEFS_CHECKS
		}

		appVersion, err := CompatibilityVersion(CfgAppVersion)
		if appVersion != ccVersion {
			log.Printf("version: non matching application compatibility "+
				"version: %d and clusterCompatibility version: %d",
				appVersion, ccVersion)
			return false, nil
		}
		if err != nil {
			log.Printf("version: CompatibilityVersion, err: %v", err)
			goto NODEDEFS_CHECKS
		}

		log.Printf("version: clusterCompatibility: %d matches with"+
			" application version: %d", ccVersion, appVersion)
		return true, err
	}

NODEDEFS_CHECKS:
	// fallback in case ns_server checks errors out for unknown reasons
	if cfg, ok := cfg.(Cfg); ok {
		for _, k := range []string{NODE_DEFS_KNOWN, NODE_DEFS_WANTED} {
			key := CfgNodeDefsKey(k)
			v, _, err := cfg.Get(key, 0)
			if err != nil {
				return false, err
			}

			if v == nil {
				// no existing nodes in cluster
				continue
			}

			nodeDefs := &NodeDefs{}
			err = json.Unmarshal(v, nodeDefs)
			if err != nil {
				return false, err
			}

			for _, node := range nodeDefs.NodeDefs {
				if myVersion != node.ImplVersion &&
					VersionGTE(myVersion, node.ImplVersion) {
					log.Printf("version: version: %s lower than myVersion: %s"+
						" found", node.ImplVersion, myVersion)
					return false, nil
				}
			}
		}
	}

	return true, nil
}

func retry(attempts int, f func() (uint64, error)) (val uint64, err error) {
	if val, err = f(); err != nil {
		if attempts > 0 {
			retry(attempts-1, f)
		}
	}
	return val, err
}
