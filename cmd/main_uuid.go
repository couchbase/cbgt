//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

// MainUUID is a helper function for cmd-line tool developers, that
// reuses a previous "baseName.uuid" file from the dataDir if it
// exists, or generates a brand new UUID (and persists it).
func MainUUID(baseName, dataDir string) (string, error) {
	uuid := cbgt.NewUUID()
	uuidPath := dataDir + string(os.PathSeparator) + baseName + ".uuid"
	uuidBuf, err := os.ReadFile(uuidPath)
	if err == nil {
		uuid = strings.TrimSpace(string(uuidBuf))
		if uuid == "" {
			return "", fmt.Errorf("error: could not parse uuidPath: %s",
				uuidPath)
		}
		log.Printf("main: manager uuid: %s", uuid)
		log.Printf("main: manager uuid was reloaded")
	} else {
		log.Printf("main: manager uuid: %s", uuid)
		log.Printf("main: manager uuid was generated")
	}
	err = os.WriteFile(uuidPath, []byte(uuid), 0600)
	if err != nil {
		return "", fmt.Errorf("error: could not write uuidPath: %s\n"+
			"  Please check that your -data/-dataDir parameter (%q)\n"+
			"  is to a writable directory where %s can store\n"+
			"  index data.",
			uuidPath, dataDir, baseName)
	}
	return uuid, nil
}
