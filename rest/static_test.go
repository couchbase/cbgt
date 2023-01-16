//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"os"
	"testing"
)

func TestAssetFS(t *testing.T) {
	// Get code coverage for the assets embedded into
	// bindata_assetfs.go via the
	// github.com/elazarl/go-bindata-assetfs tool.
	if AssetFS() == nil {
		t.Errorf("expected an assetFS")
	}

	d, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(d)

	err := RestoreAssets(d, "static")
	if err != nil {
		t.Errorf("expected RestoreAssets to work, err: %v", err)
	}
}
