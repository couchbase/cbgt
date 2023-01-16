//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"os"
	"testing"
)

func TestMainUUID(t *testing.T) {
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	uuid, err := MainUUID("cbgt", emptyDir)
	if err != nil || uuid == "" {
		t.Errorf("expected MainUUID() to work, err: %v", err)
	}

	uuid2, err := MainUUID("cbgt", emptyDir)
	if err != nil || uuid2 != uuid {
		t.Errorf("expected MainUUID() reload to give same uuid,"+
			" uuid: %s vs %s, err: %v", uuid, uuid2, err)
	}

	path := emptyDir + string(os.PathSeparator) + "cbgt.uuid"
	os.Remove(path)
	os.WriteFile(path, []byte{}, 0600)

	uuid3, err := MainUUID("cbgt", emptyDir)
	if err == nil || uuid3 != "" {
		t.Errorf("expected MainUUID() to fail on empty file")
	}
}
