//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"reflect"
	"testing"

	"github.com/couchbase/gocbcore/v10"
)

func TestManifestCollectionIDs(t *testing.T) {
	manifest := &gocbcore.Manifest{
		UID: 511,
		Scopes: []gocbcore.ManifestScope{
			{UID: 0, Name: "_default", Collections: []gocbcore.ManifestCollection{
				{UID: 0, Name: "_default"},
			}},
			{UID: 9, Name: "scope_9", Collections: []gocbcore.ManifestCollection{
				{UID: 0x1fd, Name: "coll_49"},
				{UID: 0x1fe, Name: "coll_50"},
				{UID: 0x1ff, Name: "coll_51"},
			}},
		},
	}

	tests := []struct {
		name        string
		scope       string
		collections []string
		expIDs      []uint32
		expErr      bool
	}{
		{"single collection", "scope_9", []string{"coll_49"},
			[]uint32{0x1fd}, false},
		{"subset of collections", "scope_9", []string{"coll_51", "coll_49"},
			[]uint32{0x1ff, 0x1fd}, false},
		{"whole scope", "scope_9", nil,
			[]uint32{0x1fd, 0x1fe, 0x1ff}, false},
		{"default collection", "_default", []string{"_default"},
			[]uint32{0}, false},
		{"missing collection", "scope_9", []string{"coll_49", "coll_99"},
			nil, true},
		{"collection of another scope", "_default", []string{"coll_49"},
			nil, true},
		{"missing scope", "scope_x", []string{"coll_49"},
			nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ids, err := manifestCollectionIDs(manifest, test.scope,
				test.collections)
			if test.expErr {
				if err == nil {
					t.Fatalf("expected an error, got ids: %v", ids)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !reflect.DeepEqual(ids, test.expIDs) {
				t.Errorf("expected ids: %v, got: %v", test.expIDs, ids)
			}
		})
	}
}
