//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build metakv_test
// +build metakv_test

package cbgt

import (
	"testing"
)

// This test attempts to set index defs, first with an outdated CAS,
// leading to a CAS mismatch error and then with the latest CAS.
func TestCfgSetIndexDefs(t *testing.T) {
	options := make(map[string]string)
	options["nsServerUrl"] = "http://127.0.0.0:9000"

	cfg, err := NewCfgMetaKv(NewUUID(), options)
	if err != nil {
		t.Errorf("Error getting Cfg is %v", err)
		return
	}

	indexDefs := NewIndexDefs(CfgGetVersion(cfg))
	indexDefs.IndexDefs["testIndex"] = &IndexDef{UUID: NewUUID(), Name: "testIndex"}

	// Forcibly setting it in metakv since the metakv mock has no index defs.
	cas, err := CfgSetIndexDefs(cfg, indexDefs, CFG_CAS_FORCE)
	if err != nil {
		t.Errorf("Error setting index defs: %v", err)
		return
	}
	t.Logf("CAS on setting is now %d", cas)

	indexDefs, cas1, err := CfgGetIndexDefs(cfg)
	if err != nil {
		t.Errorf("Error getting index defs: %v", err)
		return
	}
	t.Logf("CAS on getting index defs is  %d", cas1)

	indexDefs, cas2, err := CfgGetIndexDefs(cfg)
	if err != nil {
		t.Errorf("Error getting index defs: %v", err)
		return
	}
	t.Logf("CAS on getting is %d", cas2)

	indexDefs.UUID = NewUUID()
	cas3, err := CfgSetIndexDefs(cfg, indexDefs, cas1)
	t.Logf("CAS on getting index defs again is %d", cas3)
	if err != nil {
		// expect a CAS mismatch error since setting it with cas1
		// (not the latest CAS value).
		if _, ok := err.(*CfgCASError); !ok {
			t.Errorf("Did not expect an error other than CAS mismatch: %v", err)
			return
		}
	}

	indexDefs, latestCAS, err := CfgGetIndexDefs(cfg)
	indexDefs.UUID = NewUUID()
	_, err = CfgSetIndexDefs(cfg, indexDefs, latestCAS)
	if err != nil {
		if _, ok := err.(*CfgCASError); ok {
			t.Errorf("Unexpected CAS mismatch error.")
		} else {
			t.Errorf("Error setting index defs with CAS %d: %v", latestCAS, err)
		}
	}
}
