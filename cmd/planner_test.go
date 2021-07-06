//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"testing"
)

func TestParseOptionsBool(t *testing.T) {
	v := ParseOptionsBool(map[string]string{}, "x", "", false)
	exp := false
	if v != exp {
		t.Errorf("unexpected")
	}

	v = ParseOptionsBool(map[string]string{"x": "true"}, "x", "", false)
	exp = true
	if v != exp {
		t.Errorf("unexpected")
	}

	v = ParseOptionsBool(map[string]string{"x": ""}, "x", "", false)
	exp = false
	if v != exp {
		t.Errorf("unexpected")
	}

	v = ParseOptionsBool(map[string]string{"x": ""}, "x", "y", false)
	exp = false
	if v != exp {
		t.Errorf("unexpected")
	}

	v = ParseOptionsBool(map[string]string{"x-y": "true", "x": "false"},
		"x", "y", false)
	exp = true
	if v != exp {
		t.Errorf("unexpected")
	}
}
