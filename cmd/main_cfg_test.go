//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestMainCfg(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	bindHttp := "10.1.1.20:8095"
	register := "wanted"

	cfg, err := MainCfg("cbgt", "an unknown cfg provider",
		bindHttp, register, emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected MainCfg() to fail on unknown provider")
	}

	cfg, err = MainCfg("cbgt", "simple", bindHttp, register, emptyDir)
	if err != nil || cfg == nil {
		t.Errorf("expected MainCfg() to work on simple provider")
	}

	if _, err = cfg.Set("k", []byte("value"), 0); err != nil {
		t.Errorf("expected Set() to work")
	}

	cfg, err = MainCfg("cbgt", "simple",
		bindHttp, register, emptyDir)
	if err != nil || cfg == nil {
		t.Errorf("expected MainCfg() ok simple provider when reload")
	}

	cfg, err = MainCfg("cbgt", "couchbase:http://",
		bindHttp, register, emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected err on bad url")
	}

	cfg, err = MainCfg("cbgt", "couchbase:http://user:pswd@127.0.0.1:666",
		bindHttp, register, emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected err on bad server")
	}

	if false { // metakv skipped due to log spam.
		cfg, err = MainCfg("cbgt", "metakv",
			bindHttp, register, emptyDir)
		if err != nil || cfg == nil {
			t.Errorf("expected no err metakv cfg")
		}
	}
}
