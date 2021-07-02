//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"testing"
)

func TestSyncWorkReq(t *testing.T) {
	ch := make(chan *workReq)
	go func() {
		w, ok := <-ch
		if !ok || w == nil {
			t.Errorf("expected ok and w")
		}
		if w.op != "op" || w.msg != "msg" {
			t.Errorf("expected op and msg")
		}
		close(w.resCh)
		w, ok = <-ch
		if ok || w != nil {
			t.Errorf("expected done")
		}
	}()

	err := syncWorkReq(ch, "op", "msg", nil)
	if err != nil {
		t.Errorf("expect nil err")
	}
	close(ch)
}
