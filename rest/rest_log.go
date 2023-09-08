//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"

	"github.com/couchbase/cbgt"
)

// TODO: Need to give the codebase a scrub of its log
// messages and fmt.Errorf()'s.

// LogGetHandler is a REST handler that retrieves recent log messages.
type LogGetHandler struct {
	mgr *cbgt.Manager
	mr  *cbgt.MsgRing
}

func NewLogGetHandler(
	mgr *cbgt.Manager, mr *cbgt.MsgRing) *LogGetHandler {
	return &LogGetHandler{mgr: mgr, mr: mr}
}

func (h *LogGetHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(`{"messages":[`))
	if h.mr != nil {
		for i, message := range h.mr.Messages() {
			buf, err := cbgt.MarshalJSON(string(message))
			if err == nil {
				if i > 0 {
					w.Write(cbgt.JsonComma)
				}
				w.Write(buf)
			}
		}
	}
	w.Write([]byte(`],"events":[`))
	if h.mgr != nil {
		first := true
		h.mgr.VisitEvents(func(event []byte) {
			if !first {
				w.Write(cbgt.JsonComma)
			}
			first = false
			w.Write(event)
		})
	}
	w.Write([]byte(`]}`))
}
