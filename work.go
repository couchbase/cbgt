//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import "runtime"

const WORK_NOOP = ""
const WORK_KICK = "kick"

// A workReq represents an asynchronous request for work or a task,
// where results can be awaited upon via the resCh.
type workReq struct {
	op    string      // The operation.
	msg   string      // Some simple msg as part of the request.
	obj   interface{} // Any extra params for the request.
	resCh chan error  // Response/result channel.
}

// syncWorkReq makes a workReq request and synchronously awaits for a
// resCh response.
func syncWorkReq(ch chan *workReq, op, msg string, obj interface{}) error {
	resCh := make(chan error)
	ch <- &workReq{op: op, msg: msg, obj: obj, resCh: resCh}
	return <-resCh
}

func getWorkerCount(itemCount int) int {
	ncpu := runtime.NumCPU()
	if itemCount < ncpu {
		return itemCount
	}
	return ncpu
}
