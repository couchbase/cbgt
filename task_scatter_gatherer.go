//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"encoding/json"
	"fmt"
	"sync"
)

// TaskRequestHandler represents the interface that
// need to implemented by the partitions for using
// the task scatter gatherer.
type TaskRequestHandler interface {
	HandleTask([]byte) (*TaskRequestStatus, error)
	Name() string
}

// TaskRequest represent a generic task request like
// "compact" or "encrypt" for partitions
type TaskRequest struct {
	Op             string                 `json:"op"`
	UUID           string                 `json:"uuid"`
	Contents       map[string]interface{} `json:"contents,omitempty"`
	PartitionNames []string               `json:"partitionNames,omitempty"`
}

// PartitionErrMap tracks errors with the name
// of the partition where it occurred
type PartitionErrMap map[string]error

// TaskPartitionStatusMap tracks the current state
/// of a task across the partitions
type TaskPartitionStatusMap map[string]string

// MarshalJSON seralizes the error into a string for JSON consumption
func (pem PartitionErrMap) MarshalJSON() ([]byte, error) {
	tmp := make(map[string]string, len(pem))
	for k, v := range pem {
		tmp[k] = v.Error()
	}
	return json.Marshal(tmp)
}

func (pem PartitionErrMap) UnmarshalJSON(data []byte) error {
	var tmp map[string]string
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	for k, v := range tmp {
		pem[k] = fmt.Errorf("%s", v)
	}
	return nil
}

type TaskRequestStatus struct {
	Request    *TaskRequest           `json:"request"`
	Total      int                    `json:"total"`
	Failed     int                    `json:"failed"`
	Successful int                    `json:"successful"`
	Errors     PartitionErrMap        `json:"errors,omitempty"`
	Status     TaskPartitionStatusMap `json:"status,omitempty"`
}

func (trs *TaskRequestStatus) Merge(other *TaskRequestStatus) {
	trs.Total += other.Total
	trs.Successful += other.Successful
	trs.Failed += other.Failed
	if len(other.Errors) > 0 {
		if trs.Errors == nil {
			trs.Errors = make(map[string]error)
		}
		for oPartition, oErr := range other.Errors {
			trs.Errors[oPartition] = oErr
		}
	}

	if len(other.Status) > 0 {
		if trs.Status == nil {
			trs.Status = make(map[string]string)
		}
		for pname, state := range other.Status {
			trs.Status[pname] = state
		}
	}

	if trs.Request == nil {
		trs.Request = other.Request
	}
}

type partialResultStatus struct {
	name      string
	err       error
	reqStatus *TaskRequestStatus
}

func ScatterTaskRequest(req []byte,
	partitions []TaskRequestHandler) (*TaskRequestStatus, error) {
	var waitGroup sync.WaitGroup
	asyncResults := make(chan *partialResultStatus, len(partitions))

	var scatterRequest = func(in TaskRequestHandler, childReq []byte) {
		rv := partialResultStatus{name: in.Name()}
		rv.reqStatus, rv.err = in.HandleTask(childReq)
		asyncResults <- &rv
		waitGroup.Done()
	}

	waitGroup.Add(len(partitions))
	for _, p := range partitions {
		go scatterRequest(p, req)
	}

	// on another go routine, close after finished
	go func() {
		waitGroup.Wait()
		close(asyncResults)
	}()

	sr := &TaskRequestStatus{}
	partitionErrs := make(map[string]error)

	for asr := range asyncResults {
		if asr.err == nil {
			if sr == nil {
				sr = asr.reqStatus
			} else {
				sr.Merge(asr.reqStatus)
			}
		} else {
			partitionErrs[asr.name] = asr.err
		}
	}

	if len(partitionErrs) > 0 {
		if sr.Errors == nil {
			sr.Errors = make(map[string]error)
		}
		for indexName, indexErr := range partitionErrs {
			sr.Errors[indexName] = indexErr
			sr.Total++
			sr.Failed++
		}
	}

	return sr, nil
}
