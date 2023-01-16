//  Copyright 2014-Present Couchbase, Inc.
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
	"io"
	"os"
	"reflect"

	log "github.com/couchbase/clog"
)

func init() {
	RegisterPIndexImplType("blackhole", &PIndexImplType{
		New:                    NewBlackHolePIndexImpl,
		Open:                   OpenBlackHolePIndexImpl,
		OpenUsing:              OpenBlackHolePIndexImplUsing,
		Count:                  nil, // Content of blackhole isn't countable.
		Query:                  nil, // Content of blackhole isn't queryable.
		AnalyzeIndexDefUpdates: restartOnIndexDefChanges,
		Description: "advanced/blackhole" +
			" - a blackhole index ignores all data and is not queryable;" +
			" used for testing",
	})
}

func NewBlackHolePIndexImpl(indexType, indexParams,
	path string, restart func()) (PIndexImpl, Dest, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, nil, err
	}

	err = os.WriteFile(path+string(os.PathSeparator)+"black.hole",
		EMPTY_BYTES, 0600)
	if err != nil {
		return nil, nil, err
	}

	dest := &BlackHole{path: path}
	return dest, dest, nil
}

func OpenBlackHolePIndexImpl(indexType, path string, restart func()) (
	PIndexImpl, Dest, error) {
	return OpenBlackHolePIndexImplUsing(indexType, path, "", restart)
}

func OpenBlackHolePIndexImplUsing(indexType, path, indexParams string, restart func()) (
	PIndexImpl, Dest, error) {
	buf, err := os.ReadFile(path + string(os.PathSeparator) + "black.hole")
	if err != nil {
		return nil, nil, err
	}
	if len(buf) > 0 {
		return nil, nil, fmt.Errorf("blackhole: expected empty black.hole")
	}

	dest := &BlackHole{path: path}
	return dest, dest, nil
}

// ---------------------------------------------------------

// Implements both Dest and PIndexImpl interfaces.
type BlackHole struct {
	path string
}

func (t *BlackHole) Close() error {
	return nil
}

func (t *BlackHole) DataUpdate(partition string,
	key []byte, seq uint64, val []byte,
	cas uint64,
	extrasType DestExtrasType, extras []byte) error {
	return nil
}

func (t *BlackHole) DataDelete(partition string,
	key []byte, seq uint64,
	cas uint64,
	extrasType DestExtrasType, extras []byte) error {
	return nil
}

func (t *BlackHole) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	return nil
}

func (t *BlackHole) OpaqueGet(partition string) (
	value []byte, lastSeq uint64, err error) {
	return nil, 0, nil
}

func (t *BlackHole) OpaqueSet(partition string, value []byte) error {
	return nil
}

func (t *BlackHole) Rollback(partition string, rollbackSeq uint64) error {
	return nil
}

func (t *BlackHole) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	return nil
}

func (t *BlackHole) Count(pindex *PIndex,
	cancelCh <-chan bool) (uint64, error) {
	return 0, nil
}

func (t *BlackHole) Query(pindex *PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return nil
}

func (t *BlackHole) Stats(w io.Writer) error {
	_, err := w.Write(JsonNULL)
	return err
}

func reloadableIndexDefParamChange(paramPrev, paramCur string) bool {
	if paramPrev == paramCur {
		log.Printf("reloadableSourceParamsChange returned true")
		return true
	}

	if len(paramPrev) == 0 {
		// make it a json unmarshal-able string
		paramPrev = "{}"
	}

	var prevMap map[string]interface{}
	err := json.Unmarshal([]byte(paramPrev), &prevMap)
	if err != nil {
		log.Printf("pindex_bleve: reloadableSourceParamsChange"+
			" json parse paramPrev: %s, err: %v",
			paramPrev, err)
		return false
	}

	if len(paramCur) == 0 {
		// make it a json unmarshal-able string
		paramCur = "{}"
	}

	var curMap map[string]interface{}
	err = json.Unmarshal([]byte(paramCur), &curMap)
	if err != nil {
		log.Printf("pindex_bleve: reloadableSourceParamsChange"+
			" json parse paramCur: %s, err: %v",
			paramCur, err)
		return false
	}

	// any parsing err doesn't matter here.
	po, _ := ParseFeedAllotmentOption(paramPrev)
	co, _ := ParseFeedAllotmentOption(paramCur)
	if po != co {
		prevMap["feedAllotment"] = ""
		curMap["feedAllotment"] = ""
	}

	return reflect.DeepEqual(prevMap, curMap)
}

func reloadableSourceParamsChange(paramPrev, paramCur string) bool {
	if paramPrev == paramCur {
		return true
	}

	var prevMap map[string]interface{}
	err := json.Unmarshal([]byte(paramPrev), &prevMap)
	if err != nil {
		log.Printf("pindex_impl_blackhole: reloadableSourceParamsChange"+
			" json parse paramPrev: %s, err: %v",
			paramPrev, err)
		return false
	}

	var curMap map[string]interface{}
	err = json.Unmarshal([]byte(paramCur), &curMap)
	if err != nil {
		log.Printf("pindex_impl_blackhole: reloadableSourceParamsChange"+
			" json parse paramCur: %s, err: %v",
			paramCur, err)
		return false
	}

	// any parsing err doesn't matter here.
	po, _ := ParseFeedAllotmentOption(paramPrev)
	co, _ := ParseFeedAllotmentOption(paramCur)
	if po != co {
		prevMap["feedAllotment"] = ""
		curMap["feedAllotment"] = ""
	}

	return reflect.DeepEqual(prevMap, curMap)
}

// restartOnIndexDefChanges checks whether the changes in the indexDefns are
// quickly adoptable over a reboot of the pindex implementations.
// eg: kvstore configs updates like compaction percentage.
func restartOnIndexDefChanges(
	configRequest *ConfigAnalyzeRequest) ResultCode {
	if configRequest == nil || configRequest.IndexDefnCur == nil ||
		configRequest.IndexDefnPrev == nil {
		return ""
	}
	if configRequest.IndexDefnPrev.Name != configRequest.IndexDefnCur.Name ||
		configRequest.IndexDefnPrev.SourceName !=
			configRequest.IndexDefnCur.SourceName ||
		configRequest.IndexDefnPrev.SourceType !=
			configRequest.IndexDefnCur.SourceType ||
		configRequest.IndexDefnPrev.SourceUUID !=
			configRequest.IndexDefnCur.SourceUUID ||
		!reloadableSourceParamsChange(configRequest.IndexDefnPrev.SourceParams,
			configRequest.IndexDefnCur.SourceParams) ||
		configRequest.IndexDefnPrev.Type !=
			configRequest.IndexDefnCur.Type ||
		!reflect.DeepEqual(configRequest.SourcePartitionsCur,
			configRequest.SourcePartitionsPrev) ||
		!reloadableIndexDefParamChange(configRequest.IndexDefnPrev.Params,
			configRequest.IndexDefnCur.Params) {
		return ""
	}
	return PINDEXES_RESTART
}
