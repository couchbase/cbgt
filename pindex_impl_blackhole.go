//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"

	log "github.com/couchbase/clog"
)

func init() {
	RegisterPIndexImplType("blackhole", &PIndexImplType{
		New:       NewBlackHolePIndexImpl,
		Open:      OpenBlackHolePIndexImpl,
		OpenUsing: OpenBlackHolePIndexImplUsing,
		Count:     nil, // Content of blackhole isn't countable.
		Query:     nil, // Content of blackhole isn't queryable.
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

	err = ioutil.WriteFile(path+string(os.PathSeparator)+"black.hole",
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
	buf, err := ioutil.ReadFile(path + string(os.PathSeparator) + "black.hole")
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
	// place holder implementation
	return true
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
