//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"container/list"
	"fmt"
	"io"
	"sync"

	log "github.com/couchbase/clog"
	"github.com/gorilla/mux"

	"github.com/rcrowley/go-metrics"
)

// PIndexImpl represents a runtime pindex implementation instance,
// whose runtime type depends on the pindex's type.
type PIndexImpl interface{}

// PIndexImplType defines the functions that every pindex
// implementation type must register on startup.
type PIndexImplType struct {
	// Invoked by the manager to customize the index definition
	// during creating or updating indexes (Optional).
	Prepare func(mgr *Manager, indexDef *IndexDef) (*IndexDef, error)

	// Invoked by the manager to validate the index definition
	// before going ahead with the actual creation (Optional).
	Validate func(indexType, indexName, indexParams string) error

	// Invoked by the manager on index deletion to clean up
	// any stats/resources pertaining to the index before removing
	// the index (Optional).
	OnDelete func(indexDef *IndexDef)

	// Invoked by the manager when it wants to create an index
	// partition.  The pindex implementation should persist enough
	// info into the path subdirectory so that it can reconstitute the
	// pindex during restart and Open().
	New func(indexType, indexParams, path string, restart func()) (
		PIndexImpl, Dest, error)

	// NewEx is an optional method that is invoked by the manager when
	// it wants to create an index partition. The pindex implementation
	// should persist enough info into the path subdirectory so that
	// it can reconstitute the pindex during restart and Open().
	NewEx func(indexType, indexParams, sourceParams, path string, mgr *Manager,
		restart func()) (PIndexImpl, Dest, error)

	// Hibernate is an optional method that is invoked for enterprise edition users
	// when a pindex is to be hibernated and uploaded to a remote object storage.
	Hibernate func(mgr *Manager, remotePath, pindexName, path string) error

	// Unhibernate is an optional method that is invoked for enterprise edition
	// users when a pindex is to be unhibernated after download from remote object storage
	// and made feedable.
	Unhibernate func(mgr *Manager, pindex *PIndex)

	// Invoked by the manager when it wants a pindex implementation to
	// reconstitute and reload a pindex instance back into the
	// process, such as when the process has re-started.
	Open func(indexType, path string, restart func()) (
		PIndexImpl, Dest, error)

	// Optional, invoked by the manager when it wants a pindex
	// implementation to reconstitute and reload a pindex instance
	// back into the process, with the updated index parameter values.
	OpenUsing func(indexType, path, indexParams string,
		restart func()) (PIndexImpl, Dest, error)

	// Invoked by the manager when it wants a count of documents from
	// an index.  The registered Count() function can be nil.
	Count func(mgr *Manager, indexName, indexUUID string) (
		uint64, error)

	// Invoked by the manager when it wants to query an index.  The
	// registered Query() function can be nil.
	Query func(mgr *Manager, indexName, indexUUID string,
		req []byte, res io.Writer) error

	// Description is used to populate docs, UI, etc, such as index
	// type drop-down control in the web admin UI.  Format of the
	// description string:
	//
	//    $categoryName/$indexType - short descriptive string
	//
	// The $categoryName is something like "advanced", or "general".
	Description string

	// A prototype instance of indexParams JSON that is usable for
	// Validate() and New().
	StartSample interface{}

	// Example instances of JSON that are usable for Query requests().
	// These are used to help generate API documentation.
	QuerySamples func() []Documentation

	// Displayed in docs, web admin UI, etc, and often might be a link
	// to even further help.
	QueryHelp string

	// Invoked during startup to allow pindex implementation to affect
	// the REST API with its own endpoint.
	InitRouter func(r *mux.Router, phase string, mgr *Manager)

	// Optional, additional handlers a pindex implementation may have
	// for /api/diag output.
	DiagHandlers []DiagHandler

	// Optional, allows pindex implementation to add more information
	// to the REST /api/managerMeta output.
	MetaExtra func(map[string]interface{})

	// Optional, allows pindex implementation to specify advanced UI
	// implementations and information.
	UI map[string]string

	// Optional, invoked for checking whether the pindex implementations
	// can effect the config changes through a restart of pindexes.
	AnalyzeIndexDefUpdates func(configUpdates *ConfigAnalyzeRequest) ResultCode

	// Invoked by the manager when it wants to trigger generic operations
	// on the index.
	SubmitTaskRequest func(mgr *Manager, indexName,
		indexUUID string, req []byte) (*TaskRequestStatus, error)
}

type Feedable interface {
	// IsFeedable implementation checks whether the current pindex
	// instance is ready for ingesting data from a Feed implementation.
	IsFeedable() (bool, error)
}

// ConfigAnalyzeRequest wraps up the various configuration
// parameters that the PIndexImplType implementations deals with.
type ConfigAnalyzeRequest struct {
	IndexDefnCur         *IndexDef
	IndexDefnPrev        *IndexDef
	SourcePartitionsCur  map[string]bool
	SourcePartitionsPrev map[string]bool
}

// ResultCode represents the return code indicative of the various operations
// recommended by the pindex implementations upon detecting a config change.
type ResultCode string

const (
	// PINDEXES_RESTART suggests a reboot of the pindexes
	PINDEXES_RESTART ResultCode = "request_restart_pindexes"
)

// PIndexImplTypes is a global registry of pindex type backends or
// implementations.  It is keyed by indexType and should be treated as
// immutable/read-only after process init/startup.
var PIndexImplTypes = make(map[string]*PIndexImplType)

// RegisterPIndexImplType registers a index type into the system.
func RegisterPIndexImplType(indexType string, t *PIndexImplType) {
	PIndexImplTypes[indexType] = t
}

// NewPIndexImpl creates an index partition of the given, registered
// index type.
func NewPIndexImpl(indexType, indexParams, path string, restart func()) (
	PIndexImpl, Dest, error) {
	t, exists := PIndexImplTypes[indexType]
	if !exists || t == nil || t.New == nil {
		return nil, nil,
			fmt.Errorf("pindex_impl: NewPIndexImpl indexType: %s",
				indexType)
	}

	return t.New(indexType, indexParams, path, restart)
}

// NewPIndexImplEx creates an index partition of the given, registered
// index type.
func NewPIndexImplEx(indexType, indexParams, sourceParams, path string,
	mgr *Manager, restart func()) (
	PIndexImpl, Dest, error) {
	t, exists := PIndexImplTypes[indexType]
	if !exists || t == nil || t.NewEx == nil {
		// fallback to default NewPIndexImpl implementation.
		return NewPIndexImpl(indexType, indexParams, path, restart)
	}

	return t.NewEx(indexType, indexParams, sourceParams, path, mgr, restart)
}

func Hibernate(mgr *Manager, indexType, remotePath, name, path string) error {
	t, exists := PIndexImplTypes[indexType]
	if !exists {
		return fmt.Errorf("pindex_impl: cannot hibernate since pindex "+
			" implementation type %s does not exist", indexType)
	}
	return t.Hibernate(mgr, remotePath, name, path)
}

func Unhibernate(mgr *Manager, pindex *PIndex) {
	t, exists := PIndexImplTypes[pindex.IndexType]
	if !exists {
		log.Errorf("pindex_impl: cannot unhibernate since pindex "+
			" implementation type %s does not exist", pindex.IndexType)
		return
	}
	t.Unhibernate(mgr, pindex)
}

// OpenPIndexImpl loads an index partition of the given, registered
// index type from a given path.
func OpenPIndexImpl(indexType, path string, restart func()) (
	PIndexImpl, Dest, error) {
	t, exists := PIndexImplTypes[indexType]
	if !exists || t == nil || t.Open == nil {
		return nil, nil, fmt.Errorf("pindex_impl: OpenPIndexImpl"+
			" indexType: %s", indexType)
	}

	return t.Open(indexType, path, restart)
}

// OpenPIndexImplUsing loads an index partition of the given, registered
// index type from a given path with the given indexParams.
func OpenPIndexImplUsing(indexType, path, indexParams string,
	restart func()) (PIndexImpl, Dest, error) {
	t, exists := PIndexImplTypes[indexType]
	if !exists || t == nil || t.OpenUsing == nil {
		return nil, nil, fmt.Errorf("pindex_impl: OpenPIndexImplUsing"+
			" indexType: %s", indexType)
	}

	return t.OpenUsing(indexType, path, indexParams, restart)
}

// PIndexImplTypeForIndex retrieves from the Cfg provider the index
// type for a given index.
func PIndexImplTypeForIndex(cfg Cfg, indexName string) (
	*PIndexImplType, error) {
	_, pindexImplType, err := GetIndexDef(cfg, indexName)

	return pindexImplType, err
}

// GetIndexDef retrieves the IndexDef and PIndexImplType for an index.
func GetIndexDef(cfg Cfg, indexName string) (
	*IndexDef, *PIndexImplType, error) {
	indexDefs, _, err := CfgGetIndexDefs(cfg)
	if err != nil || indexDefs == nil {
		return nil, nil, fmt.Errorf("pindex_impl: could not get indexDefs,"+
			" indexName: %s, err: %v",
			indexName, err)
	}

	indexDef := indexDefs.IndexDefs[indexName]
	if indexDef == nil {
		return nil, nil, fmt.Errorf("pindex_impl: no indexDef,"+
			" indexName: %s", indexName)
	}

	pindexImplType := PIndexImplTypes[indexDef.Type]
	if pindexImplType == nil {
		return nil, nil, fmt.Errorf("pindex_impl: no pindexImplType,"+
			" indexName: %s, indexDef.Type: %s",
			indexName, indexDef.Type)
	}

	return indexDef, pindexImplType, nil
}

// ------------------------------------------------

// QueryCtlParams defines the JSON that includes the "ctl" part of a
// query request.  These "ctl" query request parameters are
// independent of any specific pindex type.
type QueryCtlParams struct {
	Ctl QueryCtl `json:"ctl"`
}

// QueryCtl defines the JSON parameters that control query execution
// and which are independent of any specific pindex type.
//
// A PartitionSelection value can optionally be specified for performing
// advanced scatter gather operations, recognized options:
// - ""              : default behavior - active partitions are selected
// - local           : local partitions are favored, pseudo random selection from remote
// - random          : pseudo random selection from available local and remote
// - random_balanced : random selection from available local and remote nodes by
//                     distributing the query load across all nodes.
type QueryCtl struct {
	Timeout            int64              `json:"timeout"`
	Consistency        *ConsistencyParams `json:"consistency"`
	PartitionSelection string             `json:"partition_selection,omitempty"`
}

// QUERY_CTL_DEFAULT_TIMEOUT_MS is the default query timeout.
const QUERY_CTL_DEFAULT_TIMEOUT_MS = int64(10000)

// ------------------------------------------------

// PINDEX_STORE_MAX_ERRORS is the max number of errors that a
// PIndexStoreStats will track.
// Updates to this setting is not thread-safe.
var PINDEX_STORE_MAX_ERRORS = 40

// PIndexStoreStats provides some common stats/metrics and error
// tracking that some pindex type backends can reuse.
type PIndexStoreStats struct {
	TimerBatchStore metrics.Timer // Access protected by an internal lock

	m               sync.RWMutex // Mutex to protect following fields
	Errors          *list.List   // Capped list of string (json)
	TotalErrorCount uint64
}

func NewPIndexStoreStats() *PIndexStoreStats {
	return &PIndexStoreStats{
		TimerBatchStore: metrics.NewTimer(),
		Errors:          list.New(),
	}
}

func (d *PIndexStoreStats) AddError(err string) {
	d.m.Lock()
	if d.Errors.Len() >= PINDEX_STORE_MAX_ERRORS {
		d.Errors.Remove(d.Errors.Front())
	}
	d.Errors.PushBack(err)
	d.TotalErrorCount++
	d.m.Unlock()
}

func (d *PIndexStoreStats) WriteJSON(w io.Writer) {
	w.Write([]byte(`{"TimerBatchStore":`))
	WriteTimerJSON(w, d.TimerBatchStore)

	d.m.RLock()
	if d.Errors != nil {
		w.Write([]byte(`,"Errors":[`))
		e := d.Errors.Front()
		i := 0
		for e != nil {
			j, ok := e.Value.(string)
			if ok && j != "" {
				if i > 0 {
					w.Write(JsonComma)
				}
				w.Write([]byte(j))
			}
			e = e.Next()
			i = i + 1
		}
		w.Write([]byte(`]`))
	}
	d.m.RUnlock()

	w.Write(JsonCloseBrace)
}

func (d *PIndexStoreStats) FetchTotalErrorCount() uint64 {
	d.m.RLock()
	defer d.m.RUnlock()

	return d.TotalErrorCount
}

var prefixPIndexStoreStats = []byte(`{"pindexStoreStats":`)
