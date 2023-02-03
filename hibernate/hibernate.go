//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package hibernate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rebalance"
	"github.com/couchbase/cbgt/rest/monitor"
	log "github.com/couchbase/clog"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objval"
)

type OperationType string

var (
	INDEX_METADATA_PATH = "index-metadata"

	SOURCE_PARTITIONS_PATH = "source-partitions"

	// This hook is used to obtain the bucket and path in remote object
	// storage, used when checking if a remote path exists.
	GetRemoteBucketAndPathHook = func(remotePath string) (string, string, error) {
		return "", "", fmt.Errorf("not-implemented")
	}

	BucketStateTrackerHook = func(*cbgt.Manager, string, string) (int, error) {
		return 0, fmt.Errorf("not-implemented")
	}

	// This hook is used to download the index definitions and source partitions metadata from the
	// remote object store, to be used when unhibernating.
	DownloadMetadataHook = func(client objcli.Client, bucket, remotePath string) ([]byte, error) {
		return nil, fmt.Errorf("not-implemented")
	}

	// This hook is used to upload the index definitions and source partitions metadata to the
	// remote object store, to be used when hibernating.
	UploadMetadataHook = func(client objcli.Client, ctx context.Context, bucket, remotePath string,
		data []byte) error {
		return fmt.Errorf("not-implemented")
	}

	// This hook checks if a given remote path is valid i.e. follows the format
	// expected of the object store's paths.
	CheckIfRemotePathIsValidHook = func(string) bool {
		// not implemented
		return false
	}
)

// HibernationProgress represents progress status information as the
// Pause/Resume() operation proceeds.
type HibernationProgress struct {
	Error error
	Index string

	// Map of pindex -> transfer progress in range of 0 to 1.
	TransferProgress map[string]float64
}

type HibernationOptions struct {
	BucketName string
	SourceType string

	ArchiveLocation string

	StatsSampleErrorThreshold *int

	// number of indexes to be hibernated currently
	MaxConcurrentIndexMoves int

	Log HibernationLogFunc

	// Optional, defaults to http.Get(); this is used, for example,
	// for unit testing.
	HttpGet func(url string) (resp *http.Response, err error)

	Verbose int

	Manager *cbgt.Manager

	DryRun bool
}

type HibernationLogFunc func(format string, v ...interface{})

// A Manager struct holds all the tracking information for the
// pause/resume (hibernation) operations.
type Manager struct {
	version       string // See cbgt.Manager's version.
	server        string
	cfg           cbgt.Cfg // See cbgt.Manager's cfg.
	options       HibernationOptions
	operationType OperationType
	progressCh    chan HibernationProgress

	monitor             *monitor.MonitorNodes
	monitorDoneCh       chan struct{}
	monitorSampleCh     chan monitor.MonitorSample
	monitorSampleWantCh chan chan monitor.MonitorSample

	nodesAll             []string
	indexDefsToHibernate *cbgt.IndexDefs

	m                sync.Mutex
	transferProgress map[string]float64 // pindex -> pause/resume progress
	stopCh           chan struct{}

	hibernationComplete bool // status flag to check if hibernation for
	// the bucket(i.e. all the indexes to hibernate) is complete.
}

func (hm *Manager) getIndexesForHibernation(begIndexDefs *cbgt.IndexDefs) (*cbgt.IndexDefs, error) {
	indexDefsToHibernate := cbgt.NewIndexDefs(hm.version)

	for _, indexDef := range begIndexDefs.IndexDefs {
		if indexDef.SourceName == hm.options.BucketName {
			indexDefsToHibernate.IndexDefs[indexDef.Name] = indexDef
		}
	}

	return indexDefsToHibernate, nil
}

func (hm *Manager) checkIfIndexMetadataExists() (bool, error) {
	client := hm.options.Manager.GetObjStoreClient()
	if client == nil {
		return false, fmt.Errorf("hibernate: failed to get object store client")
	}

	bkt, prefix, err := GetRemoteBucketAndPathHook(hm.options.ArchiveLocation)
	if err != nil {
		return false, err
	}

	stp := errors.New("stop")
	err = client.IterateObjects(context.Background(), bkt, prefix+"/"+INDEX_METADATA_PATH, "/",
		nil, nil,
		func(attrs *objval.ObjectAttrs) error { return stp })
	if err != nil && err != stp {
		return false, err // Purposefully not wrapped
	}
	return true, nil
}

func (hm *Manager) downloadIndexMetadata() (*cbgt.IndexDefs, error) {
	client := hm.options.Manager.GetObjStoreClient()
	if client == nil {
		return nil, fmt.Errorf("hibernate: failed to get object store client")
	}

	bucket, key, _, err := getBucketAndMetadataPaths(hm.options.ArchiveLocation)
	if err != nil {
		return nil, err
	}

	data, err := DownloadMetadataHook(client, bucket, key)
	if err != nil {
		return nil, err
	}

	indexDefs := new(cbgt.IndexDefs)
	err = json.Unmarshal(data, indexDefs)
	if err != nil {
		return nil, err
	}
	return indexDefs, err
}

// This function returns the S3 bucket and path for index metadata and
// source partitions.
// The remote path is of the form: s3://<s3-bucket-name>/<key>
func getBucketAndMetadataPaths(remotePath string) (string, string, string, error) {
	bucket, key, err := GetRemoteBucketAndPathHook(remotePath)
	if err != nil {
		return "", "", "", err
	}
	indexDefsKey := key + "/" + INDEX_METADATA_PATH
	sourcePartitionsKey := key + "/" + SOURCE_PARTITIONS_PATH

	return bucket, indexDefsKey, sourcePartitionsKey, nil
}

// Common start for both pause and resume.
func Start(version string, cfg cbgt.Cfg, server string, options HibernationOptions,
	hibernationType OperationType) (*Manager, error) {

	begIndexDefs, begNodeDefs, begPlanPIndexes, _, err :=
		cbgt.PlannerGetPlan(cfg, version, "")
	if err != nil {
		return nil, err
	}

	hm := &Manager{
		version:       version,
		cfg:           cfg,
		server:        server,
		options:       options,
		operationType: hibernationType,
		progressCh:    make(chan HibernationProgress),
	}

	var indexDefsToHibernate *cbgt.IndexDefs
	if hibernationType == OperationType(cbgt.HIBERNATE_TASK) {
		indexDefsToHibernate, err = hm.getIndexesForHibernation(begIndexDefs)
		if err != nil {
			return nil, err
		}
	} else if hibernationType == OperationType(cbgt.UNHIBERNATE_TASK) {
		// Checking if the metadata file exists in the path since it will
		// be downloaded during resume.
		exists, err := hm.checkIfIndexMetadataExists()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("hibernate: index metadata path does not exist")
		}

		indexDefsToHibernate, err = hm.downloadIndexMetadata()
		if err != nil {
			return nil, err
		}

		for _, index := range indexDefsToHibernate.IndexDefs {
			hm.options.SourceType = index.SourceType
			break
		}
	}

	nodesAll, _, _, _, _ := cbgt.CalcNodesLayout(begIndexDefs,
		begNodeDefs, begPlanPIndexes)

	urlUUIDs := monitor.NodeDefsUrlUUIDs(begNodeDefs)
	authURLUUIDs := make([]monitor.UrlUUID, 0)
	for _, urlUUID := range urlUUIDs {
		cbauthURL, _ := cbgt.CBAuthURL(urlUUID.Url)
		authURLUUIDs = append(authURLUUIDs, monitor.UrlUUID{Url: cbauthURL,
			UUID: urlUUID.UUID})
	}

	monitorSampleCh := make(chan monitor.MonitorSample)

	monitorOptions := monitor.MonitorNodesOptions{
		// Disabling fetching of sequence number stats since those cannot
		// be fetched during pause.
		StatsSampleDisable: true,
		DiagSampleDisable:  true,
		HttpGet:            options.HttpGet,
	}

	monitorInst, err := monitor.StartMonitorNodes(authURLUUIDs,
		monitorSampleCh, monitorOptions)
	if err != nil {
		return nil, err
	}

	transferProgress := make(map[string]float64)

	hm.indexDefsToHibernate = indexDefsToHibernate
	hm.nodesAll = nodesAll
	hm.monitor = monitorInst
	hm.monitorDoneCh = make(chan struct{})
	hm.monitorSampleCh = monitorSampleCh
	hm.monitorSampleWantCh = make(chan chan monitor.MonitorSample)
	hm.stopCh = make(chan struct{})
	hm.progressCh = make(chan HibernationProgress)
	hm.transferProgress = transferProgress

	go hm.runMonitor()

	go hm.runHibernateIndexes()

	return hm, nil
}

// --------------------------------------------------------

func (r *Manager) Logf(fmt string, v ...interface{}) {
	if r.options.Verbose < 0 {
		return
	}

	if r.options.Verbose < len(fmt) &&
		fmt[r.options.Verbose] == ' ' {
		return
	}

	f := r.options.Log
	if f == nil {
		f = log.Printf
	}

	f(fmt, v...)
}

// --------------------------------------------------------

type sourceMetadata struct {
	SourceName       string `json:"sourceName"`
	SourcePartitions string `json:"sourcePartitions"`
}

func DropRemotePaths(mgr *cbgt.Manager, indexDefsToModify *cbgt.IndexDefs) {
	err := cbgt.RetryOnCASMismatch(func() error {
		var err error

		indexDefs, cas, err := cbgt.CfgGetIndexDefs(mgr.Cfg())
		if err != nil {
			return nil
		}

		var indexesWithRemotePath bool

		for _, indexDef := range indexDefsToModify.IndexDefs {
			if _, exists := indexDefs.IndexDefs[indexDef.Name]; exists {
				if indexDef.HibernationPath != "" {
					indexesWithRemotePath = true
					indexDef.UUID = cbgt.NewUUID()
					indexDef.HibernationPath = ""
					indexDefs.IndexDefs[indexDef.Name] = indexDef
				}
			}
		}

		// If the index remote paths have already been changed, avoid doing
		// it another time.
		if !indexesWithRemotePath {
			return nil
		}

		indexDefs.UUID = cbgt.NewUUID()
		indexDefs.ImplVersion = indexDefsToModify.ImplVersion

		_, err = cbgt.CfgSetIndexDefs(mgr.Cfg(), indexDefs, cas)
		return err
	}, 100)
	if err != nil {
		log.Errorf("hibernate: DropRemotePaths: err: %v", err)
		return
	}
}

func (hm *Manager) resetHibernationPaths() {
	hm.options.ArchiveLocation = ""

	DropRemotePaths(hm.options.Manager, hm.indexDefsToHibernate)
}

func (hm *Manager) startResumeBucketStateTracker() {
	status, err := BucketStateTrackerHook(hm.options.Manager, cbgt.UNHIBERNATE_TASK,
		hm.options.BucketName)

	hm.options.Manager.ResetBucketTrackedForHibernation()
	hm.options.Manager.UnregisterBucketTracker()

	if err != nil {
		log.Errorf("hibernate: resume: error tracking bucket %s: %v",
			hm.options.BucketName, err)
		return
	}

	hm.options.Manager.SetOption(cbgt.UNHIBERNATE_TASK, "", true)

	if status == -1 {
		log.Printf("hibernate: unhibernation failed, deleting indexes for "+
			"bucket %s", hm.options.BucketName)

		hm.options.Manager.DeleteAllIndexFromSource(hm.options.SourceType,
			hm.options.BucketName, "")
	} else if status == 1 {
		hm.options.ArchiveLocation = ""
		err = hm.removeHibernationPath(hm.indexDefsToHibernate)
		if err != nil {
			hm.Logf("hibernate: error removing path: %v", err)
			return
		}
	}
}

func (hm *Manager) startPauseBucketStateTracker() {
	status, err := BucketStateTrackerHook(hm.options.Manager, cbgt.HIBERNATE_TASK,
		hm.options.BucketName)

	hm.options.Manager.ResetBucketTrackedForHibernation()
	hm.options.Manager.UnregisterBucketTracker()

	if err != nil {
		log.Errorf("hibernate: pause: error tracking bucket %s: %v",
			hm.options.BucketName, err)
		return
	}

	hm.options.Manager.SetOption(cbgt.HIBERNATE_TASK, "", true)

	if status == 1 {
		log.Printf("hibernate: hibernation succeeded, deleting indexes for "+
			"bucket %s", hm.options.BucketName)

		hm.options.Manager.DeleteAllIndexFromSource(hm.options.SourceType,
			hm.options.BucketName, "")
	} else if status == -1 {
		log.Errorf("hibernate: hibernation has failed, undoing pause changes for bucket %s.",
			hm.options.BucketName)
		hm.resetHibernationPaths()
	}
}

func (hm *Manager) runHibernateIndexes() {
	var err error
	defer func() {
		hm.Stop()

		hm.monitor.Stop()
		// Completion of hibernation operation, whether naturally or due
		// to error/Stop(), needs this cleanup.  Wait for runMonitor()
		// to finish as it may have more sends to progressCh.
		<-hm.monitorDoneCh

		if err != nil {
			hm.progressCh <- HibernationProgress{Error: err}
		}
		close(hm.progressCh)
	}()

	err = hm.hibernateIndexes(hm.indexDefsToHibernate)
	if err != nil {
		hm.Logf("run: err: %#v", err)
		return
	}

	// At this point, it indicates that the hibernation has been completed
	// for all the indexes.
	hm.hibernationComplete = true
}

// --------------------------------------------------------

func (hm *Manager) pauseIndexes() error {
	// Starting to track bucket state.
	go hm.startPauseBucketStateTracker()

	pauseFunc := func() error {
		indexDefs, cas, err := cbgt.CfgGetIndexDefs(hm.cfg)
		if err != nil {
			return err
		}

		if indexDefs == nil {
			indexDefs = cbgt.NewIndexDefs(hm.version)
		}

		for _, indexDef := range hm.indexDefsToHibernate.IndexDefs {
			if _, exists := indexDefs.IndexDefs[indexDef.Name]; exists {
				indexDef.UUID = cbgt.NewUUID()
				indexDef.HibernationPath = hm.options.ArchiveLocation
				indexDefs.IndexDefs[indexDef.Name] = indexDef
			}
		}

		indexDefs.UUID = cbgt.NewUUID()
		indexDefs.ImplVersion = hm.indexDefsToHibernate.ImplVersion

		if err = hm.uploadMetadata(); err != nil {
			return err
		}

		_, err = cbgt.CfgSetIndexDefs(hm.cfg, indexDefs, cas)
		return err
	}

	return cbgt.RetryOnCASMismatch(pauseFunc, 100)
}

func (hm *Manager) uploadMetadata() error {
	client := hm.options.Manager.GetObjStoreClient()
	if client == nil {
		return fmt.Errorf("hibernate: unable to get object store client")
	}

	ctx, _ := hm.options.Manager.GetHibernationContext()

	// uploading index defs metadata
	indexDefs := cbgt.NewIndexDefs(hm.version)
	indexDefs.UUID = cbgt.NewUUID()

	var index *cbgt.IndexDef
	for _, hibIndex := range hm.indexDefsToHibernate.IndexDefs {
		indexDefs.IndexDefs[hibIndex.Name] = hibIndex
		if index == nil {
			index = hibIndex
		}
	}

	data, err := json.Marshal(indexDefs)
	if err != nil {
		return fmt.Errorf("hibernate: error marshalling index defs: %v", err)
	}

	bucket, indexUploadPath, sourcePartitionsUploadPath, err :=
		getBucketAndMetadataPaths(hm.options.ArchiveLocation)
	if err != nil {
		return err
	}

	err = UploadMetadataHook(client, ctx, bucket, indexUploadPath, data)
	if err != nil {
		return err
	}

	_, indexPlansMap, err := hm.options.Manager.GetPlanPIndexes(true)
	if err != nil {
		return fmt.Errorf("hibernate: error getting plan pindexes: %v",
			err)
	}

	sourcePartitions := ""
	if _, exists := indexPlansMap[index.Name]; exists {
		for i := 0; i < len(indexPlansMap[index.Name])-1; i++ {
			sourcePartitions = sourcePartitions + indexPlansMap[index.Name][i].SourcePartitions + ","
		}
	}
	sourcePartitions = sourcePartitions + indexPlansMap[index.Name][len(indexPlansMap[index.Name])-1].SourcePartitions

	var sourcePartitionsMetadata = sourceMetadata{
		SourceName:       index.SourceName,
		SourcePartitions: sourcePartitions,
	}

	data, err = json.Marshal(sourcePartitionsMetadata)
	if err != nil {
		return nil
	}

	return UploadMetadataHook(client, ctx, bucket, sourcePartitionsUploadPath, data)
}

func (hm *Manager) UpdateIndexParams(indexDef *cbgt.IndexDef, uuid string) {
	indexDef.UUID = uuid
	log.Printf("hibernate: changing metadata path to %s", hm.options.ArchiveLocation)
	// Update index params with remote path
	indexDef.HibernationPath = hm.options.ArchiveLocation
	indexDef.Params = hm.AppendHibernatePath(indexDef.Params)
}

func (hm *Manager) AppendHibernatePath(params string) string {
	hibernateField := "\"hibernate\":"
	if strings.Index(params, hibernateField) == -1 {
		return params[0:len(params)-1] + ",\"hibernate\":\"" +
			hm.options.ArchiveLocation + "\"}"
	}

	pos := strings.Index(params, hibernateField) + len(hibernateField)
	firstComma := strings.Index(params[pos:], ",")

	return params[:pos] + "\"" + hm.options.ArchiveLocation + "\"" + params[firstComma+pos:]
}

func (hm *Manager) downloadSourcePartitionsMetadata() (*sourceMetadata, error) {
	client := hm.options.Manager.GetObjStoreClient()
	if client == nil {
		return nil, fmt.Errorf("hibernate: failed to get object store client")
	}

	bucket, _, key, err := getBucketAndMetadataPaths(hm.options.ArchiveLocation)
	if err != nil {
		return nil, err
	}

	data, err := DownloadMetadataHook(client, bucket, key)
	if err != nil {
		return nil, err
	}

	sourcePartitionsMetadata := new(sourceMetadata)
	err = json.Unmarshal(data, sourcePartitionsMetadata)
	if err != nil {
		return nil, err
	}

	return sourcePartitionsMetadata, nil
}

func (hm *Manager) resumeIndexes(indexesToResume *cbgt.IndexDefs) error {
	if len(indexesToResume.IndexDefs) == 0 || indexesToResume == nil {
		return nil
	}

	if hm.options.DryRun {
		// In a dry run, checking if the indexes in the remote path can be added to the
		// cluster based on their metadata.
		return hm.options.Manager.CheckIfIndexesCanBeAdded(hm.indexDefsToHibernate)
	}

	go hm.startResumeBucketStateTracker()

	sourcePartitionsMetadata, err := hm.downloadSourcePartitionsMetadata()
	if err != nil {
		return err
	}
	hm.options.Manager.SetOption("hibernationSourcePartitions", sourcePartitionsMetadata.SourcePartitions, true)

	indexResumeFunc := func() error {
		indexDefs, cas, err := cbgt.CfgGetIndexDefs(hm.cfg)
		if err != nil {
			return err
		}

		if indexDefs == nil {
			indexDefs = cbgt.NewIndexDefs(hm.version)
		}

		for _, indexDef := range hm.indexDefsToHibernate.IndexDefs {
			if _, exists := indexDefs.IndexDefs[indexDef.Name]; !exists {
				hm.UpdateIndexParams(indexDef, indexDef.UUID)
				indexDefs.IndexDefs[indexDef.Name] = indexDef
			}
		}

		indexDefs.UUID = cbgt.NewUUID()
		indexDefs.ImplVersion = hm.indexDefsToHibernate.ImplVersion

		_, err = cbgt.CfgSetIndexDefs(hm.cfg, indexDefs, cas)
		return err
	}

	return cbgt.RetryOnCASMismatch(indexResumeFunc, 100)
}

func (hm *Manager) hibernateIndexes(indexDefs *cbgt.IndexDefs) error {
	if hm.operationType == OperationType(cbgt.HIBERNATE_TASK) {
		err := hm.pauseIndexes()
		if err != nil {
			return err
		}
	} else {
		err := hm.resumeIndexes(indexDefs)
		if err != nil {
			return err
		}
	}

	// Dry runs do not involve any file transfers.
	if !hm.options.DryRun {
		return hm.waitUntilFileTransferDone(indexDefs)
	}

	return nil
}

// This function removes the remote paths from indexes.
func (hm *Manager) removeHibernationPath(indexesToChange *cbgt.IndexDefs) error {
	return cbgt.RetryOnCASMismatch(func() error {
		indexDefs, cas, err := cbgt.CfgGetIndexDefs(hm.cfg)
		if err != nil {
			return err
		}

		for _, index := range indexesToChange.IndexDefs {
			var indexDef *cbgt.IndexDef

			if _, exists := indexDefs.IndexDefs[index.Name]; !exists {
				return fmt.Errorf("hibernate: index def does not exist")
			} else {
				indexDef = indexDefs.IndexDefs[index.Name]
			}

			hm.UpdateIndexParams(indexDef, cbgt.NewUUID())
			indexDefs.IndexDefs[indexDef.Name] = indexDef
		}

		indexDefs.UUID = cbgt.NewUUID()
		indexDefs.ImplVersion = cbgt.CfgGetVersion(hm.cfg)

		_, err = cbgt.CfgSetIndexDefs(hm.cfg, indexDefs, cas)
		return err
	}, 100)
}

// --------------------------------------------------------

func (hm *Manager) waitUntilFileTransferDone(indexDefs *cbgt.IndexDefs) error {
	// listen to the stats from the hibernation channels and
	// wait until the bytes transferred and bytes expected matches.

	var caughtUp bool
MONITOR_LOOP:
	for !caughtUp {
		sampleWantCh := make(chan monitor.MonitorSample)
		select {
		case <-hm.stopCh:
			// Returning an error since stopping before completion
			// indicates an aborted pause/resume.
			return fmt.Errorf("hibernate: stopped while waiting")

		case hm.monitorSampleWantCh <- sampleWantCh:
			var sampleErr error
			for sample := range sampleWantCh {
				if sample.Error != nil {
					sampleErr = sample.Error
					continue
				}

				if sample.Kind == "/api/stats?partitions=true&seqno=false" {
					m := struct {
						Status map[string]struct {
							CopyStats struct {
								TransferProgress float64 `json:"TransferProgress"`
							} `json:"copyPartitionStats"`
						} `json:"pindexes"`
					}{}

					err := json.Unmarshal(sample.Data, &m)
					if err != nil {
						// not counted as sample error since this can be a transient error.
						continue
					}

					if len(m.Status) == 0 {
						continue
					}

					// index -> progress map
					// Each index has only 1 pindex; so the map can contain index -> progress
					indexProgress := make(map[string]float64)
					indexCount := make(map[string]int)

					hm.m.Lock()
					for nodePIndex, progress := range hm.transferProgress {
						split := strings.Split(nodePIndex, ":")
						// Here, index-metadata progress is not considered; only the transfer progress
						if len(split) <= 1 {
							continue MONITOR_LOOP
						}
						indexName, err := hm.options.Manager.GetIndexNameForPIndex(split[1])
						if err != nil {
							log.Errorf("hibernate: error getting index: %v", err)
							continue MONITOR_LOOP
						}
						// This is used for older pindexes which are now deleted.
						if indexName != "" {
							indexProgress[indexName] += progress
							indexCount[indexName] += 1
						}
					}
					hm.m.Unlock()

					for k := range indexProgress {
						indexProgress[k] /= float64(indexCount[k])
					}

					if len(indexProgress) > 0 {
						hm.progressCh <- HibernationProgress{TransferProgress: indexProgress}
					}

					for _, index := range indexDefs.IndexDefs {
						// If even one index has not completed their transfer, continue monitoring.
						if indexProgress[index.Name] < 1 {
							caughtUp = false
							continue MONITOR_LOOP
						}
					}

					caughtUp = true

					if sampleErr != nil {
						log.Printf("hibernate: hibernation complete for indexes,despite error %v:",
							sampleErr)
					} else {
						log.Printf("hibernate: hibernation complete for indexes:")
					}
					for _, index := range indexDefs.IndexDefs {
						log.Printf("%s\n", index.Name)
					}
					return sampleErr
				}
			}

			if sampleErr != nil {
				return sampleErr
			}
		}
	}
	return nil
}

// --------------------------------------------------------

// This function returns a map which maps the pindex name to the node UUID
// of the node with the active partition(a pindex with 1 or more replicas can
// have replica partitions on multiple nodes).
func (hm *Manager) getPIndexActiveNodeMap() (map[string]string, error) {
	pindexActiveNodeMap := make(map[string]string)

	_, indexPlanPIndexMap, err := hm.options.Manager.GetPlanPIndexes(true)

	if err != nil {
		return nil, err
	}

	for indexName, _ := range hm.indexDefsToHibernate.IndexDefs {
		for _, plan := range indexPlanPIndexMap[indexName] {
			if plan.IndexName == indexName {
				for nodeUUID, node := range plan.Nodes {
					// Indicates active partition.
					if node.Priority <= 0 {
						pindexActiveNodeMap[plan.Name] = nodeUUID
					}
				}
			}
		}
	}

	return pindexActiveNodeMap, nil
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// runMonitor handles any error from the nodes monitoring subsystem by
// stopping the hibernation.
func (hm *Manager) runMonitor() {
	defer close(hm.monitorDoneCh)

	errMap := make(map[string]uint8, len(hm.nodesAll))

	errThreshold := rebalance.StatsSampleErrorThreshold
	if hm.options.StatsSampleErrorThreshold != nil {
		errThreshold = uint8(*hm.options.StatsSampleErrorThreshold)
	}

	pindexActiveNodeMap, err := hm.getPIndexActiveNodeMap()
	if err != nil {
		log.Errorf("hibernate: error getting pindex-node map: %v", err)
		return
	}

	for {
		select {
		case <-hm.stopCh:
			return

		case s, ok := <-hm.monitorSampleCh:
			if !ok {
				return
			}

			if s.Error != nil {
				errMap[s.UUID]++

				hm.Logf("hibernate: runMonitor, s.Error: %#v", s.Error)

				hm.progressCh <- HibernationProgress{Error: s.Error}
				hm.Stop() // Stop the hibernate.
				continue
			}

			if s.Kind == "/api/stats?partitions=true&seqno=false" {
				if s.Data == nil {
					errMap[s.UUID]++
					if errMap[s.UUID] < errThreshold {
						hm.Logf("hibernate: runMonitor, skipping the empty response"+
							"for node: %s, for %d time", s.UUID, errMap[s.UUID])
						continue
					}
				}

				// reset the error resiliency count to zero upon a successful response.
				errMap[s.UUID] = 0

				m := struct {
					Status map[string]struct {
						CopyStats struct {
							TransferProgress         float64 `json:"TransferProgress"`
							TotalCopyPartitionErrors int32   `json:"TotCopyPartitionErrors"`
						} `json:"copyPartitionStats"`
					} `json:"pindexes"`
				}{}

				err := json.Unmarshal(s.Data, &m)
				if err != nil {
					hm.Logf("hibernate: runMonitor json, s.Data: %s, err: %#v",
						s.Data, err)

					hm.progressCh <- HibernationProgress{Error: err}
					hm.Stop() // Stop the hibernate.
					continue
				}

				for pindex, stats := range m.Status {
					// Refresh the map in case the pindex doesn't exist due to
					// pindex name change.
					if _, exists := pindexActiveNodeMap[pindex]; !exists {
						pindexActiveNodeMap, _ = hm.getPIndexActiveNodeMap()
					}
					// Doesn't exist despite refreshing the map.
					if _, exists := pindexActiveNodeMap[pindex]; !exists {
						continue
					}
					// Only nodes with active partitions are monitored for hibernation/pause
					// tasks since only they perform uploads.
					if hm.operationType == OperationType(cbgt.HIBERNATE_TASK) &&
						s.UUID != pindexActiveNodeMap[pindex] {
						continue
					}

					indexName, err := hm.options.Manager.GetIndexNameForPIndex(pindex)
					if err != nil {
						log.Errorf("hibernate: error getting idx: %v", err)
						continue
					}
					// If the pindex from the /stats endpoint is not linked to the hibernation indexes
					// do not count it as part of the progress.
					if _, exists := hm.indexDefsToHibernate.IndexDefs[indexName]; !exists {
						continue
					}

					if stats.CopyStats.TotalCopyPartitionErrors > 0 {
						errMap[s.UUID] += uint8(stats.CopyStats.TotalCopyPartitionErrors)

						hm.Logf("hibernate: runMonitor, partition errors on node %s.", s.UUID)

						hm.progressCh <- HibernationProgress{Error: fmt.Errorf("hibernate: runMonitor, partition errors.")}
						hm.Stop() // Stop the hibernate.
						continue
					}

					hm.m.Lock()
					hm.transferProgress[s.UUID+":"+pindex] = float64(stats.CopyStats.TransferProgress)
					hm.m.Unlock()
				}
			}

			notifyWanters := true

			for notifyWanters {
				select {
				case sampleWantCh := <-hm.monitorSampleWantCh:
					sampleWantCh <- s
					close(sampleWantCh)

				default:
					notifyWanters = false
				}
			}
		}
	}
}

// Stop asynchronously requests a stop to the hibernation operation.
// Callers can look for the closing of the ProgressCh() to see when
// the hibernation operation has actually stopped.
func (hm *Manager) Stop() {
	hm.m.Lock()
	if !isClosed(hm.stopCh) {
		close(hm.stopCh)
	}

	hm.m.Unlock()
}
