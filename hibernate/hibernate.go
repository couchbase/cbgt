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

	// This hook is used to obtain the bucket and path in remote object
	// storage, used when checking if a remote path exists.
	GetRemoteBucketAndPathHook = func(remotePath string) (string, string, error) {
		return "", "", fmt.Errorf("not-implemented")
	}

	BucketStateTrackerHook = func(*cbgt.Manager, string, string) (int, error) {
		return 0, fmt.Errorf("not-implemented")
	}

	// This hook is used to download the index metadata(definitions) from the
	// remote object store, to be used when unhibernating.
	DownloadIndexMetadataHook = func(client objcli.Client, remotePath string) (*cbgt.IndexDefs, error) {
		return nil, fmt.Errorf("not-implemented")
	}

	// This hook is used to upload the index metadata(definitions) to the
	// remote object store, to be used when hibernating.
	UploadIndexMetadataHook = func(client objcli.Client, ctx context.Context, data []byte, remotePath string) error {
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
	rollbackRequired bool // status flag to check if the hibernation rollback
	// is required to avoid triggering multiple/unnecessary rollbacks.
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

// Common start for both pause and resume.
func Start(version string, cfg cbgt.Cfg, server string,
	options HibernationOptions, hibernationType OperationType) (
	*Manager, error) {

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

		client := hm.options.Manager.GetObjStoreClient()
		if client == nil {
			return nil, fmt.Errorf("hibernate: failed to get object store client")
		}

		indexDefsToHibernate, err = DownloadIndexMetadataHook(client, hm.options.ArchiveLocation)
		if err != nil {
			return nil, err
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
		DiagSampleDisable: true,
		HttpGet:           options.HttpGet,
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
	hm.rollbackRequired = true

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

func (hm *Manager) uploadIndexMetadata() error {
	client := hm.options.Manager.GetObjStoreClient()
	if client == nil {
		return fmt.Errorf("hibernate: unable to get object store client")
	}

	ctx, _ := hm.options.Manager.GetHibernationContext()

	indexDefs := cbgt.NewIndexDefs(hm.version)

	for _, index := range hm.indexDefsToHibernate.IndexDefs {
		indexDefs.IndexDefs[index.Name] = index
	}

	data, err := json.Marshal(indexDefs)
	if err != nil {
		return fmt.Errorf("hibernate: error marshalling index defs: %e", err)
	}

	return UploadIndexMetadataHook(client, ctx, data, hm.options.ArchiveLocation)
}

func (hm *Manager) startPauseBucketStateTracker() {
	status, err := BucketStateTrackerHook(hm.options.Manager, cbgt.HIBERNATE_TASK,
		hm.options.BucketName)
	if err != nil {
		log.Errorf("hibernate: pause: error tracking bucket: %v", err)
		return
	}

	if status == 1 {
		log.Printf("hibernate: hibernation succeeded, deleting indexes for "+
			"bucket %s", hm.options.BucketName)

		// TODO Better way of getting source type?
		var sourceType string
		for _, index := range hm.indexDefsToHibernate.IndexDefs {
			if index.SourceName == hm.options.BucketName {
				sourceType = index.SourceType
				break
			}
		}
		hm.options.Manager.DeleteAllIndexFromSource(sourceType, hm.options.BucketName, "")
	}
}

func (hm *Manager) runHibernateIndexes() {
	var err error
	defer func() {
		// Completion of hibernation operation, whether naturally or due
		// to error/Stop(), needs this cleanup.  Wait for runMonitor()
		// to finish as it may have more sends to progressCh.
		hm.monitor.Stop()
		close(hm.monitorSampleCh)
		<-hm.monitorDoneCh

		hm.Stop()

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

	if hm.operationType == OperationType(cbgt.HIBERNATE_TASK) {
		go hm.startPauseBucketStateTracker()
	} else if hm.operationType == OperationType(cbgt.UNHIBERNATE_TASK) {
		// This distinction between dry and non-dry run for Resume is made here
		// since in a dry run, no index is 'hibernated' per se, it's a checking of remote
		// paths.
		if !hm.options.DryRun {
			hm.options.ArchiveLocation = ""
			err = hm.removeHibernationPath(hm.indexDefsToHibernate)
			if err != nil {
				hm.Logf("hibernate: error removing path: %e", err)
				return
			}
		}
	}

	// At this point, it indicates that the hibernation has been completed
	// for all the indexes.
	hm.hibernationComplete = true
}

// --------------------------------------------------------

// TODO Think of an alternative name for pauseIndexes and resumeIndexes.
func (hm *Manager) pauseIndexes(indexDefsToPause *cbgt.IndexDefs) error {
	for indexName, index := range indexDefsToPause.IndexDefs {
		// Skip indexDef's with no instantiatable pindexImplType, such
		// as index aliases.
		pindexImplType, exists := cbgt.PIndexImplTypes[index.Type]
		if !exists || pindexImplType == nil ||
			pindexImplType.New == nil || pindexImplType.Open == nil {
			delete(indexDefsToPause.IndexDefs, indexName)
		}
	}

	planPIndexes, cas, err := cbgt.PlannerGetPlanPIndexes(hm.cfg, hm.version)
	if err != nil {
		return err
	}

	indexNameDef := make(map[string]*cbgt.IndexDef)
	for _, index := range indexDefsToPause.IndexDefs {
		indexNameDef[index.Name] = index
	}

	planUUID := cbgt.NewUUID()
	for _, planPIndex := range planPIndexes.PlanPIndexes {
		if _, exists := indexNameDef[planPIndex.IndexName]; exists {
			planPIndex.HibernationPath = hm.options.ArchiveLocation
			planPIndex.UUID = planUUID
			planPIndex.IndexParams = hm.AppendHibernatePath(planPIndex.IndexParams)
		}
	}
	planPIndexes.UUID = planUUID

	select {
	case <-hm.stopCh:
		// No rollback required here since no Cfg changes have taken place yet.
		hm.rollbackRequired = false
		return fmt.Errorf("hibernate: pausing of indexes stopped")

	default:
		//NO-OP
	}

	_, err = cbgt.CfgSetPlanPIndexes(hm.cfg, planPIndexes, cas)
	return err
}

func (hm *Manager) UpdateIndexParams(indexDef *cbgt.IndexDef, uuid string) {
	indexDef.UUID = uuid
	log.Printf("hibernate: changing metadata path to %s", hm.options.ArchiveLocation)
	// Update index params with remote path
	indexDef.HibernationPath = hm.options.ArchiveLocation
	indexDef.Params = hm.AppendHibernatePath(indexDef.Params)
}

func (hm *Manager) AppendHibernatePath(params string) string {
	return params[0:len(params)-1] + ",\"hibernate\":\"" +
		hm.options.ArchiveLocation + "\"}"
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

	indexDefs, _, err := hm.options.Manager.GetIndexDefs(false)
	if err != nil {
		return err
	}

	if indexDefs == nil {
		indexDefs = cbgt.NewIndexDefs(hm.version)
	}

	for _, indexDef := range indexesToResume.IndexDefs {
		if _, exists := indexDefs.IndexDefs[indexDef.Name]; !exists {
			hm.UpdateIndexParams(indexDef, indexDef.UUID)
			indexDefs.IndexDefs[indexDef.Name] = indexDef
		}
	}

	indexDefs.UUID = cbgt.NewUUID()
	indexDefs.ImplVersion = indexesToResume.ImplVersion

	select {
	case <-hm.stopCh:
		// No rollback required here since no Cfg changes have taken place yet.
		hm.rollbackRequired = false
		return fmt.Errorf("hibernate: resuming of indexes stopped")

	default:
		//NO-OP
	}

	// TODO: Avoid force_cas key.
	_, err = cbgt.CfgSetIndexDefs(hm.options.Manager.Cfg(), indexDefs, cbgt.CFG_CAS_FORCE)
	return err
}

func (hm *Manager) hibernateIndexes(indexDefs *cbgt.IndexDefs) error {
	if hm.operationType == OperationType(cbgt.HIBERNATE_TASK) {
		err := hm.uploadIndexMetadata()
		if err != nil {
			return err
		}

		err = hm.pauseIndexes(indexDefs)
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
	indexDefs, _, err := hm.options.Manager.GetIndexDefs(false)
	if err != nil {
		return err
	}

	indexUUID := cbgt.NewUUID()

	for _, index := range indexesToChange.IndexDefs {
		var indexDef *cbgt.IndexDef

		if _, exists := indexDefs.IndexDefs[index.Name]; !exists {
			return fmt.Errorf("hibernate: index def does not exist")
		} else {
			indexDef = indexDefs.IndexDefs[index.Name]
		}

		hm.UpdateIndexParams(indexDef, indexUUID)
		indexDefs.IndexDefs[indexDef.Name] = indexDef
	}

	indexDefs.UUID = indexUUID
	indexDefs.ImplVersion = cbgt.CfgGetVersion(hm.options.Manager.Cfg())

	// TODO: Avoid force_cas key.
	_, err = cbgt.CfgSetIndexDefs(hm.options.Manager.Cfg(), indexDefs, cbgt.CFG_CAS_FORCE)
	return err
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

				if sample.Kind == "/api/stats?partitions=true" {
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

// runMonitor handles any error from the nodes monitoring subsystem by
// stopping the hibernation.
func (hm *Manager) runMonitor() {
	defer close(hm.monitorDoneCh)

	errMap := make(map[string]uint8, len(hm.nodesAll))

	errThreshold := rebalance.StatsSampleErrorThreshold
	if hm.options.StatsSampleErrorThreshold != nil {
		errThreshold = uint8(*hm.options.StatsSampleErrorThreshold)
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

			if s.Kind == "/api/stats?partitions=true" {
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
					indexName, err := hm.options.Manager.GetIndexNameForPIndex(pindex)
					if err != nil {
						log.Errorf("hibernate: error getting idx: %e", err)
						continue
					}
					// If the pindex from the /stats endpoint is not linked to the hibernation indexes
					// do not count it as part of the progress.
					if _, exists := hm.indexDefsToHibernate.IndexDefs[indexName]; !exists {
						continue
					}

					if stats.CopyStats.TotalCopyPartitionErrors > 0 {
						errMap[s.UUID] += uint8(stats.CopyStats.TotalCopyPartitionErrors)

						hm.Logf("hibernate: runMonitor, partition errors.")

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
	if hm.stopCh != nil {
		close(hm.stopCh)
		hm.stopCh = nil
	}

	hm.m.Unlock()

	if !hm.hibernationComplete && hm.rollbackRequired {
		hm.rollbackHibernation()
	}
}

// This function rolls back the effects of hibernation if
// cancelled midway/aborted.
func (hm *Manager) rollbackHibernation() {

	if hm.operationType == OperationType(cbgt.UNHIBERNATE_TASK) && !hm.options.DryRun {
		indexDefs, _, err := hm.options.Manager.GetIndexDefs(false)
		if err != nil {
			return
		}

		for _, index := range hm.indexDefsToHibernate.IndexDefs {
			if _, exists := indexDefs.IndexDefs[index.Name]; exists {
				// Deleting the indexes that are resumed/in the process of resuming.
				delete(indexDefs.IndexDefs, index.Name)
			}
		}

		indexDefs.UUID = cbgt.NewUUID()
		indexDefs.ImplVersion = cbgt.CfgGetVersion(hm.options.Manager.Cfg())

		// TODO: Avoid force_cas key.
		_, err = cbgt.CfgSetIndexDefs(hm.options.Manager.Cfg(), indexDefs, cbgt.CFG_CAS_FORCE)
		if err != nil {
			return
		}
	}

	hm.m.Lock()
	hm.rollbackRequired = false
	hm.m.Unlock()
}
