//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
)

// FeedAllotmentOption is the manager option key used the specify how
// feeds should be alloted or assigned.
const FeedAllotmentOption = "feedAllotment"

// FeedAllotmentOnePerPIndex specifies that there should be only a
// single feed per pindex.
const FeedAllotmentOnePerPIndex = "oneFeedPerPIndex"

const JANITOR_CLOSE_PINDEX = "janitor_close_pindex"
const JANITOR_REMOVE_PINDEX = "janitor_remove_pindex"
const JANITOR_ROLLBACK_PINDEX = "janitor_rollback_pindex"

// JanitorNOOP sends a synchronous NOOP to the manager's janitor, if any.
func (mgr *Manager) JanitorNOOP(msg string) {
	atomic.AddUint64(&mgr.stats.TotJanitorNOOP, 1)

	if mgr.tagsMap == nil || (mgr.tagsMap["pindex"] && mgr.tagsMap["janitor"]) {
		syncWorkReq(mgr.janitorCh, WORK_NOOP, msg, nil)
	}
}

// JanitorKick synchronously kicks the manager's janitor, if any.
func (mgr *Manager) JanitorKick(msg string) {
	atomic.AddUint64(&mgr.stats.TotJanitorKick, 1)

	if mgr.tagsMap == nil || (mgr.tagsMap["pindex"] && mgr.tagsMap["janitor"]) {
		syncWorkReq(mgr.janitorCh, WORK_KICK, msg, nil)
	}
}

// JanitorKick synchronously kicks the manager's janitor, if any, to initiate a
// rollback.
func (mgr *Manager) JanitorRollbackKick(msg string, pindex *PIndex) {
	atomic.AddUint64(&mgr.stats.TotJanitorKick, 1)

	if mgr.tagsMap == nil || (mgr.tagsMap["pindex"] && mgr.tagsMap["janitor"]) {
		syncWorkReq(mgr.janitorCh, JANITOR_ROLLBACK_PINDEX, msg, pindex)
	}
}

// A way for applications to hook into the janitor's rollback phases.
// **Must be set at init time, before the manager is started
// **Works with/alongside PIndexImplType's Rollback
var RollbackHook func(pindex *PIndex, phase RollbackPhase) (wasPartial bool, err error)

type RollbackPhase int

const (
	RollbackInit RollbackPhase = iota
	RollbackCompleted
)

func (mgr *Manager) rollbackPIndex(pindex *PIndex) error {
	var err error
	defer func() {
		if RollbackHook != nil {
			_, err = RollbackHook(pindex, RollbackCompleted)
			if err != nil {
				log.Warnf("janitor: rollbackPIndex for pindex %s, "+
					"RollbackHook, err: %v", pindex.Name, err)
			}
		}
	}()

	if pindex == nil {
		return nil
	}

	var wasPartial bool
	if RollbackHook != nil {
		wasPartial, err = RollbackHook(pindex, RollbackInit)
		if err != nil {
			return fmt.Errorf("janitor: rollbackPIndex for pindex %s, "+
				"RollbackHook, err: %v", pindex.Name, err)
		}
	}

	if !wasPartial {// Full rollback is also the default (in case RollbackHook is not registered)
		// Stopping the pindex.
		// Making sure files are removed, if necessary since cbft might create
		// files in the pindex directory.
		err := mgr.stopPIndex(pindex, true)
		if err != nil {
			return fmt.Errorf("janitor: fully rollback pindex for pindex %s, stopPIndex, "+
			"err: %v", pindex.Name, err)
		}

		return mgr.fullRollbackPIndex(pindex)
	} else {
		// Stopping the pindex.
		// Will not build the pindex from scratch, hence not removing the files.
		err := mgr.stopPIndex(pindex, false)
		if err != nil {
			return fmt.Errorf("janitor: partially rollback pindex for pindex %s, stopPIndex, "+
			"err: %v", pindex.Name, err)
		}

		// Partial rollback if the files are present.
		err = mgr.partiallyRollbackPIndex(pindex)
		if err != nil {
			log.Warnf("janitor: partiallyRollbackPIndex for pindex %s, cleaning "+
				"and trying full rollback, err: %v", pindex.Name, err)

			pindex.closed = false
			mgr.stopPIndex(pindex, true)

			// Full rollback if the partial rollback failed.
			return mgr.fullRollbackPIndex(pindex)
		}
	}

	return nil
}

func (mgr *Manager) fullRollbackPIndex(pindex *PIndex) error {
	log.Printf("janitor: fully rolling back pindex %s", pindex.Name)
	pindexName := pindex.Name
	var err error
	pindex, err = createNewPIndex(mgr, pindex.Name, pindex.UUID,
		pindex.IndexType, pindex.IndexName, pindex.IndexUUID, pindex.IndexParams,
		pindex.SourceType, pindex.SourceName, pindex.SourceUUID, pindex.SourceParams,
		pindex.SourcePartitions, pindex.Path, RollbackPIndexImpl)
	if err != nil {
		return fmt.Errorf("janitor: error rolling back pindex: %s, err: %v", pindexName, err)
	}

	err = mgr.registerPIndex(pindex)
	if err != nil {
		return fmt.Errorf("janitor: error registering pindex: %s, err: %v", pindex.Name, err)
	}

	if RollbackHook != nil {
		// Janitor notification is to be handled by calling application
		// (Only by cbft asynchronously at the moment, after pindex is copied)
		return nil
	}

	// Required if no hook was registered to setup feeds
	return mgr.JanitorOnce("Adding feeds after full rollback of pindex: " + pindex.Name)
}

func (mgr *Manager) partiallyRollbackPIndex(pindex *PIndex) error {
	// if there is an error opening pindex, the returned pindex is nil
	// hence, storing the path before opening pindex
	pindexPath := pindex.Path
	log.Printf("janitor: partial rollback, path: %s", pindexPath)

	pindex, err := OpenPIndex(mgr, pindexPath)
	if err != nil {
		return err
	}

	err = mgr.registerPIndex(pindex)
	if err != nil {
		return fmt.Errorf("janitor: error registering pindex %s: %v", pindex.Name, err)
	}

	// Required to add feeds for partial rollback.
	return mgr.JanitorOnce("Adding feeds after partial rollback of pindex: " + pindex.Name)
}

// JanitorLoop is the main loop for the janitor.
func (mgr *Manager) JanitorLoop() {
	mgr.cfgObserver(componentJanitor, func(cfgEvent *CfgEvent) {
		atomic.AddUint64(&mgr.stats.TotJanitorSubscriptionEvent, 1)
		mgr.JanitorKick("cfg changed, key: " + cfgEvent.Key)
	})

	for {
		select {
		case <-mgr.stopCh:
			atomic.AddUint64(&mgr.stats.TotJanitorStop, 1)
			return

		case m := <-mgr.janitorCh:
			atomic.AddUint64(&mgr.stats.TotJanitorOpStart, 1)

			log.Printf("janitor: awakes, op: %v, msg: %s", m.op, m.msg)

			var err error

			if m.op == WORK_KICK {
				atomic.AddUint64(&mgr.stats.TotJanitorKickStart, 1)
				err = mgr.JanitorOnce(m.msg)
				if err != nil {
					// Keep looping as perhaps it's a transient issue.
					// TODO: Perhaps need a rescheduled janitor kick.
					log.Warnf("janitor: JanitorOnce, err: %v", err)
					atomic.AddUint64(&mgr.stats.TotJanitorKickErr, 1)
				} else {
					atomic.AddUint64(&mgr.stats.TotJanitorKickOk, 1)
				}
			} else if m.op == WORK_NOOP {
				atomic.AddUint64(&mgr.stats.TotJanitorNOOPOk, 1)
			} else if m.op == JANITOR_CLOSE_PINDEX {
				err = mgr.stopPIndex(m.obj.(*PIndex), false)
			} else if m.op == JANITOR_REMOVE_PINDEX {
				err = mgr.stopPIndex(m.obj.(*PIndex), true)
			} else if m.op == JANITOR_ROLLBACK_PINDEX {
				err = mgr.rollbackPIndex(m.obj.(*PIndex))
				if err != nil {
					log.Warnf("janitor: rollbackPIndex for pindex %s, err: %v", m.obj.(*PIndex).Name, err)
				}
			} else {
				err = fmt.Errorf("janitor: unknown op: %s, m: %#v", m.op, m)
				atomic.AddUint64(&mgr.stats.TotJanitorUnknownErr, 1)
			}

			atomic.AddUint64(&mgr.stats.TotJanitorOpRes, 1)

			if m.resCh != nil {
				if err != nil {
					atomic.AddUint64(&mgr.stats.TotJanitorOpErr, 1)
					m.resCh <- err
				}
				close(m.resCh)
			}

			atomic.AddUint64(&mgr.stats.TotJanitorOpDone, 1)
		}
	}
}

func (mgr *Manager) pindexesStop(removePIndexes []*PIndex) []error {
	var wg sync.WaitGroup
	size := len(removePIndexes)
	requestCh := make(chan *PIndex, size)
	responseCh := make(chan error, size)
	nWorkers := getWorkerCount(size)
	// spawn the stop PIndex workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for pi := range requestCh {
				// check if the loadDataDir is still loading this pindex, if so
				// leave that to heal in subsequent Janitor loop?
				if mgr.bootingPIndex(pi.Name) {
					log.Printf("janitor: pindexesStop skipping stopPIndex,"+
						" pindex: %s", pi.Name)
					continue
				}
				err := mgr.stopPIndex(pi, true)
				if err != nil {
					responseCh <- fmt.Errorf("janitor: removing pindex: %s, err: %v",
						pi.Name, err)
				}
			}
			wg.Done()
		}()
	}
	// feed the workers with PIndex to remove
	for _, removePIndex := range removePIndexes {
		requestCh <- removePIndex
	}
	close(requestCh)
	wg.Wait()
	close(responseCh)
	var errs []error
	for err := range responseCh {
		errs = append(errs, err)
	}
	return errs
}

func (mgr *Manager) pindexesStart(addPlanPIndexes []*PlanPIndex) []error {
	var wg sync.WaitGroup
	size := len(addPlanPIndexes)
	requestCh := make(chan *PlanPIndex, size)
	responseCh := make(chan error, size)
	nWorkers := getWorkerCount(size)
	// spawn the start PIndex workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for pi := range requestCh {
				// check if this pindex is already in booting
				// by loadDataDir. If so just skip the processing here.
				// else update the booting status so that the manager's
				// loadDataDir won't reattempt the same pindex.
				if !mgr.updateBootingStatus(pi.Name, true) {
					// 'pi' already loaded
					continue
				}

				err := mgr.startPIndex(pi)
				if err != nil {
					responseCh <- fmt.Errorf("janitor: adding pindex: %s, err: %v",
						pi.Name, err)
				}

				// mark the pindex booting complete status
				mgr.updateBootingStatus(pi.Name, false)
			}
			wg.Done()
		}()
	}
	// feed the workers with planPIndexes
	for _, addPlanPIndex := range addPlanPIndexes {
		requestCh <- addPlanPIndex
	}
	close(requestCh)
	wg.Wait()
	close(responseCh)
	var errs []error
	for err := range responseCh {
		errs = append(errs, err)
	}
	return errs
}

func cleanDir(path string) {
	if path != "" {
		_ = os.RemoveAll(path)
	}
}

func (mgr *Manager) restartPIndex(req *pindexRestartReq) error {
	if req == nil {
		return nil
	}
	// check if the loadDataDir is still loading this pindex, if so
	// leave that to heal in subsequent Janitor loops.
	if mgr.bootingPIndex(req.pindex.Name) {
		log.Printf("janitor: restartPIndex skipping restart for "+
			" pindex: %s", req.pindex.Name)
		return nil
	}
	// stop the pindex first
	err := mgr.stopPIndex(req.pindex, false)
	if err != nil {
		cleanDir(req.pindex.Path)
		return fmt.Errorf("janitor: restartPIndex stopping "+
			" pindex: %s, err: %v", req.pindex.Name, err)
	}
	// rename the pindex folder and name as per the new plan
	newPath := mgr.PIndexPath(req.planPIndexName)
	if newPath != req.pindex.Path {
		err = os.Rename(req.pindex.Path, newPath)
		if err != nil {
			cleanDir(req.pindex.Path)
			cleanDir(newPath)
			return fmt.Errorf("janitor: restartPIndex"+
				" updating pindex: %s path: %s failed, err: %v",
				req.pindex.Name, newPath, err)
		}
	}

	pi := req.pindex.Clone()
	pi.Name = req.planPIndexName
	pi.Path = newPath

	// persist PINDEX_META only if manager's dataDir is set
	if len(mgr.dataDir) > 0 {
		// update the new indexdef param changes
		buf, err := MarshalJSON(pi)
		if err != nil {
			cleanDir(newPath)
			return fmt.Errorf("janitor: restartPIndex"+
				" Marshal pindex: %s, err: %v", pi.Name, err)

		}
		err = os.WriteFile(pi.Path+string(os.PathSeparator)+
			PINDEX_META_FILENAME, buf, 0600)
		if err != nil {
			cleanDir(pi.Path)
			return fmt.Errorf("janitor: restartPIndex could not save "+
				"PINDEX_META_FILENAME,"+" path: %s, err: %v", pi.Path, err)
		}
	}

	// open the pindex and register
	pindex, err := OpenPIndex(mgr, pi.Path)
	if err != nil {
		cleanDir(req.pindex.Path)
		return fmt.Errorf("janitor: restartPIndex could not open "+
			" pindex path: %s, err: %v", pi.Path, err)
	}
	err = mgr.registerPIndex(pindex)
	if err != nil {
		cleanDir(pindex.Path)
		return fmt.Errorf("janitor: restartPIndex failed to "+
			"register pindex: %s, err: %v", pindex.Name, err)
	}
	atomic.AddUint64(&mgr.stats.TotJanitorRestartPIndex, 1)
	return nil
}

type pindexHibernateReq struct {
	pindex     *PIndex
	planPIndex *PlanPIndex
}

type pindexRestartReq struct {
	pindex         *PIndex
	planPIndexName string
}

type pindexRestartErr struct {
	err    error
	pindex *PIndex
}

func (re *pindexRestartErr) Error() string {
	return re.err.Error()
}

func (mgr *Manager) pindexesRestart(
	restartRequests []*pindexRestartReq) []pindexRestartErr {
	var wg sync.WaitGroup
	size := len(restartRequests)
	requestCh := make(chan *pindexRestartReq, size)
	responseCh := make(chan *pindexRestartErr, size)
	nWorkers := getWorkerCount(size)
	// spawn the restart PIndex workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for req := range requestCh {
				err := mgr.restartPIndex(req)
				if err != nil {
					responseCh <- &pindexRestartErr{err: err,
						pindex: req.pindex}
				}
			}
			wg.Done()
		}()
	}
	// feed the workers with restartRequests
	for _, restartReq := range restartRequests {
		requestCh <- restartReq
	}
	close(requestCh)
	wg.Wait()
	close(responseCh)
	var errs []pindexRestartErr
	for resp := range responseCh {
		log.Warnf("janitor: restartPIndex err: %v", resp.err)
		errs = append(errs, *resp)
	}
	return errs
}

var HibernatePartitionsHook func(mgr *Manager, activePIndexes,
	replicaPIndexes []*PIndex) []error

var HibernationBucketStateTrackerHook func(*Manager, string, string)

var UnhibernationBucketStateTrackerHook func(*Manager, string, string)

// Specific restart and registration used in hibernation - followed by specific
// hibernation/unhibernation functions.
func (mgr *Manager) hibernateRestart(r *pindexRestartReq) (*PIndex, error) {
	// stop the pindex first
	err := mgr.stopPIndex(r.pindex, false)
	if err != nil {
		return nil, fmt.Errorf("janitor: hibernateRestart stopping "+
			" pindex: %s, err: %v", r.pindex.Name, err)
	}
	// rename the pindex folder and name as per the new plan
	newPath := mgr.PIndexPath(r.planPIndexName)
	if newPath != r.pindex.Path {
		err = os.Rename(r.pindex.Path, newPath)
		if err != nil {
			return nil, fmt.Errorf("janitor: hibernateRestart"+
				" updating pindex: %s path: %s failed, err: %v",
				r.pindex.Name, newPath, err)
		}
	}

	pi := r.pindex.Clone()
	pi.Name = r.planPIndexName
	pi.Path = newPath
	pi.HibernationPath = r.pindex.HibernationPath

	// persist PINDEX_META only if manager's dataDir is set
	if len(mgr.dataDir) > 0 {
		buf, err := MarshalJSON(pi)
		if err != nil {
			return nil, fmt.Errorf("janitor: hibernateRestart"+
				" Marshal pindex: %s, err: %v", pi.Name, err)

		}
		err = os.WriteFile(pi.Path+string(os.PathSeparator)+
			PINDEX_META_FILENAME, buf, 0600)
		if err != nil {
			return nil, fmt.Errorf("janitor: hibernateRestart could not save "+
				"PINDEX_META_FILENAME,"+" path: %s, err: %v", pi.Path, err)
		}
	}

	err = mgr.registerPIndex(pi)
	if err != nil {
		return nil, fmt.Errorf("janitor: hibernateRestart failed to "+
			"register pindex: %s, err: %v", pi.Name, err)
	}

	return pi, nil
}

func (mgr *Manager) hibernatePIndex(req []*pindexHibernateReq) []error {
	// map of source name -> list of pindexes with the same source
	pindexesToHibernate := make(map[string][][]*PIndex)
	var errs []error

	for _, r := range req {
		tempReq := &pindexRestartReq{pindex: r.pindex,
			planPIndexName: r.planPIndex.Name}
		pi, err := mgr.hibernateRestart(tempReq)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Change 'closed' to false so that its files gets deleted on a
		// successful hibernation.
		pi.closed = false

		if _, exists := pindexesToHibernate[pi.SourceName]; !exists {
			pindexesToHibernate[pi.SourceName] = make([][]*PIndex, 2)
		}

		// Only hibernating if it's an active partition node.
		if r.planPIndex.Nodes[mgr.uuid].Priority <= 0 {
			pindexesToHibernate[pi.SourceName][0] = append(pindexesToHibernate[pi.SourceName][0],
				pi)
		} else {
			pindexesToHibernate[pi.SourceName][1] = append(pindexesToHibernate[pi.SourceName][1],
				pi)
		}
	}

	if HibernatePartitionsHook != nil {
		for source, pindexes := range pindexesToHibernate {
			// Assuming bucket state is already being tracked for pause
			mgr.RegisterHibernationBucketTracker(source)

			hibErrs := HibernatePartitionsHook(mgr, pindexes[0], pindexes[1])
			if len(hibErrs) > 0 {
				errs = append(errs, hibErrs...)
			}
		}
	}

	return errs
}

// JanitorOnce is the main body of a JanitorLoop.
func (mgr *Manager) JanitorOnce(reason string) error {
	if mgr.cfg == nil { // Can occur during testing.
		return fmt.Errorf("janitor: skipped due to nil cfg")
	}

	feedAllotment := mgr.GetOption(FeedAllotmentOption)

	// NOTE: The janitor doesn't reconfirm that we're a wanted node
	// because instead some planner will see that & update the plan;
	// then relevant janitors will react by closing pindexes & feeds.

	planPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
	if err != nil {
		return fmt.Errorf("janitor: skipped on CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexes == nil {
		// Might happen if janitor wins an initialization race.
		return fmt.Errorf("janitor: skipped on nil planPIndexes")
	}

	_, currPIndexes := mgr.CurrentMaps()

	mapWantedPlanPIndex := mgr.reusablePIndexesPlanMap(currPIndexes, planPIndexes)
	addPlanPIndexes, removePIndexes :=
		CalcPIndexesDelta(mgr.uuid, currPIndexes, planPIndexes, mapWantedPlanPIndex)

	// check for any pindexes for restart and get classified lists of
	// pindexes to add, remove and restart
	planPIndexesToAdd, pindexesToRemove, pindexesToRestart, pindexesToHibernate :=
		classifyAddRemoveRestartPIndexes(mgr, addPlanPIndexes, removePIndexes)
	log.Printf("janitor: pindexes to remove: %d", len(pindexesToRemove))
	for _, pi := range pindexesToRemove {
		log.Printf("  pindex: %v; UUID: %v", pi.Name, pi.IndexUUID)
	}
	log.Printf("janitor: pindexes to add: %d", len(planPIndexesToAdd))
	for _, ppi := range planPIndexesToAdd {
		log.Printf("  pindex: %v; UUID: %v", ppi.Name, ppi.IndexUUID)
	}
	log.Printf("janitor: pindexes to restart: %d", len(pindexesToRestart))
	for _, pi := range pindexesToRestart {
		if pi.pindex != nil {
			log.Printf("  pindex: %v; UUID: %v", pi.pindex.Name, pi.pindex.IndexUUID)
		}
	}
	// restart any of the pindexes so that they can
	// adopt the updated indexDef parameters, ex: storeOptions
	restartErrs := mgr.pindexesRestart(pindexesToRestart)
	// upon any restart errors, bring back the addPlanPIndex for
	// starting the pindex afresh
	if len(restartErrs) > 0 {
		planPIndexesToAdd = append(planPIndexesToAdd, elicitAddPlanPIndexes(addPlanPIndexes, restartErrs)...)
	}
	var errs []error
	log.Printf("janitor: pindexes to hibernate: %d", len(pindexesToHibernate))
	for _, pi := range pindexesToHibernate {
		if pi != nil {
			log.Printf(" pindex %v; UUID: %v", pi.pindex.Name, pi.pindex.UUID)
		}
	}

	errs = append(errs, mgr.hibernatePIndex(pindexesToHibernate)...)

	// First, teardown pindexes that need to be removed.
	// batching the stop, aiming to expedite the
	// whole JanitorOnce call
	errs = append(errs, mgr.pindexesStop(pindexesToRemove)...)
	// Then, (re-)create pindexes that we're missing.
	// batching the start, aiming to expedite the
	// whole JanitorOnce call
	errs = append(errs, mgr.pindexesStart(planPIndexesToAdd)...)

	var currFeeds map[string]Feed
	currFeeds, currPIndexes = mgr.CurrentMaps()

	hibernationTask, hibernationBucket, hibernationSourceType :=
		mgr.findHibernationBucketsToMonitor()

	if hibernationTask == UNHIBERNATE_TASK {
		log.Printf("janitor: bucket to track for unhibernation: %s", hibernationBucket)
		mgr.trackResumeBucketState(hibernationBucket, hibernationSourceType)
	}

	if hibernationTask == HIBERNATE_TASK {
		log.Printf("janitor: bucket to track for hibernation: %s", hibernationBucket)
		mgr.trackPauseBucketState(hibernationBucket, hibernationSourceType)
	}

	addFeeds, removeFeeds :=
		CalcFeedsDelta(mgr.uuid, planPIndexes, currFeeds, currPIndexes,
			feedAllotment)

	// filter out non-ready feeds.
	addFeeds = filterFeedable(mgr, addFeeds)

	log.Printf("janitor: feeds to remove: %d", len(removeFeeds))
	for _, removeFeed := range removeFeeds {
		log.Printf("  %s", removeFeed.Name())
	}
	log.Printf("janitor: feeds to add: %d", len(addFeeds))
	for _, targetPIndexes := range addFeeds {
		if len(targetPIndexes) > 0 {
			log.Printf("  %s", FeedNameForPIndex(targetPIndexes[0], feedAllotment))
		}
	}

	// First, teardown feeds that need to be removed.
	for _, removeFeed := range removeFeeds {
		err = mgr.stopFeed(removeFeed)
		if err != nil {
			errs = append(errs,
				fmt.Errorf("janitor: stopping feed, name: %s, err: %v",
					removeFeed.Name(), err))
		}
	}
	// Then, (re-)create feeds that we're missing.
	for _, addFeedTargetPIndexes := range addFeeds {
		err = mgr.startFeed(addFeedTargetPIndexes)
		if err != nil {
			errs = append(errs,
				fmt.Errorf("janitor: adding feed, err: %v", err))
		}
	}

	if len(errs) > 0 {
		var s []string
		for i, err := range errs {
			s = append(s, fmt.Sprintf("#%d: %v", i, err))
		}
		return fmt.Errorf("janitor: JanitorOnce errors: %d, %#v",
			len(errs), s)
	}

	return nil
}

func filterFeedable(mgr *Manager, addFeeds [][]*PIndex) (af [][]*PIndex) {
	for _, pindexes := range addFeeds {
		addList := make([]*PIndex, 0, len(pindexes))
		for _, pindex := range pindexes {
			ready, err := pindex.IsFeedable()
			if ready && err == nil {
				// Need to filter pindexes which are being pause/
				// were being paused before start to avoid adding feeds to them.
				hibernationInProgress := mgr.IsBucketBeingHibernated(pindex.SourceName)
				// if the pindex source name has a bucket being tracked, don't add it
				if !hibernationInProgress {
					addList = append(addList, pindex)
				}
				continue
			}
			log.Printf("janitor: skip feed: %s, err: %v", pindex.Name, err)
		}

		if len(addList) > 0 {
			af = append(af, addList)
		}
	}
	return af
}

func classifyAddRemoveRestartPIndexes(mgr *Manager, addPlanPIndexes []*PlanPIndex,
	removePIndexes []*PIndex) (planPIndexesToAdd []*PlanPIndex,
	pindexesToRemove []*PIndex, pindexesToRestart []*pindexRestartReq,
	pindexesToHibernate []*pindexHibernateReq) {
	// if there are no pindexes to be removed as per planner,
	// then there won't be anything to restart as well.
	if len(removePIndexes) == 0 {
		return addPlanPIndexes, nil, nil, nil
	}
	pindexesToRestart = make([]*pindexRestartReq, 0)
	pindexesToRemove = make([]*PIndex, 0)
	pindexesToHibernate = make([]*pindexHibernateReq, 0)
	planPIndexesToAdd = make([]*PlanPIndex, 0)
	// grouping addPlanPIndexes and removePIndexes as per index for
	// checking restartable indexDef changes per index
	indexPlanPIndexMap := make(map[string][]*PlanPIndex)
	indexPIndexMap := make(map[string][]*PIndex)
	for _, rp := range removePIndexes {
		indexPIndexMap[rp.IndexName] = append(indexPIndexMap[rp.IndexName], rp)
	}
	for _, addPlan := range addPlanPIndexes {
		indexPlanPIndexMap[addPlan.IndexName] =
			append(indexPlanPIndexMap[addPlan.IndexName], addPlan)
	}

	// avoid pindex rebuild on replica updates on index defn
	// unless overridden
	if v := mgr.GetOption("rebuildOnReplicaUpdate"); v != "true" {
		return advPIndexClassifier(mgr, indexPIndexMap, indexPlanPIndexMap)
	}

	// take every pindex to remove and check the config change
	// and sort out the pindexes to add, remove or restart
	for indexName, pindexes := range indexPIndexMap {
		if len(pindexes) > 0 && pindexes[0] != nil {
			pindex := pindexes[0]
			if planPIndexes, ok := indexPlanPIndexMap[indexName]; ok {
				indexDefnCurr := getIndexDefFromPlanPIndexes(planPIndexes)
				configAnalyzeReq := &ConfigAnalyzeRequest{
					IndexDefnCur:  indexDefnCurr,
					IndexDefnPrev: getIndexDefFromPIndex(pindex),
					SourcePartitionsCur: getSourcePartitionsMapFromPlanPIndexes(
						planPIndexes),
					SourcePartitionsPrev: getSourcePartitionsMapFromPIndexes(
						pindexes)}

				pindexImplType, exists := PIndexImplTypes[pindex.IndexType]
				if !exists || pindexImplType == nil {
					pindexesToRemove = append(pindexesToRemove, pindexes...)
					planPIndexesToAdd = append(planPIndexesToAdd, planPIndexes...)
					continue
				}
				pathChange := metadataPathChange(configAnalyzeReq)
				if pathChange == HIBERNATE_TASK {
					pidxList := getPIndexesToHibernate(pindexes, planPIndexes)
					for _, pidx := range pidxList {
						pindexesToHibernate = append(pindexesToHibernate, pidx)
					}
					continue
				}
				if pindexImplType.AnalyzeIndexDefUpdates != nil &&
					pindexImplType.AnalyzeIndexDefUpdates(configAnalyzeReq) ==
						PINDEXES_RESTART {
					pindexesToRestart = append(pindexesToRestart,
						getPIndexesToRestart(pindexes, planPIndexes)...)
					continue
				} else {
					pindexesToRemove = append(pindexesToRemove, pindexes...)
					planPIndexesToAdd = append(planPIndexesToAdd, planPIndexes...)
				}
			} else {
				pindexesToRemove = append(pindexesToRemove, pindexes...)
			}
		}
	}
	return planPIndexesToAdd, pindexesToRemove, pindexesToRestart, pindexesToHibernate
}

const (
	HIBERNATE_TASK   = "pause"
	UNHIBERNATE_TASK = "resume"
)

func isHibernateChange(curr, prev string) bool {
	return strings.HasPrefix(curr, HIBERNATE_TASK) && curr != prev
	// Add condition to check if pindex files are present locally.
}

func metadataPathChange(configAnalyzeReq *ConfigAnalyzeRequest) string {
	if isHibernateChange(configAnalyzeReq.IndexDefnCur.HibernationPath,
		configAnalyzeReq.IndexDefnPrev.HibernationPath) {
		return HIBERNATE_TASK
	}

	// This condition occurs during resume, when the download of the pindex files has
	// complete, the hibernation path is changed to a blank path, followed by waiting for
	// the right bucket state to add feeds(which is part of the current janitor kick).
	if strings.HasPrefix(configAnalyzeReq.IndexDefnPrev.HibernationPath, UNHIBERNATE_TASK) &&
		configAnalyzeReq.IndexDefnCur.HibernationPath == "" {
		return UNHIBERNATE_TASK
	}

	return ""
}

func advPIndexClassifier(mgr *Manager, indexPIndexMap map[string][]*PIndex,
	indexPlanPIndexMap map[string][]*PlanPIndex) (planPIndexesToAdd []*PlanPIndex,
	pindexesToRemove []*PIndex, pindexesToRestart []*pindexRestartReq,
	pindexesToHibernate []*pindexHibernateReq) {
	pindexesToRestart = make([]*pindexRestartReq, 0)
	pindexesToRemove = make([]*PIndex, 0)
	planPIndexesToAdd = make([]*PlanPIndex, 0)
	pindexesToHibernate = make([]*pindexHibernateReq, 0)

	// take every pindex to remove and check the config change
	// and sort out the pindexes to add, remove or restart
	for indexName, pindexes := range indexPIndexMap {
		restartable := make(map[string]struct{}, len(indexPIndexMap))
		if len(pindexes) > 0 && pindexes[0] != nil {
			// look for new addPlans the index level
			if planPIndexes, ok := indexPlanPIndexMap[indexName]; ok {
				indexDefnCur := getIndexDefFromPlanPIndexes(planPIndexes)
				indexDefnPrev := getIndexDefFromPIndex(pindexes[0])

				for _, pindex := range pindexes {
					// get the unique part of the pindex name
					pName := pindex.Name[strings.LastIndex(pindex.Name, "_")+1:]
					// look for a new plan for the older pindex
					var targetPlan *PlanPIndex
					for _, ppi := range planPIndexes {
						if pName == ppi.Name[strings.LastIndex(ppi.Name, "_")+1:] {
							targetPlan = ppi
							break
						}
					}
					if targetPlan == nil {
						pindexesToRemove = append(pindexesToRemove, pindex)
						continue
					}
					configAnalyzeReq := &ConfigAnalyzeRequest{
						IndexDefnCur:  indexDefnCur,
						IndexDefnPrev: indexDefnPrev,
						SourcePartitionsCur: map[string]bool{
							targetPlan.SourcePartitions: true},
						SourcePartitionsPrev: getSourcePartitionsMapFromPIndexes(
							[]*PIndex{pindex})}
					pindexImplType, exists := PIndexImplTypes[pindex.IndexType]
					if !exists || pindexImplType == nil {
						pindexesToRemove = append(pindexesToRemove, pindex)
						continue
					}
					pathChange := metadataPathChange(configAnalyzeReq)
					if pathChange == HIBERNATE_TASK {
						pindexesToHibernate = append(pindexesToHibernate,
							newPIndexHibernateReq(targetPlan, pindex))
						restartable[targetPlan.Name] = struct{}{}
						continue
					}

					// restartable pindex found from plan
					if pindexImplType.AnalyzeIndexDefUpdates != nil &&
						pindexImplType.AnalyzeIndexDefUpdates(configAnalyzeReq) ==
							PINDEXES_RESTART {
						pindexesToRestart = append(pindexesToRestart,
							newPIndexRestartReq(targetPlan, pindex))
						restartable[targetPlan.Name] = struct{}{}
						continue
					}
					// upon no restartability, consider the pindex for removal
					pindexesToRemove = append(pindexesToRemove, pindex)
				}

				// consider the remaining addPlans
				for _, ppi := range planPIndexes {
					if _, done := restartable[ppi.Name]; !done {
						planPIndexesToAdd = append(planPIndexesToAdd, ppi)
					}
				}

				// cleanup as all addPlans already processed for the index
				delete(indexPlanPIndexMap, indexName)
			} else {
				// as there are no new addPlans for the index,
				// consider complete pindexes/index removal
				pindexesToRemove = append(pindexesToRemove, pindexes...)
			}
		}
	}

	// include the remaining addPlans for any of the newer indexes
	for _, addPlans := range indexPlanPIndexMap {
		planPIndexesToAdd = append(planPIndexesToAdd, addPlans...)
	}

	return planPIndexesToAdd, pindexesToRemove, pindexesToRestart, pindexesToHibernate
}

func newPIndexRestartReq(addPlanPI *PlanPIndex,
	pindex *PIndex) *pindexRestartReq {
	pindex.IndexUUID = addPlanPI.IndexUUID
	pindex.IndexParams = addPlanPI.IndexParams
	pindex.SourceParams = addPlanPI.SourceParams
	return &pindexRestartReq{
		pindex:         pindex,
		planPIndexName: addPlanPI.Name,
	}
}

func getPIndexesToRestart(pindexesToRemove []*PIndex,
	addPlanPIndexes []*PlanPIndex) []*pindexRestartReq {
	pindexesToRestart := make([]*pindexRestartReq, len(pindexesToRemove))
	i := 0
	for _, pindex := range pindexesToRemove {
		for _, addPlanPI := range addPlanPIndexes {
			if addPlanPI.SourcePartitions == pindex.SourcePartitions {
				pindex.IndexUUID = addPlanPI.IndexUUID
				pindex.IndexParams = addPlanPI.IndexParams
				pindex.SourceParams = addPlanPI.SourceParams
				pindexesToRestart[i] = &pindexRestartReq{
					pindex:         pindex,
					planPIndexName: addPlanPI.Name,
				}
				i++
			}
		}
	}
	return pindexesToRestart
}

func newPIndexHibernateReq(addPlanPI *PlanPIndex,
	pindex *PIndex) *pindexHibernateReq {
	pindex.IndexUUID = addPlanPI.IndexUUID
	pindex.IndexParams = addPlanPI.IndexParams
	pindex.SourceParams = addPlanPI.SourceParams
	pindex.HibernationPath = addPlanPI.HibernationPath
	return &pindexHibernateReq{
		pindex:     pindex,
		planPIndex: addPlanPI,
	}
}

func getPIndexesToHibernate(currPindexes []*PIndex,
	addPlanPIndexes []*PlanPIndex) []*pindexHibernateReq {
	pindexesToHibernate := make([]*pindexHibernateReq, len(currPindexes))
	i := 0
	for _, pindex := range currPindexes {
		for _, addPlanPI := range addPlanPIndexes {
			if addPlanPI.SourcePartitions == pindex.SourcePartitions {
				pindex.IndexUUID = addPlanPI.IndexUUID
				pindex.IndexParams = addPlanPI.IndexParams
				pindex.SourceParams = addPlanPI.SourceParams
				pindex.HibernationPath = addPlanPI.HibernationPath
				pindexesToHibernate[i] = &pindexHibernateReq{
					pindex:     pindex,
					planPIndex: addPlanPI,
				}
				i++
			}
		}
	}
	return pindexesToHibernate
}

func getIndexDefFromPIndex(pindex *PIndex) *IndexDef {
	if pindex != nil {
		return &IndexDef{Name: pindex.IndexName,
			UUID:            pindex.IndexUUID,
			SourceName:      pindex.SourceName,
			SourceParams:    pindex.SourceParams,
			SourceType:      pindex.SourceType,
			SourceUUID:      pindex.SourceUUID,
			Type:            pindex.IndexType,
			Params:          pindex.IndexParams,
			HibernationPath: pindex.HibernationPath,
		}
	}
	return nil
}

func getIndexDefFromPlanPIndexes(planPIndexes []*PlanPIndex) *IndexDef {
	if len(planPIndexes) != 0 && planPIndexes[0] != nil {
		return &IndexDef{Name: planPIndexes[0].IndexName,
			UUID:            planPIndexes[0].IndexUUID,
			SourceName:      planPIndexes[0].SourceName,
			SourceParams:    planPIndexes[0].SourceParams,
			SourceType:      planPIndexes[0].SourceType,
			SourceUUID:      planPIndexes[0].SourceUUID,
			Type:            planPIndexes[0].IndexType,
			Params:          planPIndexes[0].IndexParams,
			HibernationPath: planPIndexes[0].HibernationPath,
		}
	}
	return nil
}

func getSourcePartitionsMapFromPIndexes(pindexes []*PIndex) map[string]bool {
	sp := make(map[string]bool)
	if len(pindexes) > 0 {
		for _, pindex := range pindexes {
			if pindex != nil && pindex.SourcePartitions != "" {
				sp[pindex.SourcePartitions] = true
			}
		}
	}
	return sp
}

func getSourcePartitionsMapFromPlanPIndexes(
	planPIndexes []*PlanPIndex) map[string]bool {
	sp := make(map[string]bool)
	if len(planPIndexes) > 0 {
		for _, ppi := range planPIndexes {
			if ppi != nil && ppi.SourcePartitions != "" {
				sp[ppi.SourcePartitions] = true
			}
		}
	}
	return sp
}

func elicitAddPlanPIndexes(addPlanPIndexes []*PlanPIndex, errs []pindexRestartErr) []*PlanPIndex {
	pindexesToAdd := make([]*PlanPIndex, len(errs))
	for i, restartErr := range errs {
		for _, planPIndex := range addPlanPIndexes {
			if restartErr.pindex.IndexName == planPIndex.IndexName &&
				restartErr.pindex.SourcePartitions == planPIndex.SourcePartitions {
				pindexesToAdd[i] = planPIndex
				log.Printf("janitor: restart failed and attempting start "+
					"from scratch for pindex: %s", planPIndex.Name)
				break
			}
		}
	}
	return pindexesToAdd
}

func (mgr *Manager) reusablePIndexesPlanMap(currPIndexes map[string]*PIndex,
	wantedPlanPIndexes *PlanPIndexes) map[string]*PlanPIndex {
	if wantedPlanPIndexes == nil || currPIndexes == nil ||
		len(wantedPlanPIndexes.PlanPIndexes) == 0 {
		return nil
	}

	_, indexDefsMap, err := mgr.GetIndexDefs(true)
	if err != nil {
		log.Printf("janitor: reusablePIndexesPlanMap, "+
			"GetIndexDefs, err: %v", err)
		return nil
	}

	nodeDefs, err := mgr.GetNodeDefs(NODE_DEFS_WANTED, true)
	if err != nil {
		log.Printf("janitor: reusablePIndexesPlanMap, "+
			"GetNodeDefs, err: %v", err)
		return nil
	}

	mapWantedPlanPIndex := make(map[string]*PlanPIndex)

	for _, wantedPlanPIndex := range wantedPlanPIndexes.PlanPIndexes {
		// if the current pindex is a part of the newer plan then
		// check the possibility of a plan evolving phase like a
		// rebalance under progress so that we can skip any immediate
		// partition removals until the plan is finalised.
		if cp, exists := currPIndexes[wantedPlanPIndex.Name]; exists {
			indexDef := indexDefsMap[wantedPlanPIndex.IndexName]
			if mgr.planInProgress(cp, wantedPlanPIndex, indexDef, nodeDefs) &&
				PIndexMatchesPlan(cp, wantedPlanPIndex) {
				mapWantedPlanPIndex[wantedPlanPIndex.Name] = wantedPlanPIndex
				log.Printf("janitor: skipping removal of pindex %s "+
					" as it looks reloadable", wantedPlanPIndex.Name)
			}
		}
	}
	return mapWantedPlanPIndex
}

func (mgr *Manager) planInProgress(curPIndex *PIndex, planPIndex *PlanPIndex,
	indexDef *IndexDef, nodeDefs *NodeDefs) bool {
	if indexDef == nil || nodeDefs == nil {
		return false
	}

	// get the count of current partition-node assignments.
	cpna := len(planPIndex.Nodes)

	// get the count of wanted nodes.
	cwn := float64(len(nodeDefs.NodeDefs))

	// if the count of current partition-node assignment is zero or
	// lesser than the anticipated allocations as per the planParams
	// then we may conclude that the partition-node assignment
	// planning is still evolving or a rebalance is in progress.
	// Anticipated cpna would be the minimum value between the number
	// of wanted nodes and the replica count.
	if cpna < int(math.Min(float64(indexDef.PlanParams.NumReplicas+1), cwn)) ||
		cpna == 0 {
		// this applies to a failover-recovery usecase.
		return true
	}
	return false
}

// --------------------------------------------------------

// Functionally determine the delta of which pindexes need creation
// and which should be shut down on our local node (mgrUUID).
func CalcPIndexesDelta(mgrUUID string,
	currPIndexes map[string]*PIndex,
	wantedPlanPIndexes *PlanPIndexes,
	mapWantedPlanPIndex map[string]*PlanPIndex) (
	addPlanPIndexes []*PlanPIndex,
	removePIndexes []*PIndex) {
	// For fast transient lookups.
	if mapWantedPlanPIndex == nil {
		mapWantedPlanPIndex = map[string]*PlanPIndex{}
	}
	mapRemovePIndex := map[string]*PIndex{}

	// For each wanted plan pindex, if a pindex does not exist or is
	// different, then include for addition.
	for _, wantedPlanPIndex := range wantedPlanPIndexes.PlanPIndexes {

	nodeUUIDs:
		for nodeUUID, planPIndexNode := range wantedPlanPIndex.Nodes {
			if nodeUUID != mgrUUID || planPIndexNode == nil {
				continue nodeUUIDs
			}

			mapWantedPlanPIndex[wantedPlanPIndex.Name] = wantedPlanPIndex

			currPIndex, exists := currPIndexes[wantedPlanPIndex.Name]
			if !exists {
				addPlanPIndexes = append(addPlanPIndexes, wantedPlanPIndex)
			} else if !PIndexMatchesPlan(currPIndex, wantedPlanPIndex) {
				addPlanPIndexes = append(addPlanPIndexes, wantedPlanPIndex)
				removePIndexes = append(removePIndexes, currPIndex)
				mapRemovePIndex[currPIndex.Name] = currPIndex
			}

			break nodeUUIDs
		}
	}

	// For each existing pindex, if not part of wanted plan pindex,
	// then include for removal.
	for _, currPIndex := range currPIndexes {
		if _, exists := mapWantedPlanPIndex[currPIndex.Name]; !exists {
			if _, exists = mapRemovePIndex[currPIndex.Name]; !exists {
				removePIndexes = append(removePIndexes, currPIndex)
				mapRemovePIndex[currPIndex.Name] = currPIndex
			}
		}
	}

	return addPlanPIndexes, removePIndexes
}

// --------------------------------------------------------

func (mgr *Manager) GetHibernationBucketAndTask() (string, string) {
	// Assuming that only one bucket can be tracked at a time, since only 1 bucket
	// can be paused/resumed at a time.
	bucketTaskInHibernation := mgr.GetOption(bucketInHibernationKey)
	if bucketTaskInHibernation == NoBucketInHibernation {
		log.Printf("janitor: no hibernation bucket to track right now")
		// Removing any remaining trackers
		mgr.UnregisterBucketTracker()
		return "", ""
	}
	split := strings.SplitN(bucketTaskInHibernation, ":", 2)
	if len(split) < 2 {
		return "", ""
	}
	hibernationTask, bucketInHibernation := split[0], split[1]

	return bucketInHibernation, hibernationTask
}

// This function returns the hibernation task type and the hibernating bucket
// and source type if it isn't being tracked.
// 'Tracking' these buckets involves starting routines which track change in
// these buckets' states.
func (mgr *Manager) findHibernationBucketsToMonitor() (string, string, string) {
	bucketInHibernation, hibernationTask := mgr.GetHibernationBucketAndTask()

	currBucketBeingTracked := mgr.bucketInHibernation

	// If a bucket is there in cluster options, it has to be tracked
	_, currPIndexes := mgr.CurrentMaps()
	for _, pindex := range currPIndexes {
		// Track the bucket if not already being tracked.
		if pindex.SourceName == bucketInHibernation &&
			currBucketBeingTracked != bucketInHibernation {
			return hibernationTask, bucketInHibernation, pindex.SourceType
		}
	}

	return "", "", ""
}

// --------------------------------------------------------

// Functionally determine the delta of which feeds need creation and
// which should be shut down.  An updated feed would appear on both
// the removeFeeds and addFeeds outputs, which assumes the caller is
// going to remove feeds before adding feeds.
func CalcFeedsDelta(nodeUUID string, planPIndexes *PlanPIndexes,
	currFeeds map[string]Feed, pindexes map[string]*PIndex,
	feedAllotment string) (addFeeds [][]*PIndex, removeFeeds []Feed) {
	// Group the writable pindexes by their feed names.  Non-writable
	// pindexes (perhaps index ingest is paused) will have their feeds
	// removed.  Of note, currently, a pindex is never fed by >1 feed,
	// but a single feed may be emitting to multiple pindexes.
	groupedPIndexes := make(map[string][]*PIndex)
	for _, pindex := range pindexes {
		planPIndex, exists := planPIndexes.PlanPIndexes[pindex.Name]
		if exists && planPIndex != nil &&
			PlanPIndexNodeCanWrite(planPIndex.Nodes[nodeUUID]) {
			feedName := FeedNameForPIndex(pindex, feedAllotment)
			groupedPIndexes[feedName] =
				append(groupedPIndexes[feedName], pindex)
		}
	}

	removedFeeds := map[string]bool{}

	for feedName, feedPIndexes := range groupedPIndexes {
		currFeed, currFeedExists := currFeeds[feedName]
		if !currFeedExists {
			addFeeds = append(addFeeds, feedPIndexes)
		} else {
			changed := false
			currDests := currFeed.Dests()

		FIND_CHANGED:
			for _, feedPIndex := range feedPIndexes {
				sourcePartitions :=
					strings.Split(feedPIndex.SourcePartitions, ",")
				for _, sourcePartition := range sourcePartitions {
					if _, exists := currDests[sourcePartition]; !exists {
						changed = true
						break FIND_CHANGED
					}
				}
			}

			if changed {
				addFeeds = append(addFeeds, feedPIndexes)

				if currFeeds[feedName] != nil {
					if !removedFeeds[feedName] {
						removeFeeds = append(removeFeeds, currFeeds[feedName])
						removedFeeds[feedName] = true
					}
				}
			}
		}
	}

	for currFeedName, currFeed := range currFeeds {
		if _, exists := groupedPIndexes[currFeedName]; !exists {
			if !removedFeeds[currFeedName] {
				removeFeeds = append(removeFeeds, currFeed)
				removedFeeds[currFeedName] = true
			}
		}
	}

	return addFeeds, removeFeeds
}

func ParseFeedAllotmentOption(sourceParams string) (string, error) {
	var sourceParamsMap map[string]interface{}
	err := UnmarshalJSON([]byte(sourceParams), &sourceParamsMap)
	if err != nil {
		return "", fmt.Errorf("manager_janitor: ParseFeedAllotmentOption"+
			" json parse sourceParams: %s, err: %v",
			sourceParams, err)
	}

	if sourceParamsMap != nil {
		v, exists := sourceParamsMap["feedAllotment"]
		if exists {
			feedAllotmentOption, ok := v.(string)
			if ok {
				return feedAllotmentOption, nil
			}
		}
	}
	return "", err
}

func GetFeedAllotmentOption(sourceParams string) string {
	if len(sourceParams) > 0 {
		sp, err := ParseFeedAllotmentOption(sourceParams)
		if err != nil {
			log.Errorf("manager_janitor: feedAllotment, err: %v", err)
		}
		return sp
	}
	return ""
}

// FeedNameForPIndex functionally computes the name of a feed given a pindex.
func FeedNameForPIndex(pindex *PIndex, defaultFeedAllotment string) string {
	feedAllotment := GetFeedAllotmentOption(pindex.SourceParams)
	if feedAllotment == "" {
		feedAllotment = defaultFeedAllotment
	}

	if feedAllotment == FeedAllotmentOnePerPIndex {
		// Using the pindex.Name for the feed name means each pindex
		// will have its own, independent feed.
		return pindex.Name
	}

	// In contrast, the original default behavior was to use the
	// indexName+indexUUID as the computed feed name, which means that
	// the multiple pindexes from a single index will share a feed.
	// In other words, there will be a feed per index.
	//
	// NOTE, in this feed-per-index approach, we're depending on the
	// IndexName/IndexUUID to "cover" the SourceType, SourceName,
	// SourceUUID, SourceParams values, so we don't need to encode
	// those source parts into the feed name.
	//
	return pindex.IndexName + "_" + pindex.IndexUUID
}

// --------------------------------------------------------

func (mgr *Manager) startPIndex(planPIndex *PlanPIndex) error {
	var pindex *PIndex
	var err error

	path := mgr.PIndexPath(planPIndex.Name)
	// First, try reading the path with OpenPIndex().  An
	// existing path might happen during a case of rollback.
	_, err = os.Stat(path)
	if err == nil {
		pindex, err = OpenPIndex(mgr, path)
		if err != nil {
			log.Errorf("janitor: startPIndex, OpenPIndex error,"+
				" cleaning up and trying NewPIndex,"+
				" path: %s, err: %v", path, err)
			os.RemoveAll(path)
		} else {
			if !PIndexMatchesPlan(pindex, planPIndex) {
				log.Errorf("janitor: startPIndex, pindex does not match plan,"+
					" cleaning up and trying NewPIndex, path: %s, err: %v",
					path, err)
				pindex.Close(true)
				pindex = nil
			}
		}
	}

	if pindex == nil {
		pindex, err = NewPIndex(mgr, planPIndex.Name, NewUUID(),
			planPIndex.IndexType,
			planPIndex.IndexName,
			planPIndex.IndexUUID,
			planPIndex.IndexParams,
			planPIndex.SourceType,
			planPIndex.SourceName,
			planPIndex.SourceUUID,
			planPIndex.SourceParams,
			planPIndex.SourcePartitions,
			path)
		if err != nil {
			return fmt.Errorf("janitor: NewPIndex, name: %s, err: %v",
				planPIndex.Name, err)
		}
	}

	pindex.HibernationPath = planPIndex.HibernationPath

	err = mgr.registerPIndex(pindex)
	if err != nil {
		pindex.Close(true)
		return err
	}
	return nil
}

func (mgr *Manager) stopPIndex(pindex *PIndex, remove bool) error {
	// First, stop any feeds that might be sending to the pindex's dest.
	err := mgr.stopPIndexFeeds(pindex)
	if err != nil {
		return err
	}

	if pindex.Dest != nil {
		buf := bytes.NewBuffer(nil)
		buf.Write([]byte(fmt.Sprintf(
			`{"event":"stopPIndex","name":"%s","remove":%t,"time":"%s","stats":`,
			pindex.Name, remove, time.Now().Format(time.RFC3339Nano))))
		err := pindex.Dest.Stats(buf)
		if err == nil {
			buf.Write(JsonCloseBrace)
			mgr.AddEvent(buf.Bytes())
		}
	}

	// We provide an exact pindex to match when calling
	// unregisterPIndex to handle the case there was a race, where a
	// new pindex was started before we could shutdown the old pindex.
	pindexUnreg := mgr.unregisterPIndex(pindex.Name, pindex)
	if pindexUnreg != nil && pindexUnreg != pindex {
		return fmt.Errorf("janitor: unregistered pindex during stopPIndex,"+
			" pindex: %#v, pindexUnreg: %#v", pindex, pindexUnreg)
	}

	if remove {
		atomic.AddUint64(&mgr.stats.TotJanitorRemovePIndex, 1)
	} else {
		atomic.AddUint64(&mgr.stats.TotJanitorClosePIndex, 1)
	}

	return pindex.Close(remove)
}

// --------------------------------------------------------

func (mgr *Manager) startFeed(pindexes []*PIndex) error {
	if len(pindexes) <= 0 {
		return nil
	}

	feedAllotment := mgr.GetOption(FeedAllotmentOption)

	pindexFirst := pindexes[0]
	feedName := FeedNameForPIndex(pindexFirst, feedAllotment)

	dests := make(map[string]Dest)
	for _, pindex := range pindexes {
		if f := FeedNameForPIndex(pindex, feedAllotment); f != feedName {
			return fmt.Errorf("janitor: unexpected feedName: %s != %s,"+
				" pindex: %#v", f, feedName, pindex)
		}

		addSourcePartition := func(sourcePartition string) error {
			if _, exists := dests[sourcePartition]; exists {
				return fmt.Errorf("janitor: startFeed collision,"+
					" sourcePartition: %s, feedName: %s, pindex: %#v",
					sourcePartition, feedName, pindex)
			}
			dests[sourcePartition] = pindex.Dest
			return nil
		}

		if pindex.SourcePartitions == "" {
			err := addSourcePartition("")
			if err != nil {
				return err
			}
		} else {
			sourcePartitionsArr := strings.Split(pindex.SourcePartitions, ",")
			for _, sourcePartition := range sourcePartitionsArr {
				err := addSourcePartition(sourcePartition)
				if err != nil {
					return err
				}
			}
		}
	}

	return mgr.startFeedByType(feedName,
		pindexFirst.IndexName, pindexFirst.IndexUUID,
		pindexFirst.SourceType, pindexFirst.SourceName,
		pindexFirst.SourceUUID, pindexFirst.SourceParams,
		dests)
}

func (mgr *Manager) stopPIndexFeeds(pindex *PIndex) error {
	feeds, _ := mgr.CurrentMaps()
	for _, feed := range feeds {
		for _, dest := range feed.Dests() {
			if dest == pindex.Dest {
				err := mgr.stopFeed(feed)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (mgr *Manager) trackResumeBucketState(source, sourceType string) {
	mgr.RegisterHibernationBucketTracker(source)

	if UnhibernationBucketStateTrackerHook != nil {
		go UnhibernationBucketStateTrackerHook(mgr, source, sourceType)
	}
}

func (mgr *Manager) trackPauseBucketState(source, sourceType string) {
	mgr.RegisterHibernationBucketTracker(source)

	if HibernationBucketStateTrackerHook != nil {
		go HibernationBucketStateTrackerHook(mgr, source, sourceType)
	}
}

// TODO: Need way to track dead cows (non-beef)
// TODO: Need a way to collect these errors so REST api
// can show them to user ("hey, perhaps you deleted a bucket
// and should delete these related full-text indexes?
// or the couchbase cluster is just down.");
// perhaps as specialized clog writer?

func (mgr *Manager) startFeedByType(feedName, indexName, indexUUID,
	sourceType, sourceName, sourceUUID, sourceParams string,
	dests map[string]Dest) error {
	feedType, exists := FeedTypes[sourceType]
	if !exists || feedType == nil {
		return fmt.Errorf("janitor: unknown sourceType: %s", sourceType)
	}

	return feedType.Start(mgr, feedName, indexName, indexUUID,
		sourceType, sourceName, sourceUUID, sourceParams, dests)
}

func (mgr *Manager) stopFeed(feed Feed) error {
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte(fmt.Sprintf(
		`{"event":"stopFeed","name":"%s","time":"%s","stats":`,
		feed.Name(), time.Now().Format(time.RFC3339Nano))))
	err := feed.Stats(buf)
	if err == nil {
		buf.Write(JsonCloseBrace)
		mgr.AddEvent(buf.Bytes())
	}

	feedUnreg := mgr.unregisterFeed(feed.Name())
	if feedUnreg != nil && feedUnreg != feed {
		return fmt.Errorf("janitor: unregistered feed during stopFeed,"+
			" feed: %#v, feedUnreg: %#v", feed, feedUnreg)
	}

	// NOTE: We're depending on feed to synchronously close, so we
	// know it'll no longer be sending to any of its dests anymore.
	return feed.Close()
}
