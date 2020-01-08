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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
const JANITOR_LOAD_DATA_DIR = "janitor_load_data_dir"

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

// JanitorLoop is the main loop for the janitor.
func (mgr *Manager) JanitorLoop() {
	if mgr.cfg != nil { // Might be nil for testing.
		go func() {
			ec := make(chan CfgEvent)
			mgr.cfg.Subscribe(PLAN_PINDEXES_KEY, ec)
			mgr.cfg.Subscribe(PLAN_PINDEXES_DIRECTORY_STAMP, ec)
			mgr.cfg.Subscribe(CfgNodeDefsKey(NODE_DEFS_WANTED), ec)
			for {
				select {
				case <-mgr.stopCh:
					return
				case e := <-ec:
					atomic.AddUint64(&mgr.stats.TotJanitorSubscriptionEvent, 1)
					mgr.JanitorKick("cfg changed, key: " + e.Key)
				}
			}
		}()
	}

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
				mgr.stopPIndex(m.obj.(*PIndex), false)
				atomic.AddUint64(&mgr.stats.TotJanitorClosePIndex, 1)
			} else if m.op == JANITOR_REMOVE_PINDEX {
				mgr.stopPIndex(m.obj.(*PIndex), true)
				atomic.AddUint64(&mgr.stats.TotJanitorRemovePIndex, 1)
			} else if m.op == JANITOR_LOAD_DATA_DIR {
				mgr.LoadDataDir()
				atomic.AddUint64(&mgr.stats.TotJanitorLoadDataDir, 1)
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
				if mgr.bootingPIndex(pi.Name) {
					continue
				}
				err := mgr.startPIndex(pi)
				if err != nil {
					responseCh <- fmt.Errorf("janitor: adding pindex: %s, err: %v",
						pi.Name, err)
				}
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
		buf, err := json.Marshal(pi)
		if err != nil {
			cleanDir(newPath)
			return fmt.Errorf("janitor: restartPIndex"+
				" Marshal pindex: %s, err: %v", pi.Name, err)

		}
		err = ioutil.WriteFile(pi.Path+string(os.PathSeparator)+
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
		log.Printf("janitor: restartPIndex err: %v", resp.err)
		errs = append(errs, *resp)
	}
	return errs
}

// JanitorOnce is the main body of a JanitorLoop.
func (mgr *Manager) JanitorOnce(reason string) error {
	if mgr.cfg == nil { // Can occur during testing.
		return fmt.Errorf("janitor: skipped due to nil cfg")
	}

	feedAllotment := mgr.GetOptions()[FeedAllotmentOption]

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

	addPlanPIndexes, removePIndexes :=
		CalcPIndexesDelta(mgr.uuid, currPIndexes, planPIndexes)

	// check for any pindexes for restart and get classified lists of
	// pindexes to add, remove and restart
	planPIndexesToAdd, pindexesToRemove, pindexesToRestart :=
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

	addFeeds, removeFeeds :=
		CalcFeedsDelta(mgr.uuid, planPIndexes, currFeeds, currPIndexes,
			feedAllotment)

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

func classifyAddRemoveRestartPIndexes(mgr *Manager, addPlanPIndexes []*PlanPIndex,
	removePIndexes []*PIndex) (planPIndexesToAdd []*PlanPIndex,
	pindexesToRemove []*PIndex, pindexesToRestart []*pindexRestartReq) {
	// if there are no pindexes to be removed as per planner,
	// then there won't be anything to restart as well.
	if len(removePIndexes) == 0 {
		return addPlanPIndexes, nil, nil
	}
	pindexesToRestart = make([]*pindexRestartReq, 0)
	pindexesToRemove = make([]*PIndex, 0)
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
	if v, ok := mgr.Options()["rebuildOnReplicaUpdate"]; !ok ||
		v != "true" {
		return advPIndexClassifier(indexPIndexMap, indexPlanPIndexMap)
	}

	// take every pindex to remove and check the config change
	// and sort out the pindexes to add, remove or restart
	for indexName, pindexes := range indexPIndexMap {
		if len(pindexes) > 0 && pindexes[0] != nil {
			pindex := pindexes[0]
			if planPIndexes, ok := indexPlanPIndexMap[indexName]; ok {
				configAnalyzeReq := &ConfigAnalyzeRequest{
					IndexDefnCur: getIndexDefFromPlanPIndexes(
						planPIndexes),
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
				if pindexImplType.AnalyzeIndexDefUpdates != nil &&
					pindexImplType.AnalyzeIndexDefUpdates(configAnalyzeReq) ==
						PINDEXES_RESTART {
					pindexesToRestart = append(pindexesToRestart,
						getPIndexesToRestart(pindexes, planPIndexes)...)
					continue
				}
				pindexesToRemove = append(pindexesToRemove, pindexes...)
				planPIndexesToAdd = append(planPIndexesToAdd, planPIndexes...)
			} else {
				pindexesToRemove = append(pindexesToRemove, pindexes...)
			}
		}
	}
	return planPIndexesToAdd, pindexesToRemove, pindexesToRestart
}

func advPIndexClassifier(indexPIndexMap map[string][]*PIndex,
	indexPlanPIndexMap map[string][]*PlanPIndex) (planPIndexesToAdd []*PlanPIndex,
	pindexesToRemove []*PIndex, pindexesToRestart []*pindexRestartReq) {
	pindexesToRestart = make([]*pindexRestartReq, 0)
	pindexesToRemove = make([]*PIndex, 0)
	planPIndexesToAdd = make([]*PlanPIndex, 0)

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
					// check for restartability on the target plan
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

	return planPIndexesToAdd, pindexesToRemove, pindexesToRestart
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

func getIndexDefFromPIndex(pindex *PIndex) *IndexDef {
	if pindex != nil {
		return &IndexDef{Name: pindex.IndexName,
			UUID:         pindex.IndexUUID,
			SourceName:   pindex.SourceName,
			SourceParams: pindex.SourceParams,
			SourceType:   pindex.SourceType,
			SourceUUID:   pindex.SourceUUID,
			Type:         pindex.IndexType,
			Params:       pindex.IndexParams,
		}
	}
	return nil
}

func getIndexDefFromPlanPIndexes(planPIndexes []*PlanPIndex) *IndexDef {
	if len(planPIndexes) != 0 && planPIndexes[0] != nil {
		return &IndexDef{Name: planPIndexes[0].IndexName,
			UUID:         planPIndexes[0].IndexUUID,
			SourceName:   planPIndexes[0].SourceName,
			SourceParams: planPIndexes[0].SourceParams,
			SourceType:   planPIndexes[0].SourceType,
			SourceUUID:   planPIndexes[0].SourceUUID,
			Type:         planPIndexes[0].IndexType,
			Params:       planPIndexes[0].IndexParams,
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

// --------------------------------------------------------

// Functionally determine the delta of which pindexes need creation
// and which should be shut down on our local node (mgrUUID).
func CalcPIndexesDelta(mgrUUID string,
	currPIndexes map[string]*PIndex,
	wantedPlanPIndexes *PlanPIndexes) (
	addPlanPIndexes []*PlanPIndex,
	removePIndexes []*PIndex) {
	// For fast transient lookups.
	mapWantedPlanPIndex := map[string]*PlanPIndex{}
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
			} else if PIndexMatchesPlan(currPIndex, wantedPlanPIndex) == false {
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
	err := json.Unmarshal([]byte(sourceParams), &sourceParamsMap)
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

func feedAllotmentOption(sourceParams string) string {
	if len(sourceParams) > 0 {
		sp, err := ParseFeedAllotmentOption(sourceParams)
		if err != nil {
			log.Printf("manager_janitor: feedAllotment, err: %v", err)
		}
		return sp
	}
	return ""
}

// FeedNameForPIndex functionally computes the name of a feed given a pindex.
func FeedNameForPIndex(pindex *PIndex, defaultFeedAllotment string) string {
	feedAllotment := feedAllotmentOption(pindex.SourceParams)
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
			log.Printf("janitor: startPIndex, OpenPIndex error,"+
				" cleaning up and trying NewPIndex,"+
				" path: %s, err: %v", path, err)
			os.RemoveAll(path)
		} else {
			if !PIndexMatchesPlan(pindex, planPIndex) {
				log.Printf("janitor: startPIndex, pindex does not match plan,"+
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

	err = mgr.registerPIndex(pindex)
	if err != nil {
		pindex.Close(true)
		return err
	}
	return nil
}

func (mgr *Manager) stopPIndex(pindex *PIndex, remove bool) error {
	// First, stop any feeds that might be sending to the pindex's dest.
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

	return pindex.Close(remove)
}

// --------------------------------------------------------

func (mgr *Manager) startFeed(pindexes []*PIndex) error {
	if len(pindexes) <= 0 {
		return nil
	}

	feedAllotment := mgr.GetOptions()[FeedAllotmentOption]

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
