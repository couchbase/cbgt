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
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
)

const JANITOR_CLOSE_PINDEX = "janitor_close_pindex"
const JANITOR_REMOVE_PINDEX = "janitor_remove_pindex"

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
					log.Printf("janitor: JanitorOnce, err: %v", err)
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

// JanitorOnce is the main body of a JanitorLoop.
func (mgr *Manager) JanitorOnce(reason string) error {
	if mgr.cfg == nil { // Can occur during testing.
		return fmt.Errorf("janitor: skipped due to nil cfg")
	}

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

	currFeeds, currPIndexes := mgr.CurrentMaps()

	addPlanPIndexes, removePIndexes :=
		CalcPIndexesDelta(mgr.uuid, currPIndexes, planPIndexes)

	log.Printf("janitor: pindexes to remove: %d", len(removePIndexes))
	for _, pi := range removePIndexes {
		log.Printf("  %+v", pi)
	}
	log.Printf("janitor: pindexes to add: %d", len(addPlanPIndexes))
	for _, ppi := range addPlanPIndexes {
		log.Printf("  %+v", ppi)
	}

	var errs []error

	// First, teardown pindexes that need to be removed.
	for _, removePIndex := range removePIndexes {
		log.Printf("janitor: removing pindex: %s", removePIndex.Name)
		err = mgr.stopPIndex(removePIndex, true)
		if err != nil {
			errs = append(errs,
				fmt.Errorf("janitor: removing pindex: %s, err: %v",
					removePIndex.Name, err))
		}
	}
	// Then, (re-)create pindexes that we're missing.
	for _, addPlanPIndex := range addPlanPIndexes {
		log.Printf("janitor: adding pindex: %s", addPlanPIndex.Name)
		err = mgr.startPIndex(addPlanPIndex)
		if err != nil {
			errs = append(errs,
				fmt.Errorf("janitor: adding pindex: %s, err: %v",
					addPlanPIndex.Name, err))
		}
	}

	currFeeds, currPIndexes = mgr.CurrentMaps()

	addFeeds, removeFeeds :=
		CalcFeedsDelta(mgr.uuid, planPIndexes, currFeeds, currPIndexes)

	log.Printf("janitor: feeds to remove: %d", len(removeFeeds))
	for _, removeFeed := range removeFeeds {
		log.Printf("  %s", removeFeed.Name())
	}
	log.Printf("janitor: feeds to add: %d", len(addFeeds))
	for _, targetPIndexes := range addFeeds {
		if len(targetPIndexes) > 0 {
			log.Printf("  %s", FeedName(targetPIndexes[0]))
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
	currFeeds map[string]Feed, pindexes map[string]*PIndex) (
	addFeeds [][]*PIndex, removeFeeds []Feed) {
	// Group the writable pindexes by their feed names.  Non-writable
	// pindexes (perhaps index ingest is paused) will have their feeds
	// removed.  Of note, currently, a pindex is never fed by >1 feed,
	// but a single feed may be emitting to multiple pindexes.
	groupedPIndexes := make(map[string][]*PIndex)
	for _, pindex := range pindexes {
		planPIndex, exists := planPIndexes.PlanPIndexes[pindex.Name]
		if exists && planPIndex != nil &&
			PlanPIndexNodeCanWrite(planPIndex.Nodes[nodeUUID]) {
			feedName := FeedName(pindex)
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

// FeedName functionally computes the name of a feed given a pindex.
//
// NOTE: We're depending on the IndexName/IndexUUID to "cover" the
// SourceType, SourceName, SourceUUID, SourceParams values, so we
// don't need to encode those source parts into the feed name.
func FeedName(pindex *PIndex) string {
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

	pindexFirst := pindexes[0]
	feedName := FeedName(pindexFirst)

	dests := make(map[string]Dest)
	for _, pindex := range pindexes {
		if f := FeedName(pindex); f != feedName {
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
