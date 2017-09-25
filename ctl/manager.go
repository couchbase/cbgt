// @author Couchbase <info@couchbase.com>
// @copyright 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt/rebalance"

	"github.com/couchbase/cbauth/service"
)

// CtlMgr implements the cbauth/service.Manager interface and
// provides the adapter or glue between ns-server's service API
// and cbgt's Ctl implementation.
type CtlMgr struct {
	nodeInfo *service.NodeInfo

	ctl *Ctl

	taskProgressCh chan taskProgress

	mu sync.Mutex // Protects the fields that follow.

	revNumNext uint64 // The next rev num to use.

	tasks       tasks
	tasksWaitCh chan struct{} // Closed when the tasks change.

	lastTaskList service.TaskList
	lastTopology service.Topology
}

type tasks struct {
	revNum      uint64
	taskHandles []*taskHandle
}

type taskHandle struct { // The taskHandle fields are immutable.
	startTime time.Time
	task      *service.Task
	stop      func() // May be nil.
}

type taskProgress struct {
	taskId         string
	errs           []error
	progressExists bool
	progress       float64
}

// ------------------------------------------------

func NewCtlMgr(nodeInfo *service.NodeInfo, ctl *Ctl) *CtlMgr {
	m := &CtlMgr{
		nodeInfo:       nodeInfo,
		ctl:            ctl,
		revNumNext:     1,
		tasks:          tasks{revNum: 0},
		taskProgressCh: make(chan taskProgress, 10),
	}

	go func() {
		for taskProgress := range m.taskProgressCh {
			m.handleTaskProgress(taskProgress)
		}
	}()

	return m
}

func (m *CtlMgr) GetNodeInfo() (*service.NodeInfo, error) {
	log.Printf("ctl/manager: GetNodeInfo")

	return m.nodeInfo, nil
}

func (m *CtlMgr) Shutdown() error {
	log.Printf("ctl/manager: Shutdown")

	os.Exit(0)
	return nil
}

func (m *CtlMgr) GetTaskList(haveTasksRev service.Revision,
	cancelCh service.Cancel) (*service.TaskList, error) {
	m.mu.Lock()

	if len(haveTasksRev) > 0 {
		haveTasksRevNum, err := DecodeRev(haveTasksRev)
		if err != nil {
			m.mu.Unlock()

			log.Errorf("ctl/manager: GetTaskList, DecodeRev"+
				", haveTasksRev: %s, err: %v", haveTasksRev, err)

			return nil, err
		}

		for haveTasksRevNum == m.tasks.revNum {
			if m.tasksWaitCh == nil {
				m.tasksWaitCh = make(chan struct{})
			}
			tasksWaitCh := m.tasksWaitCh

			m.mu.Unlock()
			select {
			case <-cancelCh:
				return nil, service.ErrCanceled

			case <-tasksWaitCh:
				// FALLTHRU
			}
			m.mu.Lock()
		}
	}

	rv := m.getTaskListLOCKED()

	m.lastTaskList.Rev = rv.Rev
	same := reflect.DeepEqual(&m.lastTaskList, rv)
	m.lastTaskList = *rv

	m.mu.Unlock()

	if !same {
		log.Printf("ctl/manager: GetTaskList, haveTasksRev: %s,"+
			" changed, rv: %+v", haveTasksRev, rv)
	}

	return rv, nil
}

func (m *CtlMgr) CancelTask(
	taskId string, taskRev service.Revision) error {
	log.Printf("ctl/manager: CancelTask, taskId: %s, taskRev: %s",
		taskId, taskRev)

	m.mu.Lock()
	defer m.mu.Unlock()

	canceled := false

	taskHandlesNext := []*taskHandle(nil)

	for _, taskHandle := range m.tasks.taskHandles {
		task := taskHandle.task
		if task.ID == taskId {
			if taskRev != nil &&
				len(taskRev) > 0 &&
				!bytes.Equal(taskRev, task.Rev) {
				log.Errorf("ctl/manager: CancelTask,"+
					" taskId: %s, taskRev: %v, err: %v",
					taskId, taskRev, service.ErrConflict)
				return service.ErrConflict
			}

			if !task.IsCancelable {
				log.Errorf("ctl/manager: CancelTask,"+
					" taskId: %s, taskRev: %v, err: %v",
					taskId, taskRev, service.ErrNotSupported)
				return service.ErrNotSupported
			}

			if taskHandle.stop != nil {
				taskHandle.stop()
			} else {
				log.Printf("ctl/manager: CancelTask, taskId: %s, taskRev: %v,"+
					" nil taskHandle", taskId, taskRev)
			}

			canceled = true
		} else {
			taskHandlesNext = append(taskHandlesNext, taskHandle)
		}
	}

	if !canceled {
		log.Errorf("ctl/manager: CancelTask, taskId: %s, taskRev: %v, err: %v",
			taskId, taskRev, service.ErrNotFound)
		return service.ErrNotFound
	}

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})

	log.Printf("ctl/manager: CancelTask, taskId: %s, taskRev: %v, done",
		taskId, taskRev)

	return nil
}

func (m *CtlMgr) GetCurrentTopology(haveTopologyRev service.Revision,
	cancelCh service.Cancel) (*service.Topology, error) {
	ctlTopology, err :=
		m.ctl.WaitGetTopology(string(haveTopologyRev), cancelCh)
	if err != nil {
		if err != service.ErrCanceled {
			log.Errorf("ctl/manager: GetCurrentTopology,"+
				" haveTopologyRev: %s, err: %v", haveTopologyRev, err)
		}

		return nil, err
	}

	rv := &service.Topology{
		Rev:   service.Revision([]byte(ctlTopology.Rev)),
		Nodes: []service.NodeID{},
	}

	for _, ctlNode := range ctlTopology.MemberNodes {
		rv.Nodes = append(rv.Nodes, service.NodeID(ctlNode.UUID))
	}

	// TODO: Need a proper IsBalanced computation.
	rv.IsBalanced =
		len(ctlTopology.PrevWarnings) <= 0 && len(ctlTopology.PrevErrs) <= 0

	for resourceName, resourceWarnings := range ctlTopology.PrevWarnings {
		aggregate := map[string]bool{}
		for _, resourceWarning := range resourceWarnings {
			if strings.HasPrefix(resourceWarning, "could not meet constraints") {
				aggregate["could not meet replication constraints"] = true
			} else {
				aggregate[resourceWarning] = true
			}
		}

		for resourceWarning := range aggregate {
			rv.Messages = append(rv.Messages,
				fmt.Sprintf("warning: resource: %q -- %s",
					resourceName, resourceWarning))
		}
	}

	for _, err := range ctlTopology.PrevErrs {
		rv.Messages = append(rv.Messages, fmt.Sprintf("error: %v", err))
	}

	m.mu.Lock()
	m.lastTopology.Rev = rv.Rev
	same := reflect.DeepEqual(&m.lastTopology, rv)
	m.lastTopology = *rv
	m.mu.Unlock()

	if !same {
		log.Printf("ctl/manager: GetCurrentTopology, haveTopologyRev: %s,"+
			" changed, rv: %+v", haveTopologyRev, rv)
	}

	return rv, nil
}

func (m *CtlMgr) PrepareTopologyChange(
	change service.TopologyChange) error {
	log.Printf("ctl/manager: PrepareTopologyChange, change: %v", change)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Possible for caller to not care about current topology, but
	// just wants to impose or force a topology change.
	if len(change.CurrentTopologyRev) > 0 &&
		string(change.CurrentTopologyRev) != m.ctl.GetTopology().Rev {
		log.Errorf("ctl/manager: PrepareTopologyChange, rev check, err: %v",
			service.ErrConflict)
		return service.ErrConflict
	}

	for _, taskHandle := range m.tasks.taskHandles {
		if taskHandle.task.Type == service.TaskTypePrepared ||
			taskHandle.task.Type == service.TaskTypeRebalance {
			// NOTE: If there's an existing rebalance or preparation
			// task, even if it's done, then treat as a conflict, as
			// the caller should cancel them all first.
			log.Errorf("ctl/manager: PrepareTopologyChange, "+
				"task type check, err: %v", service.ErrConflict)
			return service.ErrConflict
		}
	}

	revNum := m.allocRevNumLOCKED(0)

	taskHandlesNext := append([]*taskHandle(nil),
		m.tasks.taskHandles...)
	taskHandlesNext = append(taskHandlesNext,
		&taskHandle{
			startTime: time.Now(),
			task: &service.Task{
				Rev:              EncodeRev(revNum),
				ID:               "prepare:" + change.ID,
				Type:             service.TaskTypePrepared,
				Status:           service.TaskStatusRunning,
				IsCancelable:     true,
				Progress:         100.0, // Prepared born as 100.0 is ok.
				DetailedProgress: nil,
				Description:      "prepare topology change",
				ErrorMessage:     "",
				Extra: map[string]interface{}{
					"topologyChange": change,
				},
			},
		})

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})

	// if the current node is in keep list, checking further
	// for a reregister
	for _, node := range change.KeepNodes {
		if m.nodeInfo.NodeID == node.NodeInfo.NodeID {
			m.ctl.checkAndReregisterSelf(string(m.nodeInfo.NodeID))
		}
	}

	log.Printf("ctl/manager: PrepareTopologyChange, done")

	return nil
}

func (m *CtlMgr) StartTopologyChange(change service.TopologyChange) error {
	log.Printf("ctl/manager: StartTopologyChange, change: %v", change)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Possible for caller to not care about current topology, but
	// just wants to impose or force a topology change.
	if len(change.CurrentTopologyRev) > 0 &&
		string(change.CurrentTopologyRev) != m.ctl.GetTopology().Rev {
		log.Errorf("ctl/manager: StartTopologyChange, rev check, err: %v",
			service.ErrConflict)
		return service.ErrConflict
	}

	var err error

	started := false

	var taskHandlesNext []*taskHandle

	for _, th := range m.tasks.taskHandles {
		if th.task.Type == service.TaskTypeRebalance {
			log.Errorf("ctl/manager: StartTopologyChange,"+
				" task rebalance check, err: %v", service.ErrConflict)
			return service.ErrConflict
		}

		if th.task.Type == service.TaskTypePrepared {
			th, err = m.startTopologyChangeTaskHandleLOCKED(change)
			if err != nil {
				log.Errorf("ctl/manager: StartTopologyChange,"+
					" prepared, err: %v", err)
				return err
			}

			started = true
		}

		taskHandlesNext = append(taskHandlesNext, th)
	}

	if !started {
		return service.ErrNotFound
	}

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})

	log.Printf("ctl/manager: StartTopologyChange, started")

	return nil
}

func (m *CtlMgr) startTopologyChangeTaskHandleLOCKED(
	change service.TopologyChange) (*taskHandle, error) {
	ctlChangeTopology := &CtlChangeTopology{
		Rev: string(change.CurrentTopologyRev),
	}

	switch change.Type {
	case service.TopologyChangeTypeRebalance:
		ctlChangeTopology.Mode = "rebalance"

	case service.TopologyChangeTypeFailover:
		ctlChangeTopology.Mode = "failover-hard"

	default:
		log.Warnf("ctl/manager: unknown change.Type: %v", change.Type)
		return nil, service.ErrNotSupported
	}

	for _, node := range change.KeepNodes {
		// TODO: What about node.RecoveryType?

		nodeUUID := string(node.NodeInfo.NodeID)

		ctlChangeTopology.MemberNodeUUIDs =
			append(ctlChangeTopology.MemberNodeUUIDs, nodeUUID)
	}

	taskId := "rebalance:" + change.ID

	// The progressEntries is a map of pindex ->
	// source_partition -> node -> *rebalance.ProgressEntry.
	onProgress := func(maxNodeLen, maxPIndexLen int,
		seenNodes map[string]bool,
		seenNodesSorted []string,
		seenPIndexes map[string]bool,
		seenPIndexesSorted []string,
		progressEntries map[string]map[string]map[string]*rebalance.ProgressEntry,
		errs []error,
	) string {
		m.updateProgress(taskId, seenNodes, seenPIndexes, progressEntries, errs)

		if progressEntries == nil {
			return "DONE"
		}

		return rebalance.ProgressTableString(
			maxNodeLen, maxPIndexLen,
			seenNodes,
			seenNodesSorted,
			seenPIndexes,
			seenPIndexesSorted,
			progressEntries)
	}

	ctlTopology, err := m.ctl.ChangeTopology(ctlChangeTopology, onProgress)
	if err != nil {
		return nil, err
	}

	revNum := m.allocRevNumLOCKED(m.tasks.revNum)

	th := &taskHandle{
		startTime: time.Now(),
		task: &service.Task{
			Rev:              EncodeRev(revNum),
			ID:               taskId,
			Type:             service.TaskTypeRebalance,
			Status:           service.TaskStatusRunning,
			IsCancelable:     true,
			Progress:         0.0,
			DetailedProgress: map[service.NodeID]float64{},
			Description:      "topology change",
			ErrorMessage:     "",
			Extra: map[string]interface{}{
				"topologyChange": change,
			},
		},
		stop: func() {
			log.Printf("ctl/manager: stop taskHandle, ctlTopology.Rev: %v",
				ctlTopology.Rev)

			m.ctl.StopChangeTopology(ctlTopology.Rev)
		},
	}

	return th, nil
}

func (m *CtlMgr) computeProgPercent(pe *rebalance.ProgressEntry,
	sourcePartitions map[string]map[string]*rebalance.ProgressEntry) float64 {
	totPct, avgPct := 0.0, -1.1
	numPct := 0
	if pe != nil {
		if sourcePartitions != nil {
			for _, nodes := range sourcePartitions {
				pex := nodes[pe.Node]
				if pex == nil || pex.WantUUIDSeq.UUID == "" {
					continue
				}

				if pex.WantUUIDSeq.Seq <= pex.CurrUUIDSeq.Seq {
					totPct = totPct + 1.0
					numPct = numPct + 1
					continue
				}

				n := pex.CurrUUIDSeq.Seq - pex.InitUUIDSeq.Seq
				d := pex.WantUUIDSeq.Seq - pex.InitUUIDSeq.Seq
				if d > 0 {
					pct := float64(n) / float64(d)
					totPct = totPct + pct
					numPct = numPct + 1
				}
			}
		}
	}
	if numPct > 0 {
		avgPct = totPct / float64(numPct)
	}
	return avgPct
}

// ------------------------------------------------

// The progressEntries is a map of...
// pindex -> sourcePartition -> node -> *ProgressEntry.
//
// The updateProgress() implementation must not block, in order to not
// block the invoking rebalancer.
func (m *CtlMgr) updateProgress(
	taskId string,
	seenNodes map[string]bool,
	seenPIndexes map[string]bool,
	progressEntries map[string]map[string]map[string]*rebalance.ProgressEntry,
	errs []error,
) {
	var progress float64
	if progressEntries != nil {
		pindexProg := map[string]float64{}
		for _, sourcePartitions := range progressEntries {
			for _, nodes := range sourcePartitions {
				for _, pex := range nodes {
					if pex == nil {
						continue
					}
					curProg := m.computeProgPercent(pex, sourcePartitions)
					if curProg > 0 || pex.TransferProgress > 0 {
						var t float64
						if pex.TransferProgress > 0 {
							t = .8 * pex.TransferProgress
							if curProg > 0 {
								t += .2 * curProg
							}
						} else {
							t = curProg
						}
						if v, ok := pindexProg[pex.PIndex]; !ok || v < t {
							pindexProg[pex.PIndex] = t
						}
					}
				}
			}
		}
		totPct := 0.0
		for _, prog := range pindexProg {
			if prog > 0 {
				totPct += prog
			}
		}

		partitionsCnt := m.ctl.movingPartitionsCount
		if partitionsCnt > 0 {
			progress = totPct / float64(partitionsCnt)
		} else if len(pindexProg) > 0 {
			// this may happen only at the very start or after all
			// movement is done, hence worked fine so far.
			progress = totPct / float64(len(pindexProg))
		}
	}

	taskProgressVal := taskProgress{
		taskId:         taskId,
		errs:           errs,
		progressExists: progressEntries != nil,
		progress:       progress,
	}

	select {
	case m.taskProgressCh <- taskProgressVal:
		// NO-OP.
	default:
		// NO-OP, if the handleTaskProgress() goroutine is behind,
		// drop notifications rather than hold up the rebalancer.
	}
}

func (m *CtlMgr) handleTaskProgress(taskProgress taskProgress) {
	m.mu.Lock()
	defer m.mu.Unlock()

	updated := false

	var taskHandlesNext []*taskHandle

	for _, th := range m.tasks.taskHandles {
		if th.task.ID == taskProgress.taskId {
			if taskProgress.progressExists || len(taskProgress.errs) > 0 {
				revNum := m.allocRevNumLOCKED(0)

				taskNext := *th.task // Copy.
				taskNext.Rev = EncodeRev(revNum)
				taskNext.Progress = taskProgress.progress

				log.Printf("ctl/manager: revNum: %d, progress: %f",
					revNum, taskProgress.progress)

				// TODO: DetailedProgress.

				taskNext.ErrorMessage = ""
				for _, err := range taskProgress.errs {
					if len(taskNext.ErrorMessage) > 0 {
						taskNext.ErrorMessage = taskNext.ErrorMessage + "\n"
					}
					taskNext.ErrorMessage = taskNext.ErrorMessage + err.Error()
				}

				if len(taskProgress.errs) > 0 {
					taskNext.Status = service.TaskStatusFailed
				}

				taskHandlesNext = append(taskHandlesNext, &taskHandle{
					startTime: th.startTime,
					task:      &taskNext,
					stop:      th.stop,
				})
			}

			updated = true
		} else {
			taskHandlesNext = append(taskHandlesNext, th)
		}
	}

	if !updated {
		return
	}

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})
}

// parsePIndexName returns the "indexName_indexUUID", given an input
// pindexName that has a format that looks like
// "indexName_indexUUID_pindexSpecificSuffix", where the indexName can
// also have more underscores.
func parsePIndexName(pindexName string) string {
	uscoreLast := strings.LastIndex(pindexName, "_")
	if uscoreLast >= 0 {
		return pindexName[0:uscoreLast]
	}
	return ""
}

// ------------------------------------------------

func (m *CtlMgr) getTaskListLOCKED() *service.TaskList {
	rv := &service.TaskList{
		Rev:   EncodeRev(m.tasks.revNum),
		Tasks: []service.Task{},
	}

	for _, taskHandle := range m.tasks.taskHandles {
		rv.Tasks = append(rv.Tasks, *taskHandle.task)
	}

	return rv
}

// ------------------------------------------------

func (m *CtlMgr) updateTasksLOCKED(body func(tasks *tasks)) {
	body(&m.tasks)

	m.tasks.revNum = m.allocRevNumLOCKED(m.tasks.revNum)

	if m.tasksWaitCh != nil {
		close(m.tasksWaitCh)
		m.tasksWaitCh = nil
	}
}

// ------------------------------------------------

func (m *CtlMgr) allocRevNumLOCKED(prevRevNum uint64) uint64 {
	rv := prevRevNum + 1
	if rv < m.revNumNext {
		rv = m.revNumNext
	}
	m.revNumNext = rv + 1
	return rv
}

// ------------------------------------------------

func EncodeRev(revNum uint64) service.Revision {
	return []byte(fmt.Sprintf("%d", revNum))
}

func DecodeRev(b service.Revision) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}
