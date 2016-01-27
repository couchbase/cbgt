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
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/cbgt/rebalance"

	"github.com/couchbase/cbgt/ctl/service_api"
)

// CtlMgr implements the service_api.ServiceManager interface and
// provides the adapter or glue between ns-server's service_api API
// and cbgt's Ctl implementation.
type CtlMgr struct {
	nodeInfo *service_api.NodeInfo

	ctl *Ctl

	mu sync.Mutex

	revNumNext uint64 // The next rev num to use.

	tasks       tasks
	tasksWaitCh chan struct{} // Closed when the tasks change.
}

type tasks struct {
	revNum      uint64
	taskHandles []*taskHandle
}

type taskHandle struct { // The taskHandle fields are immutable.
	startTime time.Time
	task      *service_api.Task
	stop      func() // May be nil.
}

// ------------------------------------------------

func NewCtlMgr(nodeInfo *service_api.NodeInfo, ctl *Ctl) *CtlMgr {
	return &CtlMgr{
		nodeInfo:   nodeInfo,
		ctl:        ctl,
		revNumNext: 1,
		tasks:      tasks{revNum: 0},
	}
}

func (m *CtlMgr) GetNodeInfo() (*service_api.NodeInfo, error) {
	return m.nodeInfo, nil
}

func (m *CtlMgr) Shutdown() error {
	os.Exit(0)
	return nil
}

func (m *CtlMgr) GetTaskList(haveTasksRev service_api.Revision,
	cancelCh service_api.Cancel) (*service_api.TaskList, error) {
	m.mu.Lock()

	if len(haveTasksRev) > 0 {
		haveTasksRevNum, err := DecodeRev(haveTasksRev)
		if err != nil {
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
				return nil, ErrCanceled
			case <-tasksWaitCh:
				// FALLTHRU
			}
			m.mu.Lock()
		}
	}

	rv := m.getTaskListLOCKED()

	m.mu.Unlock()

	return rv, nil
}

func (m *CtlMgr) CancelTask(
	taskId string, taskRev service_api.Revision) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	canceled := false

	taskHandlesNext := []*taskHandle(nil)

	for _, taskHandle := range m.tasks.taskHandles {
		task := taskHandle.task
		if task.Id == taskId {
			if taskRev != nil && !bytes.Equal(taskRev, task.Rev) {
				return service_api.ErrConflict
			}

			if !task.IsCancelable {
				return service_api.ErrNotSupported
			}

			if taskHandle.stop != nil {
				taskHandle.stop()
			}

			canceled = true
		} else {
			taskHandlesNext = append(taskHandlesNext, taskHandle)
		}
	}

	if !canceled {
		return service_api.ErrNotFound
	}

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})

	return nil
}

func (m *CtlMgr) GetCurrentTopology(haveTopologyRev service_api.Revision,
	cancelCh service_api.Cancel) (*service_api.Topology, error) {
	ctlTopology, err :=
		m.ctl.WaitGetTopology(string(haveTopologyRev), cancelCh)
	if err != nil {
		return nil, err
	}

	rv := &service_api.Topology{
		Rev: service_api.Revision([]byte(ctlTopology.Rev)),
	}

	for _, ctlNode := range ctlTopology.MemberNodes {
		rv.Nodes = append(rv.Nodes, service_api.NodeId(ctlNode.UUID))
	}

	// TODO: Need a proper IsBalanced computation.
	rv.IsBalanced =
		len(ctlTopology.PrevWarnings) <= 0 && len(ctlTopology.PrevErrs) <= 0

	for resourceName, resourceWarnings := range ctlTopology.PrevWarnings {
		for _, resourceWarning := range resourceWarnings {
			rv.Messages = append(rv.Messages,
				fmt.Sprintf("warning: resource: %q -- %s",
					resourceName, resourceWarning))
		}
	}

	for err := range ctlTopology.PrevErrs {
		rv.Messages = append(rv.Messages, fmt.Sprintf("error: %v", err))
	}

	return rv, nil
}

func (m *CtlMgr) PrepareTopologyChange(
	change service_api.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if string(change.CurrentTopologyRev) != m.ctl.GetTopology().Rev {
		return service_api.ErrConflict
	}

	for _, taskHandle := range m.tasks.taskHandles {
		if taskHandle.task.Type == service_api.TaskTypePrepared ||
			taskHandle.task.Type == service_api.TaskTypeRebalance {
			// NOTE: If there's an existing rebalance or preparation
			// task, even if it's done, then treat as a conflict, as
			// the caller should cancel them all first.
			return service_api.ErrConflict
		}
	}

	revNum := m.allocRevNumLOCKED(0)

	taskHandlesNext := append([]*taskHandle(nil),
		m.tasks.taskHandles...)
	taskHandlesNext = append(taskHandlesNext,
		&taskHandle{
			startTime: time.Now(),
			task: &service_api.Task{
				Rev:              EncodeRev(revNum),
				Id:               change.Id,
				Type:             service_api.TaskTypePrepared,
				Status:           service_api.TaskStatusRunning,
				IsCancelable:     true,
				Progress:         100.0, // TODO.
				DetailedProgress: nil,
				Description:      "prepared topology change",
				ErrorMessage:     "",
				Extra: map[string]interface{}{
					"topologyChange": change,
				},
			},
		})

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})

	return nil
}

func (m *CtlMgr) StartTopologyChange(change service_api.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if string(change.CurrentTopologyRev) != m.ctl.GetTopology().Rev {
		return service_api.ErrConflict
	}

	var err error

	started := false

	var taskHandlesNext []*taskHandle

	for _, th := range m.tasks.taskHandles {
		if th.task.Type == service_api.TaskTypeRebalance {
			return service_api.ErrConflict
		}

		if th.task.Type == service_api.TaskTypePrepared {
			if th.task.Id != change.Id {
				return service_api.ErrConflict
			}

			th, err = m.startTopologyChangeTaskHandleLOCKED(change)
			if err != nil {
				return err
			}

			started = true
		}

		taskHandlesNext = append(taskHandlesNext, th)
	}

	if !started {
		return service_api.ErrNotFound
	}

	m.updateTasksLOCKED(func(s *tasks) {
		s.taskHandles = taskHandlesNext
	})

	return nil
}

func (m *CtlMgr) startTopologyChangeTaskHandleLOCKED(
	change service_api.TopologyChange) (*taskHandle, error) {
	ctlChangeTopology := &CtlChangeTopology{
		Rev:  string(change.CurrentTopologyRev),
		Mode: "rebalance",
	}

	if change.Failover { // TODO: what about failover-graceful?
		ctlChangeTopology.Mode = "failover-hard"
	}

	for _, node := range change.Nodes {
		// TODO: What about node.RecoveryType?

		nodeUUID := string(node.NodeInfo.NodeId)

		ctlChangeTopology.MemberNodeUUIDs =
			append(ctlChangeTopology.MemberNodeUUIDs, nodeUUID)
	}

	// The progressEntries is a map of pindex ->
	// source_partition -> node -> *rebalance.ProgressEntry.
	onProgress := func(maxNodeLen, maxPIndexLen int,
		seenNodes map[string]bool,
		seenNodesSorted []string,
		seenPIndexes map[string]bool,
		seenPIndexesSorted []string,
		progressEntries map[string]map[string]map[string]*rebalance.ProgressEntry,
	) string {
		m.updateProgress(change.Id, progressEntries)

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
		task: &service_api.Task{
			Rev:              EncodeRev(revNum),
			Id:               change.Id,
			Type:             service_api.TaskTypeRebalance,
			Status:           service_api.TaskStatusRunning,
			IsCancelable:     true,
			Progress:         0.0,
			DetailedProgress: map[service_api.NodeId]float64{},
			Description:      "topology change",
			ErrorMessage:     "",
			Extra: map[string]interface{}{
				"topologyChange": change,
			},
		},
		stop: func() {
			m.ctl.StopChangeTopology(ctlTopology.Rev)
		},
	}

	return th, nil
}

// ------------------------------------------------

func (m *CtlMgr) updateProgress(
	taskId string,
	p map[string]map[string]map[string]*rebalance.ProgressEntry) {
	var progress float64

	if p != nil {
		var totCurrDelta uint64
		var totWantDelta uint64

		for _, p1 := range p {
			for _, p2 := range p1 {
				for _, pe := range p2 {
					totCurrDelta += (pe.CurrUUIDSeq.Seq - pe.InitUUIDSeq.Seq)
					totWantDelta += (pe.WantUUIDSeq.Seq - pe.InitUUIDSeq.Seq)
				}
			}
		}

		if totWantDelta > 0 {
			progress = float64(totCurrDelta) / float64(totWantDelta)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	updated := false

	var taskHandlesNext []*taskHandle

	for _, th := range m.tasks.taskHandles {
		if th.task.Id == taskId {
			if p != nil {
				revNum := m.allocRevNumLOCKED(0)

				taskNext := *th.task // Copy.
				taskNext.Rev = EncodeRev(revNum)
				taskNext.Progress = progress

				// TODO: DetailedProgress.

				taskHandlesNext = append(taskHandlesNext, &taskHandle{
					startTime: th.startTime,
					task:      &taskNext,
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

// ------------------------------------------------

func (m *CtlMgr) getTaskListLOCKED() *service_api.TaskList {
	rv := &service_api.TaskList{
		Rev: EncodeRev(m.tasks.revNum),
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

func EncodeRev(revNum uint64) service_api.Revision {
	return []byte(fmt.Sprintf("%d", revNum))
}

func DecodeRev(b service_api.Revision) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}
