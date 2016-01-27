// @author Couchbase <info@couchbase.com>
// @copyright 2015 Couchbase, Inc.
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
package service_api

import (
	"errors"
)

var (
	ErrNotFound           = errors.New("not_found")
	ErrConflict           = errors.New("conflict")
	ErrNotSupported       = errors.New("operation_not_supported")
	ErrCanceled           = errors.New("operation_canceled")
	ErrRecoveryImpossible = errors.New("recovery_impossible")
)

type Revision []byte
type Priority int64
type NodeId string

type NodeInfo struct {
	NodeId   NodeId      `json:"nodeId"`
	Priority Priority    `json:"priority"`
	Opaque   interface{} `json:"opaque"`
}

type Topology struct {
	Rev   Revision `json:"rev"`
	Nodes []NodeId `json:"nodes"`

	IsBalanced bool     `json:"isBalanced"`
	Messages   []string `json:"messages,omitempty"`
}

type TaskType string

const (
	TaskTypeRebalance = TaskType("task-rebalance")
	TaskTypePrepared  = TaskType("task-prepared")
)

type TaskStatus string

const (
	TaskStatusRunning = TaskStatus("task-running")
	TaskStatusFailed  = TaskStatus("task-failed")
)

type Task struct {
	Rev Revision `json:"rev"`

	Id           string     `json:"id"`
	Type         TaskType   `json:"type"`
	Status       TaskStatus `json:"status"`
	IsCancelable bool       `json:"isCancelable"`

	Progress         float64            `json:"progress"`
	DetailedProgress map[NodeId]float64 `json:"detailedProgress,omitempty"`

	Description  string `json:"description,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`

	Extra map[string]interface{} `json:"extra"`
}

type TaskList struct {
	Rev   Revision `json:"rev"`
	Tasks []Task   `json:"tasks"`
}

type Cancel <-chan struct{}

type RecoveryType string

const (
	RecoveryTypeNone  = RecoveryType("recovery-none")
	RecoveryTypeFull  = RecoveryType("recovery-full")
	RecoveryTypeDelta = RecoveryType("recovery-delta")
)

type TopologyChange struct {
	Id                 string   `json:"id"`
	CurrentTopologyRev Revision `json:"currentTopologyRev"`

	Failover bool `json:"failover"`

	Nodes []struct {
		NodeInfo     NodeInfo     `json:"nodeInfo"`
		RecoveryType RecoveryType `json:"recoveryType"`
	} `json:"nodes"`
}

type ServiceManager interface {
	GetNodeInfo() (*NodeInfo, error)
	Shutdown() error

	GetTaskList(rev Revision, cancel Cancel) (*TaskList, error)
	CancelTask(Id string, rev Revision) error

	GetCurrentTopology(rev Revision, cancel Cancel) (*Topology, error)

	PrepareTopologyChange(change TopologyChange) error
	StartTopologyChange(change TopologyChange) error
}
