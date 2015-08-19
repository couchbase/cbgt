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

package autofailover

import (
	"log"
	"testing"
	"time"
)

////////////////////////////////////////////////////////////////////////

type fakeHeartbeater struct {
	heartbeatSendCloser  chan bool          // Close channel used to break out of heartbeat sender goroutine
	heartbeatCheckCloser chan bool          // Close channel used to break out of heartbeat checker goroutine
	heartbeatTarget      chan<- interface{} // Where to send the heartbeats
}

func newFakeHeartbeater(heartbeatTarget chan<- interface{}) Heartbeater {
	heartBeater := &fakeHeartbeater{
		heartbeatSendCloser:  make(chan bool),
		heartbeatCheckCloser: make(chan bool),
		heartbeatTarget:      heartbeatTarget,
	}
	return heartBeater
}

func (h *fakeHeartbeater) StartSendingHeartbeats(intervalMs int) error {

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)

	go func() {
		for {
			select {
			case _ = <-h.heartbeatSendCloser:
				ticker.Stop()
				return
			case <-ticker.C:
				h.heartbeatTarget <- true
			}
		}
	}()
	return nil

}

func (h *fakeHeartbeater) StopSendingHeartbeats() {
	close(h.heartbeatSendCloser)
}

func (h *fakeHeartbeater) StopCheckingHeartbeats() {
	close(h.heartbeatCheckCloser)
}

func (h *fakeHeartbeater) StartCheckingHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error {

	// since this is just a fake implementation used in a test, just have it
	// pretend to find stale heartbeats after 5 millisconds

	ticker := time.NewTicker(5 * time.Millisecond)

	go func() {
		for {
			select {
			case _ = <-h.heartbeatCheckCloser:
				ticker.Stop()
				return
			case <-ticker.C:
				handler.StaleHeartBeatDetected("fake_uuid")
				return
			}
		}
	}()

	return nil

}

////////////////////////////////////////////////////////////////////////

type fakeHeartbeatStoppedHandler struct{}

func (f fakeHeartbeatStoppedHandler) StaleHeartBeatDetected(nodeUuid string) {
	log.Printf("Stale heartbeat detected, uuid: %v", nodeUuid)
}

func TestCouchbaseHeartbeater(t *testing.T) {

	managerNodeUuid := "123"
	cbHearbeater, err := NewCouchbaseHeartbeater(
		"http://127.0.0.1:8091",
		"default",
		"_sync:",
		managerNodeUuid,
	)
	log.Printf("cbHeartbeater: %v", cbHearbeater)

	if err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}

	intervalMs := 2000
	cbHearbeater.StartSendingHeartbeats(intervalMs)

	<-time.After(1 * time.Second)

	cbHearbeater.StopSendingHeartbeats()

	staleThresholdMs := 1000
	if err := cbHearbeater.StartCheckingHeartbeats(staleThresholdMs, fakeHeartbeatStoppedHandler{}); err != nil {
		t.Logf("Error: %v", err)
		t.Fail()
	}

	<-time.After(5 * time.Second)
	cbHearbeater.StopCheckingHeartbeats()

}
