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
	"fmt"
	"log"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/util"
)

const (
	DocTypeHeartbeat        = "heartbeat"
	DocTypeHeartbeatTimeout = "heartbeat_timeout"
)

type Heartbeater interface {
	HeartbeatChecker
	HeartbeatSender
}

type HeartbeatsStoppedHandler interface {
	StaleHeartBeatDetected(deadManagerUuid string)
}

type HeartbeatChecker interface {
	StartCheckingHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error
	StopCheckingHeartbeats()
}

type HeartbeatSender interface {
	StartSendingHeartbeats(intervalMs int) error
	StopSendingHeartbeats()
}

type couchbaseHeartBeater struct {
	bucket               *couchbase.Bucket
	couchbaseUrlStr      string
	bucketName           string
	managerNodeUuid      string
	keyPrefix            string
	heartbeatSendCloser  chan struct{} // break out of heartbeat sender goroutine
	heartbeatCheckCloser chan struct{} // break out of heartbeat checker goroutine

}

type HeartbeatMeta struct {
	Type            string `json:"type"`
	ManagerNodeUUID string `json:"manager_node_uuid"`
}

type HeartbeatTimeout struct {
	Type            string `json:"type"`
	ManagerNodeUUID string `json:"manager_node_uuid"`
}

func NewCouchbaseHeartbeater(couchbaseUrl, bucketName, keyPrefix, managerNodeUuid string) (Heartbeater, error) {

	heartbeater := &couchbaseHeartBeater{
		couchbaseUrlStr:      couchbaseUrl,
		bucketName:           bucketName,
		managerNodeUuid:      managerNodeUuid,
		keyPrefix:            keyPrefix,
		heartbeatSendCloser:  make(chan struct{}),
		heartbeatCheckCloser: make(chan struct{}),
	}

	// get bucket or else return error
	_, err := heartbeater.getBucket()
	if err != nil {
		return nil, err
	}
	log.Printf("bucket: %v", heartbeater.bucket)
	return heartbeater, nil

}

func (h *couchbaseHeartBeater) StartSendingHeartbeats(intervalMs int) error {

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)

	go func() {
		for {
			select {
			case _ = <-h.heartbeatSendCloser:
				log.Printf("Heartbeat send goroutine finished")
				ticker.Stop()
				return
			case <-ticker.C:
				if err := h.SendHeartbeat(intervalMs); err != nil {
					log.Printf("Error sending heartbeat: %v", err)
				}
			}
		}
	}()
	return nil

}

func (h *couchbaseHeartBeater) StopSendingHeartbeats() {
	close(h.heartbeatSendCloser)
}

func (h *couchbaseHeartBeater) StartCheckingHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error {

	if err := h.AddHeartbeatCheckView(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(staleThresholdMs) * time.Millisecond)

	go func() {
		for {
			select {
			case _ = <-h.heartbeatCheckCloser:
				log.Printf("Heartbeat check goroutine finished")
				ticker.Stop()
				return
			case <-ticker.C:
				if err := h.CheckStaleHeartbeats(staleThresholdMs, handler); err != nil {
					log.Printf("Error checking for stale heartbeats: %v", err)
				}
			}
		}
	}()
	return nil

}

func (h couchbaseHeartBeater) CheckStaleHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error {

	// query view to get all heartbeat docs
	heartbeatDocs, err := h.viewQueryHeartbeatDocs()
	if err != nil {
		return err
	}

	for _, heartbeatDoc := range heartbeatDocs {
		if heartbeatDoc.ManagerNodeUUID == h.managerNodeUuid {
			// that's us, and we don't care about ourselves
			continue
		}
		if heartbeatDoc.ManagerNodeUUID == "" {
			log.Printf("autofailover: skipping invalid heartbeatDoc: %+v", heartbeatDoc)
			continue
		}
		timeoutDocId := h.heartbeatTimeoutDocId(heartbeatDoc.ManagerNodeUUID)
		heartbeatTimeoutDoc := HeartbeatTimeout{}
		err := h.bucket.Get(timeoutDocId, &heartbeatTimeoutDoc)
		if err != nil {
			if !couchbase.IsKeyNoEntError(err) {
				// unexpected error
				return err
			}

			// doc not found, which means the heartbeat doc expired.
			// call back the handler.
			handler.StaleHeartBeatDetected(heartbeatDoc.ManagerNodeUUID)

			// delete the heartbeat doc itself so we don't have unwanted
			// repeated callbacks to the stale heartbeat handler
			docId := h.heartbeatDocId(heartbeatDoc.ManagerNodeUUID)
			if err := h.bucket.Delete(docId); err != nil {
				log.Printf("autofailover: warning - failed to delete heartbeat doc: %v err: %v", docId, err)
			}

		}

	}
	return nil
}

func (h couchbaseHeartBeater) heartbeatTimeoutDocId(managerNodeUuid string) string {
	return fmt.Sprintf("%vheartbeat_timeout:%v", h.keyPrefix, managerNodeUuid)
}

func (h couchbaseHeartBeater) heartbeatDocId(managerNodeUuid string) string {
	return fmt.Sprintf("%vheartbeat:%v", h.keyPrefix, managerNodeUuid)
}

func (h couchbaseHeartBeater) viewQueryHeartbeatDocs() ([]HeartbeatMeta, error) {

	viewRes := struct {
		Rows []struct {
			Id    string
			Value string
		}
		Errors []couchbase.ViewError
	}{}

	err := h.bucket.ViewCustom("cbgt", "heartbeats",
		map[string]interface{}{
			"stale": false,
		}, &viewRes)
	if err != nil {
		return nil, err
	}

	heartbeats := []HeartbeatMeta{}
	for _, row := range viewRes.Rows {
		heartbeat := HeartbeatMeta{
			Type:            DocTypeHeartbeat,
			ManagerNodeUUID: row.Value,
		}
		heartbeats = append(heartbeats, heartbeat)
	}

	return heartbeats, nil

}

func (h *couchbaseHeartBeater) StopCheckingHeartbeats() {
	close(h.heartbeatCheckCloser)
}

func (h couchbaseHeartBeater) SendHeartbeat(intervalMs int) error {

	if err := h.upsertHeartbeatDoc(); err != nil {
		return err
	}
	if err := h.upsertHeartbeatTimeoutDoc(intervalMs); err != nil {
		return err
	}
	return nil
}

func (h couchbaseHeartBeater) upsertHeartbeatDoc() error {

	heartbeatDoc := HeartbeatMeta{
		Type:            DocTypeHeartbeat,
		ManagerNodeUUID: h.managerNodeUuid,
	}
	docId := h.heartbeatDocId(h.managerNodeUuid)

	if err := h.bucket.Set(docId, 0, heartbeatDoc); err != nil {
		return err
	}
	return nil

}

func (h couchbaseHeartBeater) upsertHeartbeatTimeoutDoc(intervalMs int) error {

	heartbeatTimeoutDoc := HeartbeatTimeout{
		Type:            DocTypeHeartbeatTimeout,
		ManagerNodeUUID: h.managerNodeUuid,
	}

	docId := h.heartbeatTimeoutDocId(h.managerNodeUuid)

	expireTimeSeconds := (intervalMs / 1000)

	// make the expire time double the interval time, to ensure there is
	// always a heartbeat timeout document present under normal operation
	expireTimeSeconds *= 2

	if err := h.bucket.Set(docId, expireTimeSeconds, heartbeatTimeoutDoc); err != nil {
		return err
	}
	return nil

}

func (h *couchbaseHeartBeater) getBucket() (*couchbase.Bucket, error) {
	if h.bucket == nil {
		bucket, err := couchbase.GetBucket(h.couchbaseUrlStr, "default", h.bucketName)
		if err != nil {
			return nil, err
		}
		h.bucket = bucket
	}
	return h.bucket, nil
}

func (h couchbaseHeartBeater) AddHeartbeatCheckView() error {

	ddocVersionKey := fmt.Sprintf("%vddocVersion", h.keyPrefix)
	ddocVersion := 1
	designDoc := `
	   {
	       "views": {
	           "heartbeats": {
	               "map": "function (doc, meta) { if (doc.type == 'heartbeat') { emit(meta.id, doc.manager_node_uuid); }}"
	           }
	       }
	   }`

	return couchbaseutil.UpdateView(
		h.bucket,
		"cbgt",
		ddocVersionKey,
		designDoc,
		ddocVersion,
	)

}
