//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	log "github.com/couchbase/clog"
)

// event_code block assigned to FTS events
// ranges from 3072 - 4095
const (
	ServiceStartEventID   uint32 = iota + 3072
	IndexCreateEventID           //3073
	IndexUpdateEventID           //3074
	IndexDeleteEventID           //3075
	SettingsUpdateEventID        //3076

	// the last permissible ID for an FTS system event; any additional
	// type of event shouldn't exceed the below number
	CrashEventID uint32 = 4095
)

type systemEvent struct {
	EventID         uint32      `json:"event_id"`
	UUID            string      `json:"uuid"`
	Timestamp       string      `json:"timestamp"`
	Component       string      `json:"component"`
	Severity        string      `json:"severity"`
	Description     string      `json:"description"`
	SubComponent    string      `json:"sub_component,omitempty"`
	Node            string      `json:"node,omitempty"`
	ExtraAttributes interface{} `json:"extra_attributes,omitempty"`
}

type systemEventManager struct {
	restEndPoint string
	eventCh      chan *systemEvent
	component    string
	procName     string
	procID       int
}

var sysEventMgr *systemEventManager

func initSystemEventManager(url, component, procName string, procID int) {
	sysEventMgr = &systemEventManager{
		eventCh:      make(chan *systemEvent, 10),
		restEndPoint: url,
		component:    component,
		procName:     procName,
		procID:       procID,
	}
}

func NewSystemEvent(eventCode uint32, severity string,
	description string, extraAttributes map[string]interface{}) *systemEvent {
	if sysEventMgr == nil {
		return nil
	}
	return &systemEvent{
		EventID:         eventCode,
		UUID:            NewUUIDV4(),
		Timestamp:       time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Component:       sysEventMgr.component,
		Severity:        severity,
		Description:     description,
		ExtraAttributes: extraAttributes,
	}
}

func postEvent(sysEv []byte, url string) (*http.Response, error) {
	u, err := CBAuthURL(url)
	if err != nil {
		return nil, fmt.Errorf("system_event: auth for ns_server,"+
			" server: %s, authType: %s, err: %v",
			url, "cbauth", err)
	}

	req, err := http.NewRequest("POST", u, bytes.NewReader(sysEv))
	if err != nil {
		return nil, fmt.Errorf("system_event: error in creating POST "+
			"request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")
	client := HttpClient()
	if client == nil {
		return nil, fmt.Errorf("system_event: HttpClient unavailable")
	}
	return client.Do(req)
}

func publishSystemEventWithWait(ev *systemEvent) {
	select {
	case sysEventMgr.eventCh <- ev:
	case <-time.After(time.Minute):
		log.Warnf("system_event: system event has been dropped "+
			"due to timeout limit being reached ev: %v", ev)
	}
}

func PublishSystemEvent(ev *systemEvent) error {
	if sysEventMgr != nil {
		if ev == nil {
			return fmt.Errorf("system_event: event provided should be non nil")
		}
		select {
		case sysEventMgr.eventCh <- ev:
		case <-time.After(1 * time.Second):
			go publishSystemEventWithWait(ev)
		}
	}
	return nil
}

func processSystemEvent(retryAfter int, evBytes []byte) {
	if retryAfter > 0 {
		time.Sleep(time.Duration(retryAfter) * time.Second)
	}

	resp, err := postEvent(evBytes, sysEventMgr.restEndPoint)
	defer resp.Body.Close()
	if resp.StatusCode == 503 {
		retryAfter, err = strconv.Atoi(resp.Header.Get("Retry-after"))
		if err != nil {
			log.Warnf("system_event: incorrect value for Retry-after")
			return
		}
		go processSystemEvent(retryAfter, evBytes)
		return
	}
	if resp.StatusCode != http.StatusOK {
		respBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("system_event: error while reading "+
				"the response object err: %v", err)
			return
		}
		respString := string(respBytes)
		log.Warnf("system_event: event request unsuccessful,"+
			" err: %v", respString)
	}

	if err != nil {
		log.Errorf("system_event: error while POST'ing the event to ns_server,"+
			" err: %v", err)
	}
}

func PublishCrashEvent(extra_attrs interface{}) {
	if sysEventMgr != nil {
		evBytes, err := json.Marshal(NewSystemEvent(
			CrashEventID,
			"fatal",
			"Crash",
			map[string]interface{}{
				"processID":   sysEventMgr.procID,
				"processName": sysEventMgr.procName,
				"details":     extra_attrs,
			}))
		if err != nil {
			log.Errorf("system_event: json marshal error "+
				"err: %v", err)
			return
		}

		processSystemEvent(0, evBytes)
	}
}

func StartSystemEventListener(server, component, procName string, procID int) error {
	initSystemEventManager(server+"/_event", component, procName, procID)
	go func() {
		for {
			sysEv, more := <-sysEventMgr.eventCh
			if more {
				evBytes, err := json.Marshal(sysEv)
				if err != nil {
					log.Errorf("system_event: json marshal error %v", err)
					continue
				}
				processSystemEvent(0, evBytes)
			}
		}
	}()
	return nil
}
