//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import "time"

// Cfg is the interface that configuration providers must implement.
type Cfg interface {
	// Get retrieves an entry from the Cfg.  A zero cas means don't do
	// a CAS match on Get(), and a non-zero cas value means the Get()
	// will succeed only if the CAS matches.
	Get(key string, cas uint64) (val []byte, casSuccess uint64, err error)

	// Set creates or updates an entry in the Cfg.  A non-zero cas
	// that does not match will result in an error.  A zero cas means
	// the Set() operation must be an entry creation, where a zero cas
	// Set() will error if the entry already exists.
	Set(key string, val []byte, cas uint64) (casSuccess uint64, err error)

	// Del removes an entry from the Cfg.  A non-zero cas that does
	// not match will result in an error.  A zero cas means a CAS
	// match will be skipped, so that clients can perform a
	// "don't-care, out-of-the-blue" deletion.
	Del(key string, cas uint64) error

	// Subscribe allows clients to receive events on changes to a key.
	// During a deletion event, the CfgEvent.CAS field will be 0.
	Subscribe(key string, ch chan CfgEvent) error

	// Refresh forces the Cfg implementation to reload from its
	// backend-specific data source, clearing any locally cached data.
	// Any subscribers will receive events on a Refresh, where it's up
	// to subscribers to detect if there were actual changes or not.
	Refresh() error
}

// The error used on mismatches of CAS (compare and set/swap) values.
type CfgCASError struct{}

func (e *CfgCASError) Error() string { return "CAS mismatch" }

// See the Cfg.Subscribe() method.
type CfgEvent struct {
	Time  time.Time
	Key   string
	CAS   uint64
	Error error
}
