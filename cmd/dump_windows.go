// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build windows || !darwin || !freebsd || !linux || !openbsd || !netbsd
// +build windows !darwin !freebsd !linux !openbsd !netbsd

package cmd

import (
	log "github.com/couchbase/clog"
)

func DumpOnSignalForPlatform() {
	// No-op for windows.
}

func init() {
	log.DisableColor()
}
