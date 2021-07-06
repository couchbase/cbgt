//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"io"
)

func init() {
	RegisterFeedType("nil", &FeedType{
		Start: func(mgr *Manager, feedName, indexName, indexUUID,
			sourceType, sourceName, sourceUUID, params string,
			dests map[string]Dest) error {
			return mgr.registerFeed(NewNILFeed(feedName, indexName, dests))
		},
		Partitions: func(sourceType, sourceName, sourceUUID, sourceParams,
			server string, options map[string]string) ([]string, error) {
			return nil, nil
		},
		Public: true,
		Description: "advanced/nil" +
			" - a nil data source has no data;" +
			" used for index aliases and testing",
	})
}

// A NILFeed implements the Feed interface and never feeds any data to
// its Dest instances.  It's useful for testing and for pindexes that are
// actually primary data sources.
//
// See also the "blackhole" pindex type for the "opposite equivalent"
// of a NILFeed.
type NILFeed struct {
	name      string
	indexName string
	dests     map[string]Dest
}

// NewNILFeed creates a ready-to-be-started NILFeed instance.
func NewNILFeed(name, indexName string, dests map[string]Dest) *NILFeed {
	return &NILFeed{name: name, indexName: indexName, dests: dests}
}

func (t *NILFeed) Name() string {
	return t.name
}

func (t *NILFeed) IndexName() string {
	return t.indexName
}

func (t *NILFeed) Start() error {
	return nil
}

func (t *NILFeed) Close() error {
	return nil
}

func (t *NILFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *NILFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}
