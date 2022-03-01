cbgt
====

The cbgt project provides a golang library that helps manage
distributed partitions (or data shards) across an elastic cluster of
servers.

[![GoDoc](https://godoc.org/github.com/couchbase/cbgt?status.svg)](https://godoc.org/github.com/couchbase/cbgt) [![Coverage Status](https://coveralls.io/repos/couchbase/cbgt/badge.svg?branch=master&service=github)](https://coveralls.io/github/couchbase/cbgt?branch=master)

#### Documentation

* [REST API Reference](http://labs.couchbase.com/cbft/api-ref/) -
  these REST API Reference docs come from cbft, which uses the cbgt
  library.

NOTE: This library initializes math's random seed
(rand.Seed(time.Now().UTC().UnixNano())) for unique id generation.
