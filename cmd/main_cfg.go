//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/couchbase/cbgt"
)

var ErrorBindHttp = errors.New("main_cfg:" +
	" couchbase cfg needs network/IP address for bindHttp,\n" +
	"  Please specify a network/IP address for the -bindHttp parameter\n" +
	"  (non-loopback, non-127.0.0.1/localhost, non-0.0.0.0)\n" +
	"  so that this node can be clustered with other nodes.")

// MainCfg connects to a Cfg provider as a server peer (e.g., as a
// cbgt.Manager).
func MainCfg(baseName, connect, bindHttp,
	register, dataDir string) (cbgt.Cfg, error) {
	return MainCfgEx(baseName, connect, bindHttp, register, dataDir, "", nil)
}

// MainCfgEx connects to a Cfg provider as a server peer (e.g., as a
// cbgt.Manager), with more options.
func MainCfgEx(baseName, connect, bindHttp,
	register, dataDir, uuid string, options map[string]string) (cbgt.Cfg, error) {
	// TODO: One day, the default cfg provider should not be simple.
	// TODO: One day, Cfg provider lookup should be table driven.
	var cfg cbgt.Cfg
	var err error
	switch {
	case connect == "", connect == "simple":
		cfg, err = MainCfgSimple(baseName, connect, bindHttp, register, dataDir)
	case strings.HasPrefix(connect, "couchbase:"):
		cfg, err = MainCfgCB(baseName, connect[len("couchbase:"):],
			bindHttp, register, dataDir)
	case strings.HasPrefix(connect, "metakv"):
		cfg, err = MainCfgMetaKv(baseName, connect[len("metakv"):],
			bindHttp, register, dataDir, uuid, options)
	default:
		err = fmt.Errorf("main_cfg1: unsupported cfg connect: %s", connect)
	}
	return cfg, err
}

// ------------------------------------------------

func MainCfgSimple(baseName, connect, bindHttp, register, dataDir string) (
	cbgt.Cfg, error) {
	cfgPath := dataDir + string(os.PathSeparator) + baseName + ".cfg"
	cfgPathExists := false
	if _, err := os.Stat(cfgPath); err == nil {
		cfgPathExists = true
	}

	cfg := cbgt.NewCfgSimple(cfgPath)
	if cfgPathExists {
		err := cfg.Load()
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func MainCfgCB(baseName, urlStr, bindHttp, register, dataDir string) (
	cbgt.Cfg, error) {
	if (bindHttp[0] == ':' ||
		strings.HasPrefix(bindHttp, "0.0.0.0:") ||
		strings.HasPrefix(bindHttp, "127.0.0.1:") ||
		strings.HasPrefix(bindHttp, "localhost:")) &&
		register != "unwanted" &&
		register != "unknown" {
		return nil, ErrorBindHttp
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	bucket := "default"
	if u.User != nil && u.User.Username() != "" {
		bucket = u.User.Username()
	}

	cfg, err := cbgt.NewCfgCB(urlStr, bucket)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func MainCfgMetaKv(baseName, urlStr, bindHttp, register, dataDir, uuid string,
	options map[string]string) (
	cbgt.Cfg, error) {
	cfg, err := cbgt.NewCfgMetaKv(uuid, options)
	if err == nil {
		// Useful for reseting internal testing.
		if urlStr == ":removeAllKeys" {
			cfg.RemoveAllKeys()
		}
		err = cfg.Load()
	}
	return cfg, err
}

// ------------------------------------------------

// MainCfgClient helper function connects to a Cfg provider as a
// client (not as a known cbgt.Manager server or peer).  This is
// useful, for example, for tool developers as opposed to server
// developers.
func MainCfgClient(baseName, cfgConnect string) (cbgt.Cfg, error) {
	bindHttp := "<NO-BIND-HTTP>"
	register := "unchanged"
	dataDir := "<NO-DATA-DIR>"

	cfg, err := MainCfg(baseName, cfgConnect,
		bindHttp, register, dataDir)
	if err == ErrorBindHttp {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("main: could not start cfg,"+
			" cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%q)\n"+
			"  is correct and/or that your configuration provider\n"+
			"  is available.",
			cfgConnect, err, cfgConnect)
	}

	return cfg, err
}
