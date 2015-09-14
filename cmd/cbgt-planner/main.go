//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
	"github.com/couchbaselabs/cbgt/cmd"
)

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	if flags.Version {
		fmt.Printf("%s main: %s, data: %s\n",
			path.Base(os.Args[0]), cbgt.VERSION, cbgt.VERSION)
		os.Exit(0)
	}

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	log.Printf("main: %s started (%s/%s)",
		os.Args[0], cbgt.VERSION, cbgt.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go cmd.DumpOnSignalForPlatform()

	cmd.LogFlags(flagAliases)

	// ----------------------------------------------

	if flags.IndexTypes != "" {
		cmd.RegisterIndexTypes(strings.Split(flags.IndexTypes, ","))
	}

	// ----------------------------------------------

	bindHttp := "<NO-BIND-HTTP>"
	register := "unchanged"
	dataDir := "<NO-DATA-DIR>"

	cfg, err := cmd.MainCfg("mcp", flags.CfgConnect,
		bindHttp, register, dataDir)
	if err != nil {
		if err == cmd.ErrorBindHttp {
			log.Fatalf("%v", err)
			return
		}
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%q)\n"+
			"  is correct and/or that your configuration provider\n"+
			"  is available.",
			flags.CfgConnect, err, flags.CfgConnect)
		return
	}

	// ----------------------------------------------

	nodesToRemove := []string(nil)
	if len(flags.RemoveNodes) > 0 {
		nodesToRemove = strings.Split(flags.RemoveNodes, ",")
	}

	var steps map[string]bool
	if flags.Steps != "" {
		steps = cbgt.StringsToMap(strings.Split(flags.Steps, ","))
	}

	if steps == nil || steps["unregister"] {
		log.Printf("main: step unregister")

		err := unregisterNodes(cfg, nodesToRemove, flags.DryRun)
		if err != nil {
			log.Fatalf("main: unregisterNodes, err: %v", err)
			return
		}
	}

	if steps == nil || steps["planner"] {
		log.Printf("main: step planner")

		changed, err := cbgt.Plan(cfg, cbgt.VERSION, "", flags.Server)
		if err != nil {
			log.Fatalf("main: plan, changed: %v, err: %v", changed, err)
			return
		}
	}

	log.Printf("main: done")
}

// ------------------------------------------------------------

func unregisterNodes(cfg cbgt.Cfg, nodes []string, dryRun bool) error {
	for _, node := range nodes {
		log.Printf("main: unregistering, node: %s", node)

		if dryRun {
			continue
		}

		for _, kind := range []string{
			cbgt.NODE_DEFS_WANTED,
			cbgt.NODE_DEFS_KNOWN,
		} {
			err := cbgt.CfgRemoveNodeDef(cfg, kind, node, cbgt.VERSION)
			if err != nil {
				return fmt.Errorf("unregistering,"+
					" node: %s, kind: %s, err: %v",
					node, kind, err)
			}
		}
	}

	return nil
}
