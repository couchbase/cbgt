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
	"os"
	"path"
	"strings"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
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

	cmd.MainCommon(cbgt.VERSION, flagAliases)

	cfg, err := cmd.MainCfgClient(path.Base(os.Args[0]), flags.CfgConnect)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}

	if flags.IndexTypes != "" {
		cmd.RegisterIndexTypes(strings.Split(flags.IndexTypes, ","))
	}

	nodesToRemove := []string(nil)
	if len(flags.RemoveNodes) > 0 {
		nodesToRemove = strings.Split(flags.RemoveNodes, ",")
	}

	var steps map[string]bool
	if flags.Steps != "" {
		steps = cbgt.StringsToMap(strings.Split(flags.Steps, ","))
	}

	err = cmd.PlannerSteps(steps, cfg, cbgt.VERSION,
		flags.Server, nodesToRemove, flags.DryRun)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}

	log.Printf("main: done")
}
