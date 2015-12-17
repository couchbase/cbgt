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
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/gorilla/mux"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/ctl"
	"github.com/couchbase/cbgt/rebalance"
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

	steps := map[string]bool{}
	if flags.Steps != "" {
		steps = cbgt.StringsToMap(strings.Split(flags.Steps, ","))
	}

	// ------------------------------------------------

	if steps != nil && steps["rebalance"] {
		steps["rebalance_"] = true
		steps["unregister"] = true
		steps["planner"] = true
	}

	// ------------------------------------------------

	if steps != nil && steps["rebalance_"] {
		log.Printf("main: step rebalance_")

		err := rebalance.RunRebalance(cfg, flags.Server, nodesToRemove,
			flags.FavorMinNodes, flags.DryRun, flags.Verbose, nil)
		if err != nil {
			log.Fatalf("main: RunRebalance, err: %v", err)
			return
		}
	}

	// ------------------------------------------------

	err = cmd.PlannerSteps(steps, cfg, cbgt.VERSION,
		flags.Server, nodesToRemove, flags.DryRun, nil)
	if err != nil {
		log.Fatalf("main: PlannerSteps, err: %v", err)
		return
	}

	// ------------------------------------------------

	var c *ctl.Ctl

	if steps != nil && (steps["service"] || steps["rest"] || steps["prompt"]) {
		c, err = ctl.StartCtl(cfg, flags.Server, ctl.CtlOptions{
			DryRun:             flags.DryRun,
			Verbose:            flags.Verbose,
			FavorMinNodes:      flags.FavorMinNodes,
			WaitForMemberNodes: flags.WaitForMemberNodes,
		})
		if err != nil {
			log.Fatalf("main: StartCtl, err: %v", err)
			return
		}

		if steps["service"] {
			// TODO.
		}

		if steps["rest"] {
			bindHttp := flags.BindHttp
			if bindHttp[0] == ':' {
				bindHttp = "localhost" + bindHttp
			}
			if strings.HasPrefix(bindHttp, "0.0.0.0:") {
				bindHttp = "localhost" + bindHttp[len("0.0.0.0"):]
			}

			http.Handle("/", newRestRouter(c))

			go func() {
				log.Printf("------------------------------------------------------------")
				log.Printf("REST API is available: http://%s", bindHttp)
				log.Printf("------------------------------------------------------------")

				err := http.ListenAndServe(bindHttp, nil) // Blocks.
				if err != nil {
					log.Fatalf("main: listen, err: %v\n"+
						"  Please check that your -bindHttp parameter (%q)\n"+
						"  is correct and available.", err, bindHttp)
				}
			}()
		}

		if steps["prompt"] {
			go runCtlPrompt(c)
		}

		<-make(chan struct{})
	}

	// ------------------------------------------------

	log.Printf("main: done")
}

// ------------------------------------------------

func newRestRouter(ctl *ctl.Ctl) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/api/getTopology",
		func(w http.ResponseWriter, r *http.Request) {
			topology := ctl.GetTopology()
			b, _ := json.Marshal(topology)
			w.Write(b)
		}).Methods("GET")

	// TODO: POST /api/changeTopology
	// TODO: POST /api/stopChangeTopology
	// TODO: POST /api/indexDefsChanged

	return r
}
