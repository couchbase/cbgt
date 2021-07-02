//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

	options := cmd.ParseOptions(flags.Options, "CBGTCTL_ENV_OPTIONS", nil)

	cfg, err := cmd.MainCfgClient(path.Base(os.Args[0]), flags.CfgConnect)
	if err != nil {
		log.Fatalf("%v", err)
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

		err = rebalance.RunRebalance(cfg, flags.Server, options,
			nodesToRemove, flags.FavorMinNodes,
			flags.DryRun, flags.Verbose, nil)
		if err != nil {
			log.Fatalf("main: RunRebalance, err: %v", err)
		}
	}

	// ------------------------------------------------

	err = cmd.PlannerSteps(steps, cfg, cbgt.VERSION,
		flags.Server, options, nodesToRemove, flags.DryRun, nil)
	if err != nil {
		log.Fatalf("main: PlannerSteps, err: %v", err)
	}

	// ------------------------------------------------

	var c *ctl.Ctl

	if steps != nil && (steps["service"] || steps["rest"] || steps["prompt"]) {
		c, err = ctl.StartCtl(cfg, flags.Server, options, ctl.CtlOptions{
			DryRun:             flags.DryRun,
			Verbose:            flags.Verbose,
			FavorMinNodes:      flags.FavorMinNodes,
			WaitForMemberNodes: flags.WaitForMemberNodes,
		})
		if err != nil {
			log.Fatalf("main: StartCtl, err: %v", err)
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
