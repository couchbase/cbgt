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
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/mcp"
	"github.com/couchbase/cbgt/mcp/interfaces"
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
		log.Fatalf("main: MainCfgClient err: %v", err)
		return
	}

	if flags.IndexTypes != "" {
		cmd.RegisterIndexTypes(strings.Split(flags.IndexTypes, ","))
	}

	mcp, err := mcp.StartMCP(cfg, flags.Server, mcp.MCPOptions{
		DryRun:             flags.DryRun,
		Verbose:            flags.Verbose,
		WaitForMemberNodes: flags.WaitForMemberNodes,
	})
	if err != nil {
		log.Fatalf("main: StartMCP err: %v", err)
		return
	}

	// TODO: Need a REST API here?
	// TODO: Stats?

	log.Printf("mcp: %#v", mcp)
	log.Printf("err: %#v", err)
	log.Printf("cfg: %#v", cfg)

	if flags.Prompt {
		reader := bufio.NewReader(os.Stdin)

		i := 0
		for {
			fmt.Printf("cbgt-mcp [%d]> ", i)

			line, err := reader.ReadString('\n')
			if err != nil {
				log.Fatalf("exiting, err: %v", err)
			}

			line = strings.TrimSpace(line)
			if len(line) > 0 {
				lineParts := strings.Split(line, " ")
				if len(lineParts) > 0 {
					op := lineParts[0]
					if op == "?" || op == "h" || op == "help" {
						log.Printf("available commands:\n" +
							" getTopology\n" +
							" gt (alias for getTopology)\n" +
							" changeTopology $rev $mode $memberNodesCSV\n" +
							" ct (alias for changeTopology)\n" +
							" stopChangeTopology $rev\n" +
							" sct (alias for stopChangeTopology)\n" +
							" exit, quit, q")
					} else if op == "getTopology" || op == "gt" {
						topology := mcp.GetTopology()
						b, _ := json.Marshal(topology)
						log.Printf("topology: %s", string(b))
					} else if op == "changeTopology" || op == "ct" {
						if len(lineParts) != 4 {
							log.Printf("expected 3 arguments")
						} else {
							rev := lineParts[1]
							mode := lineParts[2]
							memberNodeUUIDs := strings.Split(lineParts[3], ",")

							log.Printf("changeTopology,"+
								" rev: %s, mode: %s, memberNodeUUIDs: %#v",
								rev, mode, memberNodeUUIDs)

							var memberNodes []interfaces.Node
							for _, memberNodeUUID := range memberNodeUUIDs {
								memberNodes = append(memberNodes, interfaces.Node{
									UUID: interfaces.UUID(memberNodeUUID),
								})
							}

							topology, err :=
								mcp.ChangeTopology(&interfaces.ChangeTopology{
									Rev:         interfaces.Rev(rev),
									Mode:        mode,
									MemberNodes: memberNodes,
								})

							b, _ := json.Marshal(topology)
							log.Printf("topology: %s", string(b))

							log.Printf("err: %v", err)
						}
					} else if op == "stopChangeTopology" || op == "sct" {
						if len(lineParts) != 2 {
							log.Printf("expected 1 arguments")
						} else {
							rev := lineParts[1]

							log.Printf("stopChangeTopology, rev: %s", rev)

							mcp.StopChangeTopology(interfaces.Rev(rev))
						}
					} else if op == "quit" || op == "q" || op == "exit" {
						log.Printf("bye")
						os.Exit(0)
					} else {
						log.Printf("unknown op: %s", op)
					}
				}
			}

			i++
		}
	}
}
