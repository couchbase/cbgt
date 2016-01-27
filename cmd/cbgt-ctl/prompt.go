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
	"fmt"
	"os"
	"strings"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt/ctl"
)

func runCtlPrompt(ctlInst *ctl.Ctl) {
	reader := bufio.NewReader(os.Stdin)

	i := 0
	for {
		fmt.Printf("ctl [%d]> ", i)

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
						" changeTopology $rev $mode $memberNodeUUIDsCSV\n" +
						" ct (alias for changeTopology)\n" +
						" stopChangeTopology $rev\n" +
						" sct (alias for stopChangeTopology)\n" +
						" indexDefsChanged\n" +
						" idc (alias for indexDefsChanged)\n" +
						" exit, quit, q")
				} else if op == "getTopology" || op == "gt" {
					topology := ctlInst.GetTopology()
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
						topology, err :=
							ctlInst.ChangeTopology(&ctl.CtlChangeTopology{
								Rev:             rev,
								Mode:            mode,
								MemberNodeUUIDs: memberNodeUUIDs,
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

						ctlInst.StopChangeTopology(rev)
					}
				} else if op == "indexDefsChanged" || op == "idc" {
					err = ctlInst.IndexDefsChanged()

					log.Printf("err: %v", err)
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
