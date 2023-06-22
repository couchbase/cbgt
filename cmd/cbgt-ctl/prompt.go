//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

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
						log.Warnf("expected 3 arguments")
					} else {
						rev := lineParts[1]
						mode := lineParts[2]
						memberNodeUUIDs := strings.Split(lineParts[3], ",")

						log.Printf("changeTopology,"+
							" rev: %s, mode: %s, memberNodeUUIDs: %#v",
							rev, mode, memberNodeUUIDs)
						var topology *ctl.CtlTopology
						topology, err =
							ctlInst.ChangeTopology(&ctl.CtlChangeTopology{
								Rev:             rev,
								Mode:            mode,
								MemberNodeUUIDs: memberNodeUUIDs,
							}, nil)

						b, _ := json.Marshal(topology)
						log.Printf("topology: %s", string(b))

						log.Printf("err: %v", err)
					}
				} else if op == "stopChangeTopology" || op == "sct" {
					if len(lineParts) != 2 {
						log.Warnf("expected 1 arguments")
					} else {
						rev := lineParts[1]

						log.Printf("stopChangeTopology, rev: %s", rev)

						ctlInst.StopChangeTopology(rev)
					}
				} else if op == "indexDefsChanged" || op == "idc" {
					err = ctlInst.IndexDefsChanged(time.Time{})

					log.Printf("err: %v", err)
				} else if op == "quit" || op == "q" || op == "exit" {
					log.Printf("bye")
					os.Exit(0)
				} else {
					log.Warnf("unknown op: %s", op)
				}
			}
		}

		i++
	}
}
