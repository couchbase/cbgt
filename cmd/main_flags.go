//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

func MainCommon(version string, flagAliases map[string][]string) {
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	log.Printf("main: %s started (%s/%s)",
		os.Args[0], version, cbgt.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go DumpOnSignalForPlatform()

	LogFlags(flagAliases)
}

func LogFlags(flagAliases map[string][]string) {
	flag.VisitAll(func(f *flag.Flag) {
		if flagAliases[f.Name] != nil {
			log.Printf("  -%s=%q\n", f.Name, f.Value)
		}
	})
	log.Printf("  GOMAXPROCS=%d", runtime.GOMAXPROCS(-1))
}

// --------------------------------------------------

// The user may have informed the cmd about application specific index
// types, which we need to register (albeit with fake, "error-only"
// implementations) because the cbgt's planner (cbgt.CalcPlan()) has
// safety checks which skips any unknown, unregistered index types.
func RegisterIndexTypes(indexTypes []string) {
	newErrorPIndexImpl := func(indexType, indexParams,
		path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
		return nil, nil, fmt.Errorf("ErrorPIndex-NEW")
	}

	openErrorPIndexImpl := func(indexType, path string, restart func()) (
		cbgt.PIndexImpl, cbgt.Dest, error) {
		return nil, nil, fmt.Errorf("ErrorPIndex-OPEN")
	}

	for _, indexType := range indexTypes {
		if cbgt.PIndexImplTypes[indexType] == nil {
			cbgt.RegisterPIndexImplType(indexType,
				&cbgt.PIndexImplType{
					New:  newErrorPIndexImpl,
					Open: openErrorPIndexImpl,
				})
		}
	}
}

// ParseOptions parses an options string and environment variable.
// The optionKVs string is a comma-separated string formated as
// "key=val,key=val,...".
func ParseOptions(optionKVs string, envName string,
	options map[string]string) map[string]string {
	if options == nil {
		options = map[string]string{}
	}

	if optionKVs != "" {
		for _, kv := range strings.Split(optionKVs, ",") {
			a := strings.Split(kv, "=")
			if len(a) >= 2 {
				options[a[0]] = a[1]
			}
		}
	}

	optionsFile, exists := options["optionsFile"]
	if exists {
		log.Printf("main_flags: loading optionsFile: %q", optionsFile)

		b, err := ioutil.ReadFile(optionsFile)
		if err != nil {
			log.Warnf("main_flags: reading options file: %s, err: %v",
				optionsFile, err)
		} else {
			err = json.Unmarshal(b, &options)
			if err != nil {
				log.Warnf("main_flags: JSON parse option file: %s, err: %v",
					optionsFile, err)
			}
		}
	}

	optionsEnv := os.Getenv(envName)
	if optionsEnv != "" {
		log.Printf("main_flags: ParseOptions, envName: %s", envName)
		for _, kv := range strings.Split(optionsEnv, ",") {
			a := strings.Split(kv, "=")
			if len(a) >= 2 {
				log.Printf("main_flags: ParseOptions, option: %s", a[0])
				options[a[0]] = a[1]
			}
		}
	}

	return options
}
