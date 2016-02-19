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

package cmd

import (
	"flag"
	"fmt"
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

	optionsEnv := os.Getenv(envName)
	if optionsEnv != "" {
		log.Printf(envName)
		for _, kv := range strings.Split(optionsEnv, ",") {
			a := strings.Split(kv, "=")
			if len(a) >= 2 {
				log.Printf("  %s", a[0])
				options[a[0]] = a[1]
			}
		}
	}

	return options
}
