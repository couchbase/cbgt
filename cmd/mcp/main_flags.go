//  Copyright (c) 2014 Couchbase, Inc.
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
	"sort"
	"strings"
)

const DEFAULT_DATA_DIR = "data"

type Flags struct {
	CfgConnect string
	DataDir    string
	Help       bool
	Server     string
	Version    bool
}

var flags Flags
var flagAliases map[string][]string

func init() {
	flagAliases = initFlags(&flags)
}

func initFlags(flags *Flags) map[string][]string {
	flagAliases := map[string][]string{} // main flag name => all aliases.
	flagKinds := map[string]string{}

	s := func(v *string, names []string, kind string,
		defaultVal, usage string) { // String cmd-line param.
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	b := func(v *bool, names []string, kind string,
		defaultVal bool, usage string) { // Bool cmd-line param.
		for _, name := range names {
			flag.BoolVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
		flagKinds[names[0]] = kind
	}

	s(&flags.CfgConnect,
		[]string{"cfgConnect", "cfg", "c"}, "CFG_CONNECT", "simple",
		"connection string to a configuration provider/server"+
			"\nfor clustering multiple nodes:"+
			"\n* couchbase:http://BUCKET_USER:BUCKET_PSWD@CB_HOST:CB_PORT"+
			"\n     - manages a cluster configuration in a couchbase"+
			"\n       3.x bucket; for example:"+
			"\n       'couchbase:http://my-cfg-bucket@127.0.0.1:8091';"+
			"\n* simple"+
			"\n     - intended for development usage, the 'simple'"+
			"\n       configuration provider manages a configuration"+
			"\n       for a single, unclustered node in a local"+
			"\n       file that's stored in the dataDir;"+
			"\n* metakv"+
			"\n     - manages a cluster configuration in metakv store;"+
			"\n       environment variable CBAUTH_REVRPC_URL needs to be set"+
			"\n       for metakv; for example:"+
			"\n       'export CBAUTH_REVRPC_URL=http://user:password@localhost:9000/mcp';"+
			"\ndefault is 'simple'.")
	b(&flags.Help,
		[]string{"help", "?", "H", "h"}, "", false,
		"print this usage message and exit.")
	s(&flags.Server,
		[]string{"server", "s"}, "URL", "",
		"URL to datasource server; example when using couchbase 3.x as"+
			"\nyour datasource server: 'http://localhost:8091';"+
			"\nuse '.' when there is no datasource server.")
	b(&flags.Version,
		[]string{"version", "v"}, "", false,
		"print version string and exit.")

	flag.Usage = func() {
		if !flags.Help {
			return
		}

		base := path.Base(os.Args[0])

		fmt.Fprintf(os.Stderr, "%s: couchbase mcp\n", base)
		fmt.Fprintf(os.Stderr, "\nUsage: %s [flags]\n", base)
		fmt.Fprintf(os.Stderr, "\nFlags:\n")

		flagsByName := map[string]*flag.Flag{}
		flag.VisitAll(func(f *flag.Flag) {
			flagsByName[f.Name] = f
		})

		flags := []string(nil)
		for name := range flagAliases {
			flags = append(flags, name)
		}
		sort.Strings(flags)

		for _, name := range flags {
			aliases := flagAliases[name]
			a := []string(nil)
			for i := len(aliases) - 1; i >= 0; i-- {
				a = append(a, aliases[i])
			}
			f := flagsByName[name]
			fmt.Fprintf(os.Stderr, "  -%s %s\n",
				strings.Join(a, ", -"), flagKinds[name])
			fmt.Fprintf(os.Stderr, "      %s\n",
				strings.Join(strings.Split(f.Usage, "\n"),
					"\n      "))
		}

		fmt.Fprintf(os.Stderr, "\nExamples:")
		fmt.Fprintf(os.Stderr, examples)
		fmt.Fprintf(os.Stderr, "\nSee also:"+
			" http://github.com"+
			"/couchbaselabs/cbgt/tree/master/cmd/mcp\n\n")
	}

	return flagAliases
}

const examples = `
  Example where mcp's configuration is kept in a couchbase "cfg-bucket":
    ./mcp -cfg=couchbase:http://cfg-bucket@CB_HOST:8091
`