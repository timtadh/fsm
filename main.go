package main

/* Tim Henderson (tadh@case.edu)
*
* Copyright (c) 2015, Tim Henderson, Case Western Reserve University
* Cleveland, Ohio 44106. All Rights Reserved.
*
* This library is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 3 of the License, or (at
* your option) any later version.
*
* This library is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this library; if not, write to the Free Software
* Foundation, Inc.,
*   51 Franklin Street, Fifth Floor,
*   Boston, MA  02110-1301
*   USA
*/

import (
	"os"
	"fmt"
	"path"
	"io"
	"io/ioutil"
	"compress/gzip"
	"strings"
	"strconv"
)

import (
	"github.com/timtadh/getopt"
)

import (
	"github.com/timtadh/fsm/graph"
	"github.com/timtadh/fsm/mine"
)

var ErrorCodes map[string]int = map[string]int{
	"usage":1,
	"version":2,
	"opts":3,
	"badint":5,
	"baddir":6,
}

var UsageMessage string = "fsm [options] <graphs>"
var ExtendedMessage string = `
fsm - frequent subgraph mine the graph(s)

Options
-h, --help                          print this message
-s, --stdin                         read from stdin instead
-e, --edge-filter=<string>          filter edges of type <string> (can be
specified multiple times)
-n, --node-filter=<string>          filter nodes of type <string> (can be
specified multiple times)

Specs
<graphs>                            path to graph files
<direction>                         {backward, forward, both}

Graph File Format
The graph file format is a line delimited format with vertex lines and edge
lines. For example:

vertex	{"id":136,"label":""}
edge	{"src":23,"targ":25,"label":"ddg"}

Format:

line -> vertex "\n"
| edge "\n"

vertex -> "vertex" "\t" vertex_json

edge -> "edge" "\t" edge_json

vertex_json -> {"id": int, "label": string, ...}
// other items are optional

edge_json -> {"src": int, "targ": int, "label": int, ...}
// other items are  optional
`

func Usage(code int) {
	fmt.Fprintln(os.Stderr, UsageMessage)
	if code == 0 {
		fmt.Fprintln(os.Stdout, ExtendedMessage)
		code = ErrorCodes["usage"]
	} else {
		fmt.Fprintln(os.Stderr, "Try -h or --help for help")
	}
	os.Exit(code)
}

func Input(input_path string) (reader io.Reader, closeall func()) {
	stat, err := os.Stat(input_path)
	if err != nil { panic(err) }
	if stat.IsDir() {
		return InputDir(input_path)
	} else {
		return InputFile(input_path)
	}
}

func InputFile(input_path string) (reader io.Reader, closeall func()) {
	freader, err := os.Open(input_path)
	if err != nil { panic(err) }
	if strings.HasSuffix(input_path, ".gz") {
		greader, err := gzip.NewReader(freader)
		if err != nil { panic(err) }
		return greader, func() {
			greader.Close()
			freader.Close()
		}
	}
	return freader, func() {
		freader.Close()
	}
}

func InputDir(input_dir string) (reader io.Reader, closeall func()) {
	var readers []io.Reader
	var closers []func()
	dir, err := ioutil.ReadDir(input_dir)
	if err != nil { panic(err) }
	for _, info := range dir {
		if info.IsDir() { continue }
		creader, closer := InputFile(path.Join(input_dir, info.Name()))
		readers = append(readers, creader)
		closers = append(closers, closer)
	}
	reader = io.MultiReader(readers...)
	return reader, func() {
		for _, closer := range closers {
			closer()
		}
	}
}

func ParseInt(str string) int {
	i, err := strconv.Atoi(str)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing '%v' expected an int\n", str)
		Usage(ErrorCodes["badint"])
	}
	return i
}

func main() {

	args, optargs, err := getopt.GetOpt(
		os.Args[1:],
		"hsp:c:d:e:n:",
		[]string{
			"help",
			"stdin",
			"edge-filter=",
			"node-filter=",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	stdin := false
	filtered_edges := make(map[string]bool)
	filtered_nodes := make(map[string]bool)
	for _, oa := range optargs {
		switch oa.Opt() {
			case "-h", "--help": Usage(0)
			case "-s", "--stdin": stdin = true
		case "-e", "--edge-filter":
			filtered_edges[oa.Arg()] = true
		case "-n", "--node-filter":
			filtered_nodes[oa.Arg()] = true
		}
	}

	var reader io.Reader
	var close_reader func()
	if stdin {
		reader = os.Stdin
		close_reader = func() {}
	} else {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "Expected a path to the graph file")
			Usage(ErrorCodes["opts"])
		}
		reader, close_reader = Input(args[0])
	}
	defer close_reader()

	type json_object map[string]interface{}


	G, err := graph.LoadGraph(reader)

	if err != nil {
		fmt.Fprintln(os.Stderr, "Error loading the graph")
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	for sg := range mine.Mine(G, 4) {
		fmt.Println(sg)
	}
}


