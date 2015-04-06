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
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
)

import (
	"github.com/timtadh/getopt"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)

import (
	"github.com/timtadh/fsm/graph"
	"github.com/timtadh/fsm/mine"
	"github.com/timtadh/fsm/store"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var ErrorCodes map[string]int = map[string]int{
	"usage":   1,
	"version": 2,
	"opts":    3,
	"badint":  5,
	"baddir":  6,
	"badfile": 7,
}

var UsageMessage string = "fsm [options] -s <support> <graphs>"
var ExtendedMessage string = `
fsm - frequent subgraph mine the graph(s)

Options
    -h, --help                          print this message
    -o, --output=<path>                 path to output directory
    -s, --support=<int>                 number of unique embeddings (required)
    -m, --min-vertices=<int>            minimum number of nodes to report
                                        (5 by default)
    -c, --cache=<path>                  use an on disk cache. put the cache files
                                        in the given directory.
    --mem-cache                         use an anonymous memmory mapped cache.
                                        similar to the --cache option but doesn't
                                        create any files. relies on swap.
    --mem-profile=<path>                do a memory profile
    --cpu-profile=<path>                do a cpu profile

Specs
    <graphs>                            path to graph files
    <support>                           an int

Graph File Format

    The graph file format is a line delimited format with vertex lines and edge
    lines. For example:

    vertex	{"id":136,"label":""}
    edge	{"src":23,"targ":25,"label":"ddg"}

Grammar:

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
	if err != nil {
		panic(err)
	}
	if stat.IsDir() {
		return InputDir(input_path)
	} else {
		return InputFile(input_path)
	}
}

func InputFile(input_path string) (reader io.Reader, closeall func()) {
	freader, err := os.Open(input_path)
	if err != nil {
		panic(err)
	}
	if strings.HasSuffix(input_path, ".gz") {
		greader, err := gzip.NewReader(freader)
		if err != nil {
			panic(err)
		}
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
	if err != nil {
		panic(err)
	}
	for _, info := range dir {
		if info.IsDir() {
			continue
		}
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

func AssertDir(dir string) string {
	dir = path.Clean(dir)
	fi, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0775)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			Usage(ErrorCodes["baddir"])
		}
		return dir
	} else if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		Usage(ErrorCodes["baddir"])
	}
	if !fi.IsDir() {
		fmt.Fprintf(os.Stderr, "Passed in file was not a directory, %s", dir)
		Usage(ErrorCodes["baddir"])
	}
	return dir
}

func AssertFile(fname string) string {
	fname = path.Clean(fname)
	fi, err := os.Stat(fname)
	if err != nil && os.IsNotExist(err) {
		return fname
	} else if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		Usage(ErrorCodes["badfile"])
	} else if fi.IsDir() {
		fmt.Fprintf(os.Stderr, "Passed in file was a directory, %s", fname)
		Usage(ErrorCodes["badfile"])
	}
	return fname
}

func main() {

	log.Printf("Number of goroutines = %v", runtime.NumGoroutine())
	args, optargs, err := getopt.GetOpt(
		os.Args[1:],
		"hs:m:o:",
		[]string{
			"help",
			"support=",
			"min-vertices=",
			"cache=",
			"mem-cache",
			"mem-profile=",
			"cpu-profile=",
			"output=",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	support := -1
	minVert := 5
	cache := ""
	memCache := false
	memProfile := ""
	cpuProfile := ""
	outputDir := ""
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "-o", "--output":
			outputDir = AssertDir(oa.Arg())
		case "-s", "--support":
			support = ParseInt(oa.Arg())
		case "-m", "--min-vertices":
			minVert = ParseInt(oa.Arg())
		case "--cache":
			cache = AssertDir(oa.Arg())
		case "--mem-cache":
			memCache = true
		case "--mem-profile":
			memProfile = AssertFile(oa.Arg())
		case "--cpu-profile":
			cpuProfile = AssertFile(oa.Arg())
		}
	}

	if support < 1 {
		fmt.Fprintf(os.Stderr, "You must supply a support greater than 0, you gave %v\n", support)
		Usage(ErrorCodes["opts"])
	}

	if outputDir == "" {
		fmt.Fprintf(os.Stderr, "You must supply an output file (use -o)\n")
		Usage(ErrorCodes["opts"])
	}

	if memCache && cache != "" {
		fmt.Fprintf(os.Stderr, "You cannot supply both --cache and --mem-cache")
		Usage(ErrorCodes["opts"])
	}

	var reader io.Reader
	var close_reader func()
	if len(args) <= 0 {
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

	log.Print("Loaded graph, starting mining")
	memMaker := func() store.SubGraphs {
		return store.NewMemBpTree(127)
	}

	count := 0
	fsMaker := func() store.SubGraphs {
		name := fmt.Sprintf("fsm_bptree_%d", count)
		count++
		path := path.Join(cache, name)
		return store.NewFs2BpTree(G, path)
	}

	memFsMaker := func() store.SubGraphs {
		return store.AnonFs2BpTree(G)
	}

	var maker func() store.SubGraphs
	if memCache {
		maker = memFsMaker
	} else if cache != "" {
		maker = fsMaker
	} else {
		maker = memMaker
	}

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	var memProfFile io.WriteCloser
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Fatal(err)
		}
		memProfFile = f
		defer f.Close()
	}

	allPath := path.Join(outputDir, "all.bptree")
	labelsPath := path.Join(outputDir, "labels.bptree")
	// closedPath := path.Join(outputDir, "closed.dot")

	all := store.NewFs2BpTree(G, allPath)
	labelsBf, err := fmap.CreateBlockFile(labelsPath)
	if err != nil {
		log.Fatal(err)
	}
	labels, err := bptree.New(labelsBf, -1, 1)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(labels)

	for psg := range mine.Mine(G, support, minVert, maker, memProfFile) {
		all.Add(psg.Sg.ShortLabel(), psg)
	}

	/*
	for key, next := all.Keys()(); next != nil; key, next = next() {
		fmt.Println(hex.EncodeToString(key))
	}
	*/

	/*
	var cur []byte
	for key, _, next := all.Backward()(); next != nil; key, _, next = next() {
		if !bytes.Equal(key, cur) {
			fmt.Println(hex.EncodeToString(key))
		}
		cur = key
	}
	*/

	var cur []byte
	var had bool = false
	for key, psg, next := all.Backward()(); next != nil; key, psg, next = next() {
		if cur != nil && !bytes.Equal(key, cur) {
			if !had {
				had = false
				// output key here
				doOutput(all, cur)
			}
			addToLabels(labels, cur)
		}
		has, err := labels.Has(key)
		if err != nil {
			log.Fatal(err)
		}
		had = had || has
		cur = key
		addToLabels(labels, psg.Parent)
	}
	if !had && cur != nil {
		// output key here
		doOutput(all, cur)
	}
}

func addToLabels(labels *bptree.BpTree, label []byte) {
	has, err := labels.Has(label)
	if err != nil {
		log.Fatal(err)
	}
	if !has {
		err = labels.Add(label, []byte{0})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func doOutput(all store.SubGraphs, key []byte) {
	fmt.Println("//", hex.EncodeToString(key))
	fmt.Println()
	for _, psg, next := all.Find(key)(); next != nil; _, psg, next = next() {
		fmt.Println(psg.Sg)
	}
	fmt.Println()
	fmt.Println()
}

