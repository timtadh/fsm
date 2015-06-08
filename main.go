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
	"compress/gzip"
	"encoding/binary"
	// "encoding/hex"
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
	"usage":   0,
	"version": 2,
	"opts":    3,
	"badint":  5,
	"baddir":  6,
	"badfile": 7,
}

var UsageMessage string = "fsm --help"
var ExtendedMessage string = `
fsm - frequent subgraph mining

fsm is a multi-mode program. It is really several programs in one. The
first argument specifies the mode to use. To see the available modes run:

    fsm --modes

Global Options
    -h, --help                          view this message
    --modes                             show the available modes

Modes
    breadth                             generate the frequent subgraphs in a breadth
                                        first manner.

Usage for breadth
    fsm breadth [options] -s <support> -o <output-dir> <graphs>

Options for 'breadth'
    -h, --help                          print this message
    -o, --output=<path>                 path to output directory
    --vertex-extend                     extend the subgraphs one vertex at a
                                        time (instead of one edge at a time).
                                        The result may be less complete than the
                                        default mode.
    -s, --support=<int>                 number of unique embeddings (required)
    --max-support=<int>                 max-number of unique embeddings
    -m, --min-vertices=<int>            minimum number of nodes to report
                                        (5 by default)
    --start-prefix=<string>             prefix of nodes to use in initial set
    --support-attr=<string>             an attribute in the json of the node which
                                        should be unique when counting support.
                                        Note the value of this attr must be a string
                                        or things will fail horribly!
    --max-rounds=<int>                  maxium number of rounds to do
    --maximal                           only report maximal frequent subgraphs
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
	args, optargs, err := getopt.GetOpt(
		os.Args[1:],
		"h", []string{ "help", "modes" },
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, "could not process your arguments (perhaps you forgot a mode?) try:")
		fmt.Fprintf(os.Stderr, "$ %v breadth %v\n", os.Args[0], strings.Join(os.Args[1:], " "))
		Usage(ErrorCodes["opts"])
	}

	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "--modes":
			fmt.Println("breadth")
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "Unknown flag '%v'\n", oa.Opt())
			Usage(ErrorCodes["opts"])
		}
	}
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "You must supply a mode (eg. breadth)\n")
		Usage(ErrorCodes["opts"])
	}
	switch args[0] {
	case "breadth":
		Breadth(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode %v\n", args[0])
		Usage(ErrorCodes["opts"])
	}
	log.Println("Done!")
}

func Breadth(argv []string) {

	log.Printf("Number of goroutines = %v", runtime.NumGoroutine())
	args, optargs, err := getopt.GetOpt(
		argv,
		"hs:m:o:",
		[]string{
			"help",
			"vertex-extend",
			"maximal",
			"support=",
			"max-support=",
			"min-vertices=",
			"max-rounds=",
			"start-prefix=",
			"support-attr=",
			"cache=",
			"mem-cache",
			"mem-profile=",
			"cpu-profile=",
			"output=",
			"no-attrs",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	vertexExtend := false
	support := -1
	maxSupport := -1
	minVert := 5
	maxRounds := -1
	startPrefix := ""
	supportAttr := ""
	maximal := false
	cache := ""
	memCache := false
	memProfile := ""
	cpuProfile := ""
	outputDir := ""
	noAttrs := false
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "-o", "--output":
			outputDir = AssertDir(oa.Arg())
		case "--vertex-extend":
			vertexExtend = true
		case "--maximal":
			maximal = true
		case "-s", "--support":
			support = ParseInt(oa.Arg())
		case "--max-support":
			maxSupport = ParseInt(oa.Arg())
		case "-m", "--min-vertices":
			minVert = ParseInt(oa.Arg())
		case "--start-prefix":
			startPrefix = oa.Arg()
		case "--support-attr":
			supportAttr = oa.Arg()
		case "--max-rounds":
			maxRounds = ParseInt(oa.Arg())
		case "--cache":
			cache = AssertDir(oa.Arg())
		case "--mem-cache":
			memCache = true
		case "--mem-profile":
			memProfile = AssertFile(oa.Arg())
		case "--cpu-profile":
			cpuProfile = AssertFile(oa.Arg())
		case "--no-attrs":
			noAttrs = true
		}
	}

	if support < 1 {
		fmt.Fprintf(os.Stderr, "You must supply a support greater than 0, you gave %v\n", support)
		Usage(ErrorCodes["opts"])
	}

	if maxSupport < 1 {
		maxSupport = support*100
	}

	if outputDir == "" {
		fmt.Fprintf(os.Stderr, "You must supply an output file (use -o)\n")
		Usage(ErrorCodes["opts"])
	}

	if memCache && cache != "" {
		fmt.Fprintf(os.Stderr, "You cannot supply both --cache and --mem-cache")
		Usage(ErrorCodes["opts"])
	}

	var getReader func() (io.Reader, func())
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "You must supply the graphs as a file")
		Usage(ErrorCodes["opts"])
	} else {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "Expected a path to the graph file")
			Usage(ErrorCodes["opts"])
		}
		getReader = func() (io.Reader, func()) { return Input(args[0]) }
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

	allPath := path.Join(outputDir, "all-embeddings.bptree")
	nodePath := path.Join(outputDir, "node-attrs.bptree")

	var nodeAttrs *bptree.BpTree = nil
	if !noAttrs {
		nodeBf, err := fmap.CreateBlockFile(nodePath)
		if err != nil {
			log.Fatal(err)
		}
		defer nodeBf.Close()
		nodeAttrs, err = bptree.New(nodeBf, 4, -1)
		if err != nil {
			log.Fatal(err)
		}
	}

	supportAttrs := make(map[int]string)
	G, err := graph.LoadGraph(getReader, supportAttr, nodeAttrs, supportAttrs)
	if err != nil {
		log.Println("Error loading the graph")
		log.Panic(err)
	}
	log.Print("Loaded graph, about to start mining")

	all := store.NewFs2BpTree(G, allPath)
	defer all.Close()

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
	
	embeddings := mine.Mine(
		G, supportAttrs,
		support, maxSupport, minVert, maxRounds,
		startPrefix, supportAttr,
		vertexExtend,
		maker,
		memProfFile,
	)
	for sg := range embeddings {
		all.Add(sg.ShortLabel(), sg)
	}

	if maximal {
		log.Print("Finished mining, about to compute maximal frequent subgraphs.")
		writeMaximalSubGraphs(all, nodeAttrs, outputDir)
	}
	log.Print("Finished mining, writing output.")
	/*
	for i, c := range G.Colors {
		fmt.Printf("%d '%v'\n", i, c)
	}*/
	log.Print("Done!")
}

func writeAllPatterns(all store.SubGraphs, nodeAttrs *bptree.BpTree, outputDir string) {
	alle, err := os.Create(path.Join(outputDir, "all-embeddings.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer alle.Close()
	allp, err := os.Create(path.Join(outputDir, "all-patterns.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer allp.Close()
	for key, next := all.Keys()(); next != nil; key, next = next() {
		writePattern(alle, allp, nodeAttrs, all, key)
	}
}

func writeMaximalSubGraphs(all store.SubGraphs, nodeAttrs *bptree.BpTree, outputDir string) {
	maxe, err := os.Create(path.Join(outputDir, "maximal-embeddings.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer maxe.Close()
	maxp, err := os.Create(path.Join(outputDir, "maximal-patterns.dot"))
	if err != nil {
		log.Fatal(err)
	}
	defer maxp.Close()
	keys, err := mine.MaximalSubGraphs(all, nodeAttrs, outputDir) 
	if err != nil {
		log.Fatal(err)
	}
	for key := range keys {
		writePattern(maxe, maxp, nodeAttrs, all, key)
	}
}


func writePattern(embeddings, patterns io.Writer, nodeAttrs *bptree.BpTree, all store.SubGraphs, key []byte) {
	i := 0
	for _, sg, next := all.Find(key)(); next != nil; _, sg, next = next() {
		if i == 0 {
			fmt.Fprintln(patterns, "//", sg.Label())
			fmt.Fprintln(patterns)
			fmt.Fprintln(patterns, sg.String())
			fmt.Fprintln(embeddings, "//", sg.Label())
			fmt.Fprintln(embeddings)
		}
		if nodeAttrs != nil {
			attrs := make(map[int]map[string]interface{})
			for _, v := range sg.V {
				bid := make([]byte, 4)
				binary.BigEndian.PutUint32(bid, uint32(v.Id))
				err := nodeAttrs.DoFind(
					bid,
					func(key, value []byte) error {
						a, err := graph.ParseJson(value)
						if err != nil {
							log.Fatal(err)
						}
						attrs[v.Id] = a
						return nil
					})
				if err != nil {
					log.Fatal(err)
				}
			}
			fmt.Fprintln(embeddings, sg.StringWithAttrs(attrs))
		} else {
			fmt.Fprintln(embeddings, sg.String())
		}
		i++
	}
	fmt.Fprintln(patterns)
	fmt.Fprintln(patterns)
	fmt.Fprintln(embeddings)
	fmt.Fprintln(embeddings)
}

