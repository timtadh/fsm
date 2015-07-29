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
	"encoding/json"
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
    --left-most                         use left most extension (non-complete)
    -s, --support=<int>                 number of unique embeddings (required)
    --max-support=<int>                 max-number of unique embeddings
    -m, --min-vertices=<int>            minimum number of nodes to report
                                        (5 by default)
    --start-prefix=<string>             prefix of nodes to use in initial set
    --support-attr=<string>             an attribute in the json of the node which
                                        should be unique when counting support.
                                        Note the value of this attr must be a string
                                        or things will fail horribly!
    --no-attrs                          do not load node attributes
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

func EmptyDir(dir string) string {
	dir = path.Clean(dir)
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0775)
		if err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	} else {
		// something already exists lets delete it
		err := os.RemoveAll(dir)
		if err != nil {
			log.Fatal(err)
		}
		err = os.MkdirAll(dir, 0775)
		if err != nil {
			log.Fatal(err)
		}
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
			fmt.Println("depth")
			fmt.Println("random-walk")
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
	case "depth":
		Depth(args[1:])
	case "random-walk":
		RandomWalk(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode %v\n", args[0])
		Usage(ErrorCodes["opts"])
	}
	log.Println("Done!")
}

func RandomWalk(argv []string) {
	log.Printf("Number of goroutines = %v", runtime.NumGoroutine())
	args, optargs, err := getopt.GetOpt(
		argv,
		"hs:m:o:",
		[]string{
			"help",
			"support=",
			"cache=",
			"min-vertices=",
			"sample-size=",
			"mem-profile=",
			"cpu-profile=",
			"output=",
			"probabilities",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	support := -1
	minVertices := -1
	sampleSize := -1
	memProfile := ""
	cpuProfile := ""
	outputDir := ""
	cache := ""
	matrices := true
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "-o", "--output":
			outputDir = EmptyDir(AssertDir(oa.Arg()))
		case "-s", "--support":
			support = ParseInt(oa.Arg())
		case "-m", "--min-vertices":
			minVertices = ParseInt(oa.Arg())
		case "--probabilities":
			matrices = false
		case "--cache":
			cache = AssertDir(oa.Arg())
		case "--sample-size":
			sampleSize = ParseInt(oa.Arg())
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

	if sampleSize < 1 {
		fmt.Fprintf(os.Stderr, "You must supply a sample-size greater than 0, you gave %v\n", sampleSize)
		Usage(ErrorCodes["opts"])
	}

	if outputDir == "" {
		fmt.Fprintf(os.Stderr, "You must supply an output file (use -o)\n")
		Usage(ErrorCodes["opts"])
	}

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Expected a path to the graph file")
		Usage(ErrorCodes["opts"])
	}
	getReader := func() (io.Reader, func()) { return Input(args[0]) }

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
	maxPath := path.Join(outputDir, "max-embeddings.bptree")
	nodePath := path.Join(outputDir, "node-attrs.bptree")

	nodeBf, err := fmap.CreateBlockFile(nodePath)
	if err != nil {
		log.Fatal(err)
	}
	defer nodeBf.Close()
	nodeAttrs, err := bptree.New(nodeBf, 4, -1)
	if err != nil {
		log.Fatal(err)
	}

	G, err := graph.LoadGraph(getReader, "", nodeAttrs, nil)
	if err != nil {
		log.Println("Error loading the graph")
		log.Panic(err)
	}
	log.Print("Loaded graph, about to start mining")

	all := store.NewFs2BpTree(G, allPath)
	defer all.Close()

	max := store.NewFs2BpTree(G, maxPath)
	defer max.Close()

	fsCount := 0
	fsMaker := func() store.SubGraphs {
		name := fmt.Sprintf("fsm_bptree_%d", fsCount)
		fsCount++
		path := path.Join(cache, name)
		s := store.NewFs2BpTree(G, path)
		// os.Remove(path)
		// s, err := store.NewSqlite(G, path)
		// if err != nil {
		// 	log.Panic(err)
		// }
		return s
	}

	memMaker := func() store.SubGraphs {
		return store.NewMemBpTree(127)
	}

	// memFsMaker := func() store.SubGraphs {
	// 	return store.AnonFs2BpTree(G)
	// }

	var maker func() store.SubGraphs
	if cache != "" {
		maker = fsMaker
	} else {
		maker = memMaker
	}

	m := mine.RandomWalk(
		G,
		support,
		minVertices,
		sampleSize,
		memProfFile,
		maker,
	)
	for sg := range m.MaxReport {
		max.Add(sg.ShortLabel(), sg)
	}
	log.Println("Finished mining! Writing output...")
	keys := make(chan []byte)
	go func() {
		for key, next := max.Keys()(); next != nil; key, next = next() {
			if max.Count(key) < support {
				continue
			}
			keys<-key
		}
		close(keys)
	}()
	writeMaximalPatterns(keys, max, nodeAttrs, outputDir)
	log.Println("Finished writing patterns. Computing probabilities...")

	count := 0
	for key, next := max.Keys()(); next != nil; key, next = next() {
		patDir := path.Join(outputDir, fmt.Sprintf("%d", count))
		log.Println("-----------------------------------")
		if max.Count(key) < support {
			log.Println("wat not enough subgraphs", max.Count(key))
			continue
		}
		for _, sg, next := max.Find(key)(); next != nil; _, sg, next = next() {
			if matrices {
				vp, Q, R, u, err := m.PrMatrices(sg)
				if err != nil {
					log.Println(err)
					errPath := path.Join(patDir, "error")
					if f, e := os.Create(errPath); e != nil {
						log.Fatal(err)
					} else {
						fmt.Fprintln(f, err)
						f.Close()
					}
				} else {
					bytes, err := json.Marshal(map[string]interface{}{
						"Q": Q,
						"R": R,
						"u": u,
						"startingPoints": vp,
					})
					if err != nil {
						log.Fatal(err)
					}
					matPath := path.Join(patDir, "matrices.json")
					if m, err := os.Create(matPath); err != nil {
						log.Fatal(err)
					} else {
						_, err := m.Write(bytes)
						if err != nil {
							m.Close()
							log.Fatal(err)
						}
						m.Close()
					}
				}
			} else {
				P, err := m.SelectionProbability(sg)
				if err == nil {
					log.Println(P, sg.Label())
					patPr := path.Join(patDir, "pattern.pr")
					if pr, err := os.Create(patPr); err != nil {
						log.Fatal(err)
					} else {
						fmt.Fprintln(pr, P)
						pr.Close()
					}
				}
			}
			break
		}
		count++
	}
	log.Println("Done!")
}

func Depth(argv []string) {
	log.Printf("Number of goroutines = %v", runtime.NumGoroutine())
	args, optargs, err := getopt.GetOpt(
		argv,
		"hs:m:o:",
		[]string{
			"help",
			"support=",
			"max-support=",
			"min-vertices=",
			"queue-size=",
			"score=",
			"mem-profile=",
			"cpu-profile=",
			"output=",
			"maximal",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	support := -1
	maxSupport := -1
	minVertices := 5
	queueSize := 100
	memProfile := ""
	cpuProfile := ""
	outputDir := ""
	score := "random"
	maximal := false
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "-o", "--output":
			outputDir = AssertDir(oa.Arg())
		case "-s", "--support":
			support = ParseInt(oa.Arg())
		case "--max-support":
			maxSupport = ParseInt(oa.Arg())
		case "-m", "--min-vertices":
			minVertices = ParseInt(oa.Arg())
		case "--queue-size":
			queueSize = ParseInt(oa.Arg())
		case "--maximal":
			maximal = true
		case "--mem-profile":
			memProfile = AssertFile(oa.Arg())
		case "--cpu-profile":
			cpuProfile = AssertFile(oa.Arg())
		case "--score":
			score = oa.Arg()
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

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Expected a path to the graph file")
		Usage(ErrorCodes["opts"])
	}
	getReader := func() (io.Reader, func()) { return Input(args[0]) }

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

	nodeBf, err := fmap.CreateBlockFile(nodePath)
	if err != nil {
		log.Fatal(err)
	}
	defer nodeBf.Close()
	nodeAttrs, err := bptree.New(nodeBf, 4, -1)
	if err != nil {
		log.Fatal(err)
	}

	G, err := graph.LoadGraph(getReader, "", nodeAttrs, nil)
	if err != nil {
		log.Println("Error loading the graph")
		log.Panic(err)
	}
	log.Print("Loaded graph, about to start mining")

	all := store.NewFs2BpTree(G, allPath)
	defer all.Close()

	memFsMaker := func() store.SubGraphs {
		return store.AnonFs2BpTree(G)
	}

	embeddings := mine.Depth(
		G,
		score,
		support, maxSupport, minVertices, queueSize,
		memFsMaker,
		memProfFile,
	)
	for sg := range embeddings {
		all.Add(sg.ShortLabel(), sg)
	}
	if maximal {
		log.Print("Finished mining, about to compute maximal frequent subgraphs.")
		writeMaximalSubGraphs(all, nodeAttrs, outputDir)
	}
	log.Println("Finished mining! Writing output...")
	writeAllPatterns(all, nodeAttrs, outputDir)
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
			"left-most",
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
	leftMost := false
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
		case "--left-most":
			leftMost = true
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

	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Expected a path to the graph file")
		Usage(ErrorCodes["opts"])
	}
	getReader := func() (io.Reader, func()) { return Input(args[0]) }

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
	
	embeddings := mine.Breadth(
		G, supportAttrs,
		support, maxSupport, minVert, maxRounds,
		startPrefix, supportAttr,
		vertexExtend, leftMost,
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
	writeAllPatterns(all, nodeAttrs, outputDir)
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
	count := 0
	for key, next := all.Keys()(); next != nil; key, next = next() {
		writePattern(count, outputDir, alle, allp, nodeAttrs, all, key)
		count++
	}
}

func writeMaximalSubGraphs(all store.SubGraphs, nodeAttrs *bptree.BpTree, outputDir string) {
	keys, err := mine.MaximalSubGraphs(all, nodeAttrs, outputDir) 
	if err != nil {
		log.Fatal(err)
	}
	writeMaximalPatterns(keys, all, nodeAttrs, outputDir)
}

func writeMaximalPatterns(keys <-chan []byte, sgs store.SubGraphs, nodeAttrs *bptree.BpTree, outputDir string) {
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
	count := 0
	for key := range keys {
		writePattern(count, outputDir, maxe, maxp, nodeAttrs, sgs, key)
		count++
	}
	countPath := path.Join(outputDir, "count")
	if c, err := os.Create(countPath); err != nil {
		log.Fatal(err)
	} else {
		fmt.Fprintln(c, count)
		c.Close()
	}
}

func writePattern(count int, outDir string, embeddings, patterns io.Writer, nodeAttrs *bptree.BpTree, all store.SubGraphs, key []byte) {
	patDir := EmptyDir(path.Join(outDir, fmt.Sprintf("%d", count)))
	patDot := path.Join(patDir, "pattern.dot")
	patVeg := path.Join(patDir, "pattern.veg")
	patName := path.Join(patDir, "pattern.name")
	patCount := path.Join(patDir, "count")
	instDir := EmptyDir(path.Join(patDir, "instances"))
	i := 0
	for _, sg, next := all.Find(key)(); next != nil; _, sg, next = next() {
		if i == 0 {
			fmt.Fprintln(patterns, "//", sg.Label())
			fmt.Fprintln(patterns)
			fmt.Fprintln(patterns, sg.String())
			fmt.Fprintln(embeddings, "//", sg.Label())
			fmt.Fprintln(embeddings)
			if pat, err := os.Create(patDot); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(pat, sg.String())
				pat.Close()
			}
			if name, err := os.Create(patName); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(name, sg.Label())
				name.Close()
			}
			if veg, err := os.Create(patVeg); err != nil {
				log.Fatal(err)
			} else {
				veg.Write(sg.VEG(nil))
				veg.Close()
			}
		}
		curDir := EmptyDir(path.Join(instDir, fmt.Sprintf("%d", i)))
		emDot := path.Join(curDir, "embedding.dot")
		emVeg := path.Join(curDir, "embedding.veg")
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
			if em, err := os.Create(emDot); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(em, sg.StringWithAttrs(attrs))
				em.Close()
			}
			if veg, err := os.Create(emVeg); err != nil {
				log.Fatal(err)
			} else {
				veg.Write(sg.VEG(attrs))
				veg.Close()
			}
		} else {
			fmt.Fprintln(embeddings, sg.String())
			if em, err := os.Create(emDot); err != nil {
				log.Fatal(err)
			} else {
				fmt.Fprintln(em, sg.String())
				em.Close()
			}
			if veg, err := os.Create(emVeg); err != nil {
				log.Fatal(err)
			} else {
				veg.Write(sg.VEG(nil))
				veg.Close()
			}
		}
		i++
	}
	if c, err := os.Create(patCount); err != nil {
		log.Fatal(err)
	} else {
		fmt.Fprintln(c, i)
		c.Close()
	}
	fmt.Fprintln(patterns)
	fmt.Fprintln(patterns)
	fmt.Fprintln(embeddings)
	fmt.Fprintln(embeddings)
}

