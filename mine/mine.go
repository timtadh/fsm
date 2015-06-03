package mine

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
	"fmt"
	"bytes"
	"hash/fnv"
	"io"
	"log"
	"runtime"
	"runtime/pprof"
	// "time"
	"strings"
	"sync"
)

import (
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/fsm/store"
	"github.com/timtadh/goiso"
)

type partitionIterator func() (part store.Iterator, next partitionIterator)

type Miner struct {
	Graph *goiso.Graph
	VertexExtend bool
	Support int
	MinVertices int
	MaxSupport int
	MaxRounds int
	StartPrefix string
	Report chan<- *goiso.SubGraph
	MakeStore func() store.SubGraphs
}

func Mine(
	G *goiso.Graph,
	support, maxSupport, minVertices, maxRounds int,
	startPrefix string,
	vertexExtend bool,
	makeStore func() store.SubGraphs,
	memProf io.Writer,
) (
	<-chan *goiso.SubGraph,
) {
	CPUs := runtime.NumCPU()
	fsg := make(chan *goiso.SubGraph)
	// ticker := time.NewTicker(5000 * time.Millisecond)
	// go func(ch <-chan time.Time) {
	// 	for _ = range ch {
	// 		runtime.GC()
	// 	}
	// }(ticker.C)
	m := &Miner{
		Graph: G,
		Support: support,
		MaxSupport: maxSupport,
		MinVertices: minVertices,
		MaxRounds: maxRounds,
		StartPrefix: startPrefix,
		VertexExtend: vertexExtend,
		Report: fsg,
		MakeStore: makeStore,
	}
	var profMutex sync.Mutex
	miner := func() {
		p_it, collectors := m.initial()
		round := 1
		var pc *Collectors
		for true {
			if pc != nil {
				pc.delete()
			}
			pc = collectors
			collectors = m.makeCollectors(CPUs*2)
			log.Printf("starting filtering %v", round)
			m.filterAndExtend(CPUs*4, p_it, collectors.makeSend())
			collectors.close()
			size := collectors.size() 
			if size <= 0 || (m.MaxRounds > 0 && round >= m.MaxRounds) {
				break
			}
			p_it = collectors.partsCh()
			// runtime.GC()
			log.Printf("finished %v with %v", round, size)
			log.Printf("Number of goroutines = %v", runtime.NumGoroutine())
			round++
			if memProf != nil {
				profMutex.Lock()
				pprof.WriteHeapProfile(memProf)
				profMutex.Unlock()
			}
		}
		if pc != nil {
			pc.delete()
		}
		if collectors != nil {
			collectors.delete()
		}
		// ticker.Stop()
		close(m.Report)
	}
	go miner()
	return fsg
}

func (m *Miner) initial() (<-chan store.Iterator, *Collectors) {
	CPUs := runtime.NumCPU()
	collectors := m.makeCollectors(CPUs)
	m.Initial(collectors.makeSend())
	collectors.close()
	return collectors.partsCh(), collectors
}

func (m *Miner) Initial(send func(*goiso.SubGraph)) {
	log.Printf("Creating initial set")
	graphs := 0
	for _, v := range m.Graph.V {
		color := m.Graph.Colors[v.Color]
		if m.StartPrefix != "" && !strings.HasPrefix(color, m.StartPrefix) {
			continue
		}
		freq := m.Graph.ColorFrequency(v.Color)
		if freq < m.Support || freq > m.MaxSupport {
			continue
		}
		send(m.Graph.SubGraph([]int{v.Idx}, nil))
		graphs += 1
	}
	log.Printf("Done creating initial set %d", graphs)
}

func vertexSet(sg *goiso.SubGraph) *set.SortedSet {
	s := getSet()
	for _, v := range sg.V {
		if err := s.Add(types.Int(v.Id)); err != nil {
			panic(err)
		}
	}
	return s
}

var sliceRecycler chan []*goiso.SubGraph
var setRecycler chan *set.SortedSet

func init() {
	sliceRecycler = make(chan []*goiso.SubGraph, 50)
	setRecycler = make(chan *set.SortedSet, 50)
}

func getSlice() []*goiso.SubGraph {
	select {
	case slice := <-sliceRecycler: return slice
	default: return make([]*goiso.SubGraph, 0, 100)
	}
}

func releaseSlice(slice []*goiso.SubGraph) {
	slice = slice[0:0]
	select {
	case sliceRecycler<-slice: return
	default: return
	}
}

func getSet() *set.SortedSet {
	select {
	case set := <-setRecycler: return set
	default: return set.NewSortedSet(100)
	}
}

func releaseSet(set *set.SortedSet) {
	set.Clear()
	select {
	case setRecycler<-set: return
	default: return
	}
}

func (m *Miner) nonOverlapping(sgs store.Iterator) []*goiso.SubGraph {
	vids := getSet()
	non_overlapping := getSlice()
	i := 0
	var sg *goiso.SubGraph
	for _, sg, sgs = sgs(); sgs != nil; _, sg, sgs = sgs() {
		if i > m.MaxSupport - 1 {
			// skip super big groups as nonOverlapping takes for ever
			return nil
		}
		s := vertexSet(sg)
		if !vids.Overlap(s) {
			non_overlapping = append(non_overlapping, sg)
			for v, next := s.Items()(); next != nil; v, next = next() {
				item := v.(types.Int)
				if err := vids.Add(item); err != nil {
					panic(err)
				}
			}
		}
		releaseSet(s)
		i++
	}
	releaseSet(vids)
	return non_overlapping
}

func (m *Miner) filterAndExtend(N int, parts <-chan store.Iterator, send func(*goiso.SubGraph)) {
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go m.worker(parts, send, done)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(done)
}

func (m *Miner) worker(in <-chan store.Iterator, send func(*goiso.SubGraph), done chan<- bool) {
	for part := range in {
		m.do_filter(part, func(sg *goiso.SubGraph) {
			m.do_extend(sg, send)
		})
	}
	done <- true
}

func (m *Miner) do_filter(part store.Iterator, send func(*goiso.SubGraph)) {
	non_overlapping := m.nonOverlapping(part)
	if len(non_overlapping) >= m.Support {
		for _, sg := range non_overlapping {
			if len(sg.V) >= m.MinVertices {
				m.Report<-sg
			}
			send(sg)
		}
	}
	releaseSlice(non_overlapping)
}

func (m *Miner) do_extend(sg *goiso.SubGraph, send func(*goiso.SubGraph)) {
	for _, v := range sg.V {
		// v.Idx is the index on the SubGraph
		// v.Id is the index on the original Graph
		for _, e := range m.Graph.Kids[v.Id] {
			if m.Graph.ColorFrequency(m.Graph.V[e.Targ].Color) < m.Support {
				continue
			}
			if m.VertexExtend {
				if !sg.HasVertex(e.Targ) {
					send(sg.Extend(e.Targ))
				}
			} else {
				if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
					send(sg.EdgeExtend(e))
				}
			}
		}
		for _, e := range m.Graph.Parents[v.Id] {
			if m.Graph.ColorFrequency(m.Graph.V[e.Src].Color) < m.Support {
				continue
			}
			if m.VertexExtend {
				if !sg.HasVertex(e.Src) {
					send(sg.Extend(e.Src))
				}
			} else {
				if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
					send(sg.EdgeExtend(e))
				}
			}
		}
	}
}

func makePartitions(sgs store.SubGraphs) (p_it partitionIterator) {
	keys := sgs.Keys()
	p_it = func() (part store.Iterator, next partitionIterator) {
		var key []byte
		key, keys = keys()
		if keys == nil {
			return nil, nil
		}
		return sgs.Find(key), p_it
	}
	return p_it
}

type Collectors struct {
	trees []store.SubGraphs
	chs []chan<- *labelGraph
}

type labelGraph struct {
	label []byte
	sg *goiso.SubGraph
}

func (m *Miner) collector(tree store.SubGraphs, in <-chan *labelGraph) {
	for lg := range in {
		{
			key := lg.label
			sg := lg.sg
			if len(key) < 0 {
				panic(fmt.Errorf("Key was a bad value %d %v %p\n%p", len(key), key, key, sg))
			}
			if sg == nil {
				panic(fmt.Errorf("sg was a nil %d %v %p\n%p", len(key), key, key, sg))
			}
			value := sg.Serialize()
			if len(value) < 0 {
				panic(fmt.Errorf("Could not serialize sg, %v\n%v\n%v", len(value), sg, value))
			}
		}
		if tree.Count(lg.label) > m.MaxSupport + 1 {
			continue
		}
		tree.Add(lg.label, lg.sg)
	}
}

func (m *Miner) makeCollectors(N int) *Collectors {
	trees := make([]store.SubGraphs, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	for i := 0; i < N; i++ {
		tree := m.MakeStore()
		ch := make(chan *labelGraph)
		trees = append(trees, tree)
		chs = append(chs, ch)
		go m.collector(tree, ch)
	}
	return &Collectors{trees, chs}
}

func (c *Collectors) close() {
	for _, ch := range c.chs {
		close(ch)
	}
}

func (c *Collectors) delete() {
	for _, bpt := range c.trees {
		bpt.Delete()
	}
}

func hash(bytes []byte) int {
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

func (c *Collectors) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator)
	go func() {
		for k, keys := c.keys()(); keys != nil; k, keys = keys() {
			out <- c.partitionIterator(k)
		}
		close(out)
	}()
	return out
}

func (c *Collectors) makeSend() func(*goiso.SubGraph) {
	next := 0
	return func(sg *goiso.SubGraph) {
		label := sg.ShortLabel()
		lg := &labelGraph{label, sg}
		bkt := hash(label) % len(c.chs)
		next = bkt
		{
			key := label
			if len(key) < 0 {
				panic(fmt.Errorf("Key was a bad value %d %v %p\n%p", len(key), key, key, sg))
			}
			if sg == nil {
				panic(fmt.Errorf("sg was a nil %d %v %p\n%p", len(key), key, key, sg))
			}
			value := sg.Serialize()
			if len(value) < 0 {
				panic(fmt.Errorf("Could not serialize sg, %v\n%v\n%v", len(value), sg, value))
			}
		}
		for i := 0; i < len(c.chs); i++ {
			select {
			case c.chs[next]<-lg:
				return
			default:
				next = (next + 1) % len(c.chs)
			}
		}
		c.chs[bkt]<-lg
	}
}

func (c *Collectors) keys() (kit store.BytesIterator) {
	its := make([]store.BytesIterator, len(c.trees))
	peek := make([][]byte, len(c.trees))
	for i, tree := range c.trees {
		its[i] = tree.Keys()
		peek[i], its[i] = its[i]()
	}
	getMin := func() int {
		min := 0
		for i := range peek {
			if peek[i] == nil {
				continue
			}
			if bytes.Compare(peek[i], peek[min]) < 0 {
				min = i
			}
		}
		return min
	}
	var last []byte = nil
	kit = func() (item []byte, _ store.BytesIterator) {
		item = last
		for bytes.Equal(item, last) {
			min := getMin()
			if peek[min] == nil {
				return nil, nil
			}
			item = peek[min]
			peek[min], its[min] = its[min]()
		}
		last = item
		return item, kit
	}
	return kit
}

func (c *Collectors) partitionIterator(key []byte) (pit store.Iterator) {
	its := make([]store.Iterator, len(c.trees))
	for i, tree := range c.trees {
		its[i] = tree.Find(key)
	}
	j := 0
	pit = func() (k []byte, sg *goiso.SubGraph, _ store.Iterator) {
		for j < len(its) {
			if its[j] == nil {
				j++
			} else {
				k, sg, its[j] = its[j]()
				if its[j] != nil {
					return k, sg, pit
				}
			}
		}
		return nil, nil, nil
	}
	return pit
}

func (c *Collectors) size() int {
	sum := 0
	for _, tree := range c.trees {
		sum += tree.Size()
	}
	return sum
}

