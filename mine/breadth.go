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
	"io"
	"log"
	"runtime"
	"runtime/pprof"
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

type BreadthMiner struct {
	Graph *goiso.Graph
	SupportAttrs map[int]string
	VertexExtend bool
	LeftMostExtension bool
	Support int
	MinVertices int
	MaxSupport int
	MaxRounds int
	StartPrefix string
	SupportAttr string
	Report chan<- *goiso.SubGraph
	MakeStore func() store.SubGraphs
	MakeUnique func() store.UniqueIndex
}

func Breadth(
	G *goiso.Graph,
	supportAttrs map[int]string,
	support, maxSupport, minVertices, maxRounds int,
	startPrefix string,
	supportAttr string,
	vertexExtend, leftMost bool,
	makeStore func() store.SubGraphs,
	makeUnique func() store.UniqueIndex,
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
	m := &BreadthMiner{
		Graph: G,
		SupportAttrs: supportAttrs,
		Support: support,
		MaxSupport: maxSupport,
		MinVertices: minVertices,
		MaxRounds: maxRounds,
		StartPrefix: startPrefix,
		SupportAttr: supportAttr,
		VertexExtend: vertexExtend,
		LeftMostExtension: leftMost,
		Report: fsg,
		MakeStore: makeStore,
		MakeUnique: makeUnique,
	}
	var profMutex sync.Mutex
	miner := func() {
		p_it, collectors := m.initial()
		round := 1
		var pc Collectors
		for true {
			if pc != nil {
				pc.delete()
			}
			pc = collectors
			collectors = m.makeCollectors(CPUs*2)
			log.Printf("starting filtering %v", round)
			m.filterAndExtend(CPUs*4, p_it, collectors.send)
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

func (m *BreadthMiner) initial() (<-chan store.Iterator, Collectors) {
	CPUs := runtime.NumCPU()
	collectors := m.makeCollectors(CPUs)
	m.Initial(collectors.send)
	collectors.close()
	return collectors.partsCh(), collectors
}

func (m *BreadthMiner) Initial(send func(*goiso.SubGraph)) {
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


func (m *BreadthMiner) nonOverlapping(sgs store.Iterator) []*goiso.SubGraph {
	vids := getSet()
	non_overlapping := getSlice()
	i := 0
	var sg *goiso.SubGraph
	for _, sg, sgs = sgs(); sgs != nil; _, sg, sgs = sgs() {
		if i > m.MaxSupport - 1 {
			// skip super big groups as nonOverlapping takes for ever
			return nil
		}
		s := VertexSet(sg)
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

func (m *BreadthMiner) countSupport(sgs []*goiso.SubGraph) (support int) {
	if m.SupportAttr == "" {
		return len(sgs)
	}
	S := getSet()
	for _, sg := range sgs {
		has := false
		for _, v := range sg.V {
			a := types.String(m.SupportAttrs[v.Id])
			has = has || S.Has(a)
		}
		if !has {
			support++
			for _, v := range sg.V {
				a := types.String(m.SupportAttrs[v.Id])
				S.Add(a)
			}
		}
	}
	releaseSet(S)
	return support
}

func (m *BreadthMiner) filterAndExtend(N int, parts <-chan store.Iterator, send func(*goiso.SubGraph)) {
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go m.worker(parts, send, done)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(done)
}

func (m *BreadthMiner) worker(in <-chan store.Iterator, send func(*goiso.SubGraph), done chan<- bool) {
	for part := range in {
		m.do_filter(part, func(sg *goiso.SubGraph) {
			m.do_extend(sg, send)
		})
	}
	done <- true
}

func (m *BreadthMiner) do_filter(part store.Iterator, send func(*goiso.SubGraph)) {
	non_overlapping := m.nonOverlapping(part)
	if m.countSupport(non_overlapping) >= m.Support {
		for _, sg := range non_overlapping {
			if len(sg.V) >= m.MinVertices {
				m.Report<-sg
			}
			send(sg)
		}
	}
	releaseSlice(non_overlapping)
}

type vertexIterator func() (*goiso.Vertex, vertexIterator)

func leftMost(sg *goiso.SubGraph) (vi vertexIterator) {
	visited := set.NewSortedSet(len(sg.V))
	tree := make(map[int][]int, len(sg.V))
	var visit func(*goiso.Vertex)
	visit = func(v *goiso.Vertex) {
		visited.Add(types.Int(v.Idx))
		tree[v.Idx] = make([]int, 0, 5)
		for _, e := range sg.Kids[v.Idx] {
			if !visited.Has(types.Int(e.Targ)) {
				tree[v.Idx] = append(tree[v.Idx], e.Targ)
			}
		}
		for _, e := range sg.Kids[v.Idx] {
			if !visited.Has(types.Int(e.Targ)) {
				visit(&sg.V[e.Targ])
			}
		}
	}
	roots := make([]*goiso.Vertex, 0, 5)
	for i := range sg.V {
		v := &sg.V[i]
		if !visited.Has(types.Int(v.Idx)) {
			visit(v)
			roots = append(roots, v)
		}
	}
	i := 0
	vi = func() (left *goiso.Vertex, _ vertexIterator) {
		if i >= len(roots) {
			return nil, nil
		}
		left = roots[i]
		for len(tree[left.Idx]) > 0 {
			left = &sg.V[tree[left.Idx][0]]
		}
		i++
		return left, vi
	}
	return vi
}

func allVertices(sg *goiso.SubGraph) (vi vertexIterator) {
	i := 0
	vi = func() (v *goiso.Vertex, _ vertexIterator) {
		if i >= len(sg.V) {
			return nil, nil
		}
		v = &sg.V[i]
		i++
		return v, vi
	}
	return vi
}

func (m *BreadthMiner) do_extend(sg *goiso.SubGraph, send func(*goiso.SubGraph)) {
	var V vertexIterator
	if m.LeftMostExtension {
		V = leftMost(sg)
	} else {
		V = allVertices(sg)
	}
	for v, next := V(); next != nil; v, next = next() {
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
		if m.StartPrefix != "" {
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
}

type labelGraph struct {
	label []byte
	sg *goiso.SubGraph
}

func (m *BreadthMiner) collector(tree store.SubGraphs, in <-chan *labelGraph, done chan<- bool) {
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
	done<-true
}
