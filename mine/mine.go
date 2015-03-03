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
	"hash/fnv"
	"log"
	"runtime"
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
	Support int
	MinVertices int
	Report chan<- *goiso.SubGraph
	MakeStore func() store.SubGraphs
}

func Mine(G *goiso.Graph, support, minpat int, makeStore func() store.SubGraphs) <-chan *goiso.SubGraph {
	CPUs := runtime.NumCPU()
	fsg := make(chan *goiso.SubGraph)
	m := &Miner{Graph: G, Support: support, MinVertices: minpat, Report: fsg, MakeStore: makeStore}
	miner := func() {
		p_it := m.initial()
		round := 1
		for true {
			collectors := m.makeCollectors(CPUs)
			log.Printf("starting filtering %v", round)
			m.filterAndExtend(CPUs*4, p_it, func(sg *goiso.SubGraph) {
				collectors.send(sg)
			})
			collectors.close()
			size := collectors.size() 
			if size <= 0 {
				break
			}
			p_it = collectors.partsCh()
			runtime.GC()
			log.Printf("finished %v with %v", round, size)
			log.Printf("Number of goroutines = %v", runtime.NumGoroutine())
			round++
		}
		close(m.Report)
	}
	go miner()
	return fsg
}

func (m *Miner) initial() <-chan store.Iterator {
	CPUs := runtime.NumCPU()
	collectors := m.makeCollectors(CPUs)
	m.Initial(func(sg *goiso.SubGraph) {
		collectors.send(sg)
	})
	collectors.close()
	return collectors.partsCh()
}

func (m *Miner) Initial(send func(*goiso.SubGraph)) {
	log.Printf("Creating initial set")
	graphs := 0
	for _, v := range m.Graph.V {
		if m.Graph.ColorFrequency(v.Color) < m.Support {
			continue
		}
		send(m.Graph.SubGraph([]int{v.Idx}, nil))
		graphs += 1
	}
	log.Printf("Done creating initial set %d", graphs)
}

func vertexSet(sg *goiso.SubGraph) *set.SortedSet {
	s := set.NewSortedSet(len(sg.V))
	for _, v := range sg.V {
		if err := s.Add(types.Int(v.Id)); err != nil {
			panic(err)
		}
	}
	return s
}

func (m *Miner) nonOverlapping(sgs store.Iterator) []*goiso.SubGraph {
	vids := set.NewSortedSet(100)
	non_overlapping := make([]*goiso.SubGraph, 0, 100)
	i := 0
	var sg *goiso.SubGraph
	for _, sg, sgs = sgs(); sgs != nil; _, sg, sgs = sgs() {
		if i > m.Support*100 {
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
		i++
	}
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
			if len(sg.V) > m.MinVertices {
				m.Report<-sg
			}
			send(sg)
		}
	}
}

func (m *Miner) do_extend(sg *goiso.SubGraph, send func(*goiso.SubGraph)) {
	for _, v := range sg.V {
		// v.Idx is the index on the SubGraph
		// v.Id is the index on the original Graph
		for _, e := range m.Graph.Kids[v.Id] {
			if m.Graph.ColorFrequency(m.Graph.V[e.Targ].Color) < m.Support {
				continue
			}
			if !sg.Has(e.Targ) {
				send(sg.Extend(e.Targ))
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

func joinParts(pits []partitionIterator) (p_it partitionIterator) {
	remove := func(i int, pits []partitionIterator) []partitionIterator {
		for j := i; j+1 < len(pits); j++ {
			pits[j] = pits[j+1]
		}
		pits = pits[:len(pits)-1]
		return pits
	}
	i := 0
	p_it = func() (part store.Iterator, next partitionIterator) {
		part, pits[i] = pits[i]()
		for pits[i] == nil {
			pits = remove(i, pits)
			if len(pits) <= 0 {
				return nil, nil
			}
			i = i % len(pits)
			part, pits[i] = pits[i]()
		}
		i = (i + 1) % len(pits)
		return part, p_it
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

func collector(tree store.SubGraphs, in <-chan *labelGraph) {
	for lg := range in {
		tree.Add(lg.label, lg.sg)
	}
}

func (self *Miner) makeCollectors(N int) *Collectors {
	trees := make([]store.SubGraphs, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	for i := 0; i < N; i++ {
		tree := self.MakeStore()
		ch := make(chan *labelGraph)
		trees = append(trees, tree)
		chs = append(chs, ch)
		go collector(tree, ch)
	}
	return &Collectors{trees, chs}
}

func (c *Collectors) close() {
	for _, ch := range c.chs {
		close(ch)
	}
}

func hash(bytes []byte) int {
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

func (c *Collectors) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator)
	done := make(chan bool)
	for _, tree := range c.trees {
		go func(tree store.SubGraphs) {
			for part, next := makePartitions(tree)(); next != nil; part, next = next() {
				out <- part
			}
			done <- true
		}(tree)
	}
	go func() {
		for _ = range c.trees {
			<-done
		}
		close(out)
		close(done)
		/*
		time.Sleep(10 * time.Second)
		for _, bpt := range c.trees {
			bpt.Delete()
		}
		*/
	}()
	return out
}

func (c *Collectors) partitions() partitionIterator {
	parts := make([]partitionIterator, 0, len(c.trees))
	for _, tree := range c.trees {
		parts = append(parts, makePartitions(tree))
	}
	return joinParts(parts)
}

func (c *Collectors) send(sg *goiso.SubGraph) {
	label := sg.ShortLabel()
	idx := hash(label) % len(c.chs)
	c.chs[idx] <- &labelGraph{label, sg}
}

func (c *Collectors) size() int {
	sum := 0
	for _, tree := range c.trees {
		sum += tree.Size()
	}
	return sum
}

