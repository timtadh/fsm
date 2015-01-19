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
	"io"
	"log"
	"runtime"
	"runtime/pprof"
)

import (
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/data-structures/tree/bptree"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
)

const TREESIZE = 127

type Miner struct {
	Graph *goiso.Graph
	Support int
	MinVertices int
	Report chan<- *goiso.SubGraph
}

func Mine(G *goiso.Graph, support, minpat int, mprof io.Writer) <-chan *goiso.SubGraph {
	fsg := make(chan *goiso.SubGraph)
	m := &Miner{Graph: G, Support: support, MinVertices: minpat, Report: fsg}

	partitioner := func(nsgs *bptree.BpTree, out chan<- *goiso.SubGraph) {
		m.partitionAndFilter(nsgs, func(sg *goiso.SubGraph) {
			out<-sg
		})
		close(out)
	}

	miner := func(nsgs *bptree.BpTree) {
		for true {
			filtered := make(chan *goiso.SubGraph)
			go partitioner(nsgs, filtered)
			nsgs = bptree.NewBpTree(TREESIZE)
			runtime.GC()
			count := 0
			m.extendAll(filtered, func(sg *goiso.SubGraph) {
				if err := nsgs.Add(types.String(sg.Label()), sg); err != nil {
					panic(err)
				}
				count++
			})
			if count <= 0 {
				break
			}
			pprof.WriteHeapProfile(mprof)
			runtime.GC()
		}
		close(m.Report)
	}
	go miner(m.initial())
	return fsg
}

func (m *Miner) initial() *bptree.BpTree {
	graphs := bptree.NewBpTree(TREESIZE)
	m.Initial(func(sg *goiso.SubGraph) {
		if err := graphs.Add(types.String(sg.Label()), sg); err != nil {
			panic(err)
		}
	})
	return graphs
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

func (m *Miner) extendAll(in <-chan *goiso.SubGraph, send func(*goiso.SubGraph)) {
	rcv := make(chan *goiso.SubGraph)
	go m.extenders(in, rcv)
	i := 0
	for sg := range rcv {
		send(sg)
		if i%1000 == 0 {
			log.Printf("extended %d", i)
		}
		i++
	}
}

func (m *Miner) extenders(in <-chan *goiso.SubGraph, out chan<- *goiso.SubGraph) {
	const N = 4
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go m.extend(in, out, done)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(out)
	close(done)
}

func (m *Miner) extend(in <-chan *goiso.SubGraph, out chan<- *goiso.SubGraph, done chan<- bool) {
	for sg := range in {
		m.do_extend(sg, func(ext *goiso.SubGraph) {
			out <- ext
		})
	}
	done <- true
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

func (m *Miner) Extend(sg *goiso.SubGraph) (extensions []*goiso.SubGraph) {
	m.do_extend(sg, func(ext *goiso.SubGraph) {
		extensions = append(extensions, ext)
	})
	return extensions
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

func nonOverlapping(sgs []*goiso.SubGraph) []*goiso.SubGraph {
	log.Printf("computing non-overlapping %d", len(sgs))
	vids := set.NewSortedSet(len(sgs))
	non_overlapping := make([]*goiso.SubGraph, 0, len(sgs))
	for _, sg := range sgs {
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
	}
	log.Printf("done computing non-overlapping (%d) %d -> %d", len(sgs[0].V), len(sgs), len(non_overlapping))
	return non_overlapping
}

func (m *Miner) filters(in <-chan []*goiso.SubGraph, out chan<- *goiso.SubGraph) {
	log.Printf("creating filters")
	const N = 4
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go m.filter(in, out, done)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(out)
	close(done)
	log.Printf("done filtering")
}

func (m *Miner) filter(in <-chan []*goiso.SubGraph, out chan<- *goiso.SubGraph, done chan<- bool) {
	for part := range in {
		m.do_filter(part, func(sg *goiso.SubGraph) {
			out <- sg
		})
	}
	done <- true
}

func (m *Miner) do_filter(part []*goiso.SubGraph, send func(*goiso.SubGraph)) {
	if len(part) < m.Support {
		return
	}
	part = nonOverlapping(part)
	if len(part) >= m.Support {
		for _, sg := range part {
			send(sg)
			if len(sg.V) >= m.MinVertices {
				m.Report<-sg
			}
		}
	}
}

func (m *Miner) Filter(part []*goiso.SubGraph) (filtered []*goiso.SubGraph) {
	m.do_filter(part, func(sg *goiso.SubGraph) {
		filtered = append(filtered, sg)
	})
	return filtered
}

func (m *Miner) partitionAndFilter(sgs *bptree.BpTree, send func(*goiso.SubGraph)) {
	snd := make(chan []*goiso.SubGraph)
	rcv := make(chan *goiso.SubGraph)
	go m.filters(snd, rcv)
	go func() {
		for k, next := sgs.Keys()(); next != nil; k, next = next() {
			key := k.(types.String)
			var part []*goiso.SubGraph
			for _, v, next := sgs.Range(key, key)(); next != nil; _, v, next = next() {
				sg := v.(*goiso.SubGraph)
				part = append(part, sg)
			}
			log.Printf("filtering partition of size %d", len(part))
			snd <- part
		}
		close(snd)
	}()
	for sg := range rcv {
		send(sg)
	}
}

