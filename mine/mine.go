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
	"github.com/timtadh/data-structures/tree/bptree"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
)

const TREESIZE = 237

type partitionIterator func() (part []*goiso.SubGraph, next partitionIterator)

type Miner struct {
	Graph *goiso.Graph
	Support int
	MinVertices int
	Report chan<- *goiso.SubGraph
}

func Mine(G *goiso.Graph, support, minpat int) <-chan *goiso.SubGraph {
	fsg := make(chan *goiso.SubGraph)
	m := &Miner{Graph: G, Support: support, MinVertices: minpat, Report: fsg}
	miner := func(p_it <-chan []*goiso.SubGraph) {
		for true {
			collectors := makeCollectors(4)
			m.filterAndExtend(p_it, func(sg *goiso.SubGraph) {
				collectors.send(sg)
			})
			collectors.close()
			if collectors.size() <= 0 {
				break
			}
			p_it = collectors.partsCh()
			runtime.GC()
		}
		close(m.Report)
	}
	go miner(m.initial())
	return fsg
}

func (m *Miner) initial() <-chan []*goiso.SubGraph {
	collectors := makeCollectors(4)
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

func nonOverlapping(sgs []*goiso.SubGraph) []*goiso.SubGraph {
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
	return non_overlapping
}

func (m *Miner) workers(in <-chan []*goiso.SubGraph, send func(*goiso.SubGraph)) {
	const N = 8
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go m.worker(in, send, done)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(done)
}

func (m *Miner) worker(in <-chan []*goiso.SubGraph, send func(*goiso.SubGraph), done chan<- bool) {
	for part := range in {
		m.do_filter(part, func(sg *goiso.SubGraph) {
			m.do_extend(sg, send)
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

func (m *Miner) filterAndExtend(parts <-chan []*goiso.SubGraph, send func(*goiso.SubGraph)) {
	m.workers(parts, send)
}

func makePartitions(sgs *bptree.BpTree) (p_it partitionIterator) {
	keys := sgs.Keys()
	p_it = func() (part []*goiso.SubGraph, next partitionIterator) {
		var k types.Equatable
		k, keys = keys()
		if keys == nil {
			return nil, nil
		}
		key := k.(types.ByteSlice)
		for _, v, next := sgs.Range(key, key)(); next != nil; _, v, next = next() {
			sg := v.(*goiso.SubGraph)
			part = append(part, sg)
		}
		return part, p_it
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
	p_it = func() (part []*goiso.SubGraph, next partitionIterator) {
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
	trees []*bptree.BpTree
	chs []chan<- *labelGraph
}

type labelGraph struct {
	label []byte
	sg *goiso.SubGraph
}

func collector(tree *bptree.BpTree, in <-chan *labelGraph) {
	for lg := range in {
		if err := tree.Add(types.ByteSlice(lg.label), lg.sg); err != nil {
			panic(err)
		}
	}
}

func makeCollectors(N int) *Collectors {
	trees := make([]*bptree.BpTree, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	for i := 0; i < N; i++ {
		tree := bptree.NewBpTree(TREESIZE)
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

func (c *Collectors) partsCh() <-chan []*goiso.SubGraph {
	out := make(chan []*goiso.SubGraph)
	done := make(chan bool)
	for _, tree := range c.trees {
		go func(tree *bptree.BpTree) {
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

