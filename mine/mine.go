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
	"log"
	"runtime"
)

import (
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/data-structures/tree/bptree"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
)

const TREESIZE = 127

func Mine(G *goiso.Graph, support, minpat int) <-chan *goiso.SubGraph {
	fsg := make(chan *goiso.SubGraph)
	sgs := cycle(initial(G, support), support)
	miner := func(sgs []*goiso.SubGraph) {
		for len(sgs) > 0 {
			log.Printf("Extending Subgraphs of Size %v", len(sgs[0].V))
			nsgs := bptree.NewBpTree(TREESIZE)
			snd := make(chan *goiso.SubGraph)
			rcv := make(chan *goiso.SubGraph)
			go extenders(snd, rcv, support)
			go func() {
				i := 0
				for _, sg := range sgs {
					if len(sg.V) > minpat {
						fsg <- sg
					}
					snd <- sg
					if i%1000 == 0 {
						log.Printf("extended %d", i)
					}
					i += 1
				}
				close(snd)
			}()
			for sg := range rcv {
				if err := nsgs.Add(types.String(sg.Label()), sg); err != nil {
					panic(err)
				}
			}
			log.Printf("extended size %d", nsgs.Size())
			log.Printf("Filtering Subgraphs of Size %v", len(sgs[0].V)+1)
			sgs = nil
			runtime.GC()
			sgs = cycle(nsgs, support)
			nsgs = nil
			runtime.GC()
		}
		close(fsg)
	}
	go miner(sgs)
	return fsg
}

func initial(G *goiso.Graph, support int) *bptree.BpTree {
	graphs := bptree.NewBpTree(TREESIZE)
	Initial(G, support, func(sg *goiso.SubGraph) {
		if err := graphs.Add(types.String(sg.Label()), sg); err != nil {
			panic(err)
		}
	})
	return graphs
}

func Initial(G *goiso.Graph, support int, send func(*goiso.SubGraph)) {
	log.Printf("Creating initial set")
	graphs := 0
	for _, v := range G.V {
		if G.ColorFrequency(v.Color) < support {
			continue
		}
		send(G.SubGraph([]int{v.Idx}, nil))
		graphs += 1
	}
	log.Printf("Done creating initial set %d", graphs)
}

func extenders(in <-chan *goiso.SubGraph, out chan<- *goiso.SubGraph, support int) {
	const N = 4
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go extend(in, out, done, support)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(out)
	close(done)
}

func extend(in <-chan *goiso.SubGraph, out chan<- *goiso.SubGraph, done chan<- bool, support int) {
	for sg := range in {
		do_extend(sg, support, func(ext *goiso.SubGraph) {
			out <- ext
		})
	}
	done <- true
}

func do_extend(sg *goiso.SubGraph, support int, send func(*goiso.SubGraph)) {
	for _, v := range sg.V {
		// v.Idx is the index on the SubGraph
		// v.Id is the index on the original Graph
		for _, e := range sg.G.Kids[v.Id] {
			if sg.G.ColorFrequency(sg.G.V[e.Targ].Color) < support {
				continue
			}
			if !sg.Has(e.Targ) {
				send(sg.Extend(e.Targ))
			}
		}
	}
}

func Extend(sg *goiso.SubGraph, support int) (extensions []*goiso.SubGraph) {
	do_extend(sg, support, func(ext *goiso.SubGraph) {
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

func filters(support int, in <-chan []*goiso.SubGraph, out chan<- *goiso.SubGraph) {
	log.Printf("creating filters")
	const N = 4
	done := make(chan bool)
	for i := 0; i < N; i++ {
		go filter(support, in, out, done)
	}
	for i := 0; i < N; i++ {
		<-done
	}
	close(out)
	close(done)
	log.Printf("done filtering")
}

func filter(support int, in <-chan []*goiso.SubGraph, out chan<- *goiso.SubGraph, done chan<- bool) {
	for part := range in {
		do_filter(part, support, func(sg *goiso.SubGraph) {
			out <- sg
		})
	}
	done <- true
}

func do_filter(part []*goiso.SubGraph, support int, send func(*goiso.SubGraph)) {
	if len(part) < support {
		return
	}
	part = nonOverlapping(part)
	if len(part) >= support {
		for _, sg := range part {
			send(sg)
		}
	}
}

func Filter(part []*goiso.SubGraph, support int) (filtered []*goiso.SubGraph) {
	do_filter(part, support, func(sg *goiso.SubGraph) {
		filtered = append(filtered, sg)
	})
	return filtered
}

func cycle(sgs *bptree.BpTree, support int) []*goiso.SubGraph {
	snd := make(chan []*goiso.SubGraph)
	rcv := make(chan *goiso.SubGraph)
	go filters(support, snd, rcv)
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
	filtered := make([]*goiso.SubGraph, 0, sgs.Size())
	for sg := range rcv {
		filtered = append(filtered, sg)
	}
	log.Printf("filtered size %d", len(filtered))
	return filtered
}

