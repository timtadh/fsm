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
	"sort"
)

import (
	"github.com/timtadh/goiso"
	"github.com/timtadh/data-structures/tree/bptree"
	"github.com/timtadh/data-structures/types"
)

type byFirstId []*goiso.SubGraph

func (self byFirstId) Len() int           { return len(self) }
func (self byFirstId) Swap(i, j int)      { self[i], self[j] = self[j], self[i] }
func (self byFirstId) Less(i, j int) bool { return self[i].V[0].Id < self[j].V[0].Id }

func Mine(G *goiso.Graph, support int) (<-chan *goiso.SubGraph) {
	fsg := make(chan *goiso.SubGraph)
	sgs := Cycle(Initial(G), support)
	miner := func(sgs []*goiso.SubGraph) {
		for len(sgs) > 0 {
			nsgs := make([]*goiso.SubGraph, 0, len(sgs))
			for _, sg := range sgs {
				if len(sg.V) > 1 {
					fsg <- sg
				}
				if nsg := Extend(sg); nsg != nil {
					nsgs = append(nsgs, nsg)
				}
			}
			sgs = Cycle(nsgs, support)
		}
		close(fsg)
	}
	go miner(sgs)
	return fsg
}

func Initial(G *goiso.Graph) []*goiso.SubGraph {
	var graphs []*goiso.SubGraph
	for _, v := range G.V {
		graphs = append(graphs, G.SubGraph([]int{v.Idx}, nil))
	}
	sort.Sort(byFirstId(graphs))
	return graphs
}

func Extend(sg *goiso.SubGraph) (*goiso.SubGraph) {
	for _, v := range sg.V {
		// v.Idx is the index on the SubGraph
		// v.Id is the index on the original Graph
		for _, e := range sg.G.Kids[v.Id] {
			if !sg.Has(e.Targ) {
				nsg := sg.Extend(e.Targ)
				return nsg
			}
		}
	}
	return nil
}

func Support(sgs []*goiso.SubGraph) int {
	roots := make(map[int]bool)
	for _, sg := range sgs {
		roots[sg.V[0].Id] = true
	}
	return len(roots)
}

func Filter(support int, partition [][]*goiso.SubGraph) []*goiso.SubGraph {
	var filtered []*goiso.SubGraph
	for _, part := range partition {
		if Support(part) >= support {
			filtered = append(filtered, part...)
		}
	}
	return filtered
}

func Cycle(sgs []*goiso.SubGraph, support int) []*goiso.SubGraph {
	graphs := bptree.NewBpTree(18)
	for _, sg := range sgs {
		if err := graphs.Add(types.String(sg.Label()), sg); err != nil {
			panic(err)
		}
	}
	var partition [][]*goiso.SubGraph
	keys := make(map[string]bool)
	for k, next := graphs.Keys()(); next != nil; k, next = next() {
		key := string(k.(types.String))
		keys[key] = true
	}
	for k, _ := range keys {
		key := types.String(k)
		var part []*goiso.SubGraph
		for _, v, next := graphs.Range(key,key)(); next != nil; _, v, next = next() {
			sg := v.(*goiso.SubGraph)
			part = append(part, sg)
		}
		partition = append(partition, part)
	}
	filtered := Filter(support, partition)
	sort.Sort(byFirstId(filtered))
	return filtered
}

