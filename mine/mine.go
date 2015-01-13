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
	"github.com/timtadh/fsm/graph"
)

func Mine(G *graph.Graph, support int) (<-chan *graph.Graph) {
	return nil
}

func initial(G *graph.Graph, out chan<- *graph.Graph) {
	for _, v := range G.V {
		out<-G.SubGraph([]int64{v.Id}, nil, nil)
	}
	close(out)
}

func extend(G *graph.Graph, in <-chan *graph.Graph, out chan<- *graph.Graph) {
	for sg := range in {
		for _, v := range sg.V {
			for _, u := range G.Kids(v) {
				if !sg.Has(u) {
					V := sg.Vertices()
					V = append(V, u.Id)
					out<-G.SubGraph(V, nil, nil)
				}
			}
		}
	}
	close(out)
}

