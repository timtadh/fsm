package main

import (
	"fmt"
)

import (
	"github.com/timtadh/goiso/bliss"
)

func main() {
	bliss.Graph(0, func(g *bliss.BlissGraph) {
		a := g.AddVertex(1)
		b := g.AddVertex(1)
		c := g.AddVertex(2)
		d := g.AddVertex(2)
		g.AddEdge(a, b)
		g.AddEdge(c, d)
		g.AddEdge(a, c)
		g.AddEdge(b, d)
		fmt.Println(g)
	})
}


