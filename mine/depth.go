package mine

import (
	"fmt"
	"io"
	"hash/fnv"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/goiso"
	"github.com/timtadh/fsm/store"
)


type DepthMiner struct {
	Graph *goiso.Graph
	Support int
	MaxSupport int
	MinVertices int
	Report chan *goiso.SubGraph
	MakeStore func() store.SubGraphs
}

func Depth(
	G *goiso.Graph,
	support, maxSupport, minVertices int,
	makeStore func() store.SubGraphs,
	memProf io.Writer,
) (
	<-chan *goiso.SubGraph,
) {
	m := &DepthMiner{
		Graph: G,
		Support: support,
		MaxSupport: maxSupport,
		MinVertices: minVertices,
		Report: make(chan *goiso.SubGraph),
		MakeStore: makeStore,
	}

	go m.mine()

	return m.Report
}

type partition []*goiso.SubGraph

type extension struct {
	srcIdx    int
	edgeColor int
	targColor int
}

var extensionSize int
func init() {
	extensionSize = int(reflect.TypeOf(extension{}).Size())
}

func (e *extension) Equals(other types.Equatable) bool {
	if o, ok := other.(*extension); ok {
		return *e == *o
	} else {
		return false
	}
}

func (e *extension) Less(other types.Sortable) bool {
	if o, ok := other.(*extension); ok {
		if e.srcIdx < o.srcIdx {
			return true
		} else if e.srcIdx > o.srcIdx {
			return false
		} else if e.edgeColor < o.edgeColor {
			return true
		} else if e.edgeColor > o.edgeColor {
			return false
		} else if e.targColor < o.targColor {
			return true
		}
		return false
	} else {
		return false
	}
}

func (e *extension) Hash() int {
	s := &reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(e)),
		Len: extensionSize,
		Cap: extensionSize,
	}
	bytes := *(*[]byte)(unsafe.Pointer(s))
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

func (m *DepthMiner) mine() {
	queue := make(chan partition)
	go m.initial(queue)
	part := m.nonOverlapping(<-queue)
	exts := m.extensions(part)
	for e, next := exts.Items()(); next != nil; e, next = next() {
		ext := e.(*extension)
		fmt.Println(ext)
		for _, sg := range m.extend(part, ext) {
			fmt.Println("  ", sg)
		}
	}
	close(m.Report)
}

func (m *DepthMiner) initial(queue chan<- partition) {
	max := 0
	maxFreq := 0
	for _, v := range m.Graph.V {
		i := v.Color
		freq := m.Graph.ColorFrequency(i)
		if freq > maxFreq {
			max = i
			maxFreq = freq
		}
	}
	partition := make(partition, 0, maxFreq)
	for _, v := range m.Graph.V {
		if v.Color != max {
			continue
		}
		partition = append(partition, m.Graph.SubGraph([]int{v.Idx}, nil))
	}
	queue <- partition
}

/*
func (m *DepthMiner) DFS(sgs partition) {
	visit := func(node) {
		visited.add(node)
		for kid in node.kids {
			if kid not in visited {
				visit(kid)
			}
		}
	}
	subgraphs := func(sg) {
		emit sg
		for node in sg.nodes {
			for ext in extentions(node) {
				if ext not in sg {
					subgraphs(ext)
				}
			}
		}
	}
}
*/

/*
func (m *DepthMiner) extensions(sg *goiso.SubGraph) []*extension {
	exts := make([]*extension, 0, 10)
	for _, v := range sg.V {
		for _, e := range m.Graph.Kids[v.Id] {
			targColor := m.Graph.V[e.Targ].Color
			if m.support(targColor) < m.Support {
				continue
			}
			if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
				exts = append(exts, &extension{
					srcIdx: v.Idx,
					edgeColor: e.Color,
					targColor: targColor,
				})
			}
		}
	}
	return exts
}
*/

func (m *DepthMiner) extensions(sgs []*goiso.SubGraph) *set.SortedSet {
	exts := set.NewSortedSet(len(sgs))
	for i := range sgs[0].V {
		for _, sg := range sgs {
			v := sg.V[i]
			for _, e := range m.Graph.Kids[v.Id] {
				targColor := m.Graph.V[e.Targ].Color
				if m.support(targColor) < m.Support {
					continue
				}
				if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
					exts.Add(&extension{
						srcIdx: v.Idx,
						edgeColor: e.Color,
						targColor: targColor,
					})
				}
			}
		}
	}
	return exts
}

func (m *DepthMiner) extend(sgs partition, ext *extension) partition {
	exts := make(partition, 0, len(sgs))
	for _, sg := range sgs {
		esg := m.extendOne(sg, ext)
		if esg != nil {
			exts = append(exts, esg)
		}
	}
	return exts
}

func (m *DepthMiner) extendOne(sg *goiso.SubGraph, ext *extension) *goiso.SubGraph {
	src := sg.V[ext.srcIdx]
	for _, e := range m.Graph.Kids[src.Id] {
		targColor := m.Graph.V[e.Targ].Color
		if e.Color == ext.edgeColor && targColor == ext.targColor {
			return sg.EdgeExtend(e)
		}
	}
	return nil
}

func (m *DepthMiner) support(color int) int {
	return m.Graph.ColorFrequency(color)
}

func (m *DepthMiner) supported(sgs partition) bool {
	sgs = m.nonOverlapping(sgs)
	return len(sgs) >= m.Support
}

func (m *DepthMiner) nonOverlapping(sgs partition) partition {
	vids := getSet()
	non_overlapping := make(partition, 0, len(sgs))
	for i, sg := range sgs {
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
