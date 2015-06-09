package mine

import (
	"encoding/binary"
	"io"
	"log"
	"math/rand"
	"os"
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
	"github.com/timtadh/fsm/store"
)

func init() {
	if urandom, err := os.Open("/dev/urandom"); err != nil {
		panic(err)
	} else {
		seed := make([]byte, 8)
		if _, err := urandom.Read(seed); err == nil {
			rand.Seed(int64(binary.BigEndian.Uint64(seed)))
		}
		urandom.Close()
	}
}

type DepthMiner struct {
	Graph *goiso.Graph
	Support int
	MaxSupport int
	MinVertices int
	MaxQueueSize int
	Report chan *goiso.SubGraph
	MakeStore func() store.SubGraphs
	seen store.SubGraphs
}

func Depth(
	G *goiso.Graph,
	support, maxSupport, minVertices, maxQueueSize int,
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
		MaxQueueSize: maxQueueSize,
		Report: make(chan *goiso.SubGraph),
		MakeStore: makeStore,
		seen: makeStore(),
	}

	go m.mine()

	return m.Report
}

type partition []*goiso.SubGraph
type labeledPartition struct {
	label []byte
	part partition
}

func (m *DepthMiner) mine() {
	m.search(m.MaxQueueSize)
	close(m.Report)
}

func (m *DepthMiner) initial() <-chan partition {
	exts := m.MakeStore()
	for i := range m.Graph.V {
		v := &m.Graph.V[i]
		if m.Graph.ColorFrequency(v.Color) > m.Support && m.Graph.ColorFrequency(v.Color) < m.MaxSupport {
			sg := m.Graph.SubGraph([]int{v.Idx}, nil)
			label := sg.ShortLabel()
			exts.Add(label, sg)
		}
	}
	return m.partition(exts)
}

func (m *DepthMiner) search(N int) {
	log.Println("Max Queue Size", N)
	queue := make([]*labeledPartition, 0, N)
	initial := m.initial()
	addInitial := func() {
		for part := range initial {
			s := m.support(part)
			if s > m.Support && s < m.MaxSupport {
				queue = append(queue, &labeledPartition{part[0].ShortLabel(), part})
				break
			}
		}
	}
	addInitial()
	i := 0
	for len(queue) > 0 {
		var item *labeledPartition
		item, queue = takeOne(queue)
		if i % 1000 == 0 {
			log.Println("process:", len(queue), len(item.part), item.part[0].Label())
		}
		m.process(item, func(lp *labeledPartition) {
			for len(queue) > N {
				if rand.Int() % 2 == 0 {
					_, queue = takeOne(queue)
				} else {
					return
				}
			}
			queue = append(queue, lp)
		})
		if len(queue) == 0 {
			addInitial()
		}
		i++
	}
}

func takeOne(queue []*labeledPartition) (*labeledPartition, []*labeledPartition) {
	i := rand.Intn(len(queue))
	item := queue[i]
	copy(queue[i:], queue[i+1:])
	queue = queue[:len(queue)-1]
	return item, queue
}

func (m *DepthMiner) process(lp *labeledPartition, send func(*labeledPartition)) {
	if m.seen.Has(lp.label) {
		return
	}
	m.seen.Add(lp.label, lp.part[0])
	if len(lp.part[0].V) > m.MinVertices {
		for _, sg := range lp.part {
			m.Report <- sg
		}
	}
	for extended := range m.partition(m.extensions(lp.part)) {
		extended = m.nonOverlapping(extended)
		if len(extended) <= 0 {
			continue
		}
		label := extended[0].ShortLabel()
		s := m.support(extended)
		if s > m.Support && s < m.MaxSupport && !m.seen.Has(label) {
			send(&labeledPartition{label, extended})
		}
	}
}

func (m *DepthMiner) extensions(sgs []*goiso.SubGraph) store.SubGraphs {
	exts := m.MakeStore()
	for u, next := leftMost(sgs[0])(); next != nil; u, next = next() {
		for _, sg := range sgs {
			v := sg.V[u.Idx]
			for _, e := range m.Graph.Kids[v.Id] {
				targColor := m.Graph.V[e.Targ].Color
				if m.Graph.ColorFrequency(targColor) < m.Support {
					continue
				}
				if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
					esg := sg.EdgeExtend(e)
					label := esg.ShortLabel()
					if m.seen.Has(label) {
						continue
					}
					if exts.Count(label) > m.MaxSupport {
						continue
					}
					exts.Add(label, esg)
				}
			}
		}
	}
	return exts
}

func (m *DepthMiner) partition(sgs store.SubGraphs) <-chan partition {
	ch := make(chan partition)
	go func() {
		for key, keys := sgs.Keys()(); keys != nil; key, keys = keys() {
			part := make(partition, 0, sgs.Count(key))
			for _, sg, next := sgs.Find(key)(); next != nil; _, sg, next = next() {
				part = append(part, sg)
			}
			ch<-part
		}
		close(ch)
		sgs.Delete()
	}()
	return ch
}

func (m *DepthMiner) support(sgs partition) int {
	return len(sgs)
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
