package mine

import (
	"io"
	"log"
	"math/rand"
	"runtime"
	"time"
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
	"github.com/timtadh/fsm/store"
)


type RandomWalkMiner struct {
	Graph *goiso.Graph
	Support int
	MinVertices int
	SampleSize int
	PLevel int
	AllReport, MaxReport chan *goiso.SubGraph
}

func RandomWalk(
	G *goiso.Graph,
	support, minVertices, sampleSize int,
	memProf io.Writer,
) (
	all, max <-chan *goiso.SubGraph,
) {
	m := &RandomWalkMiner{
		Graph: G,
		Support: support,
		MinVertices: minVertices,
		SampleSize: sampleSize,
		PLevel: runtime.NumCPU(),
		AllReport: make(chan *goiso.SubGraph),
		MaxReport: make(chan *goiso.SubGraph),
	}
	go m.sample(sampleSize)
	return m.AllReport, m.MaxReport
}

func (m *RandomWalkMiner) sample(size int) {
	WORKERS := 1
	initial := m.initial()
	done := make(chan bool)
	sample := make(chan int)
	go func() {
		for i := 0; i < size; i++ {
			sample<-i
		}
		close(sample)
	}()
	for i := 0; i < WORKERS; i++ {
		go func() {
			for _ = range sample {
				for {
					part := m.walk(initial)
					if len(part[0].V) < m.MinVertices {
						log.Println("found mfsg but it was too small", part[0].Label())
						continue
					}
					log.Println("found mfsg", part[0].Label())
					for _, sg := range part {
						m.MaxReport<-sg
					}
					break
				}
			}
			done<-true
		}()
	}
	for i := 0; i < WORKERS; i++ {
		<-done
	}
	close(done)
	initial.delete()
	close(m.AllReport)
	close(m.MaxReport)
}

func (m *RandomWalkMiner) walk(initial *Collectors) partition {
	node := m.randomInitialPartition(initial)
	exts := m.extensions(node)
	log.Printf("start node (%v) (%d) %v", exts.size(), len(node), node[0].Label())
	next := m.randomPartition(exts)
	for len(next) > 0 {
		node = next
		exts = m.extensions(node)
		log.Printf("cur node (%v) (%d) %v", exts.size(), len(node), node[0].Label())
		next = m.randomPartition(exts)
		exts.delete()
	}
	return node
}

func (m *RandomWalkMiner) initial() *Collectors {
	groups := m.makeCollectors(m.PLevel)
	for i := range m.Graph.V {
		v := &m.Graph.V[i]
		if m.Graph.ColorFrequency(v.Color) >= m.Support {
			sg := m.Graph.SubGraph([]int{v.Idx}, nil)
			groups.send(sg)
		}
	}
	groups.close()
	return groups
}

func (m *RandomWalkMiner) extensions(sgs []*goiso.SubGraph) *Collectors {
	type extension struct {
		sg *goiso.SubGraph
		e *goiso.Edge
	}
	extend := make(chan extension)
	extended := make(chan *goiso.SubGraph)
	done := make(chan bool)
	WORKERS := m.PLevel
	for i := 0; i < WORKERS; i++ {
		go func() {
			for ext := range extend {
				extended<-ext.sg.EdgeExtend(ext.e)
			}
			done <-true
		}()
	}
	go func() {
		for i := 0; i < WORKERS; i++ {
			<-done
		}
		close(extended)
		close(done)
	}()
	go func() {
		for u, next := allVertices(sgs[0])(); next != nil; u, next = next() {
			for _, sg := range sgs {
				if u.Idx >= len(sg.V) {
					continue
				}
				v := sg.V[u.Idx]
				for _, e := range m.Graph.Kids[v.Id] {
					if m.Graph.ColorFrequency(m.Graph.V[e.Targ].Color) < m.Support {
						continue
					}
					if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
						extend<-extension{sg, e}
					}
				}
				for _, e := range m.Graph.Parents[v.Id] {
					if m.Graph.ColorFrequency(m.Graph.V[e.Src].Color) < m.Support {
						continue
					}
					if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
						extend<-extension{sg, e}
					}
				}
			}
		}
		close(extend)
	}()
	exts := m.makeCollectors(m.PLevel)
	for esg := range extended {
		exts.send(esg)
	}
	runtime.Gosched()
	time.Sleep(1*time.Millisecond)
	exts.close()
	return exts
}

func (m *RandomWalkMiner) randomPartition(c *Collectors) partition {
	keys := make([][]byte, 0, 10)
	for key, next := c.keys()(); next != nil; key, next = next() {
		part := make(partition, 0, 10)
		for _, sg, next := c.partitionIterator(key)(); next != nil; _, sg, next = next() {
			part = append(part, sg)
		}
		part = m.nonOverlapping(part)
		if len(part) >= m.Support {
			keys = append(keys, key)
		}
	}
	// here we know the probability of the next hop for this node
	// we should cache it.
	if len(keys) <= 0 {
		return nil
	}
	key := keys[rand.Intn(len(keys))]
	part := make(partition, 0, 10)
	for _, sg, next := c.partitionIterator(key)(); next != nil; _, sg, next = next() {
		part = append(part, sg)
	}
	return m.nonOverlapping(part)
}

func (m *RandomWalkMiner) randomInitialPartition(c *Collectors) partition {
	keys := make([][]byte, 0, 10)
	for key, next := c.keys()(); next != nil; key, next = next() {
		keys = append(keys, key)
	}
	key := keys[rand.Intn(len(keys))]
	part := make(partition, 0, 10)
	for _, sg, next := c.partitionIterator(key)(); next != nil; _, sg, next = next() {
		part = append(part, sg)
	}
	return m.nonOverlapping(part)
}

func (m *RandomWalkMiner) makeCollectors(N int) *Collectors {
	trees := make([]store.SubGraphs, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		tree := store.AnonFs2BpTree(m.Graph)
		ch := make(chan *labelGraph, 1)
		trees = append(trees, tree)
		chs = append(chs, ch)
		go m.collector(tree, ch, done)
	}
	return &Collectors{trees, chs, done}
}

func (m *RandomWalkMiner) collector(tree store.SubGraphs, in <-chan *labelGraph, done chan bool) {
	for lg := range in {
		tree.Add(lg.label, lg.sg)
	}
	done<-true
}

func (m *RandomWalkMiner) nonOverlapping(sgs partition) partition {
	vids := getSet()
	non_overlapping := make(partition, 0, len(sgs))
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
		releaseSet(s)
	}
	releaseSet(vids)
	return non_overlapping
}

