package mine

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"runtime"
	"sort"
	"time"
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/goiso"
	"github.com/timtadh/fsm/store"
	"github.com/timtadh/matrix"
)


type RandomWalkMiner struct {
	Graph *goiso.Graph
	Support int
	MinVertices int
	SampleSize int
	PLevel int
	AllReport, MaxReport chan *goiso.SubGraph
}

type isoGroupWithSet struct {
	sg *goiso.SubGraph
	vertices *set.SortedSet
}

type sortableIsoGroup []*isoGroupWithSet
func (s sortableIsoGroup) Len() int { return len(s) }
func (s sortableIsoGroup) Less(i, j int) bool { return s[i].vertices.Less(s[j].vertices) }
func (s sortableIsoGroup) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func RandomWalk(
	G *goiso.Graph,
	support, minVertices, sampleSize int,
	memProf io.Writer,
) (
	m *RandomWalkMiner,
) {
	m = &RandomWalkMiner{
		Graph: G,
		Support: support,
		MinVertices: minVertices,
		SampleSize: sampleSize,
		PLevel: runtime.NumCPU(),
		AllReport: make(chan *goiso.SubGraph),
		MaxReport: make(chan *goiso.SubGraph),
	}
	go m.sample(sampleSize)
	return m
}

func (m *RandomWalkMiner) SelectionProbability(sg *goiso.SubGraph) (float64, error) {
	lattice := sg.Lattice()
	vp, p := m.probabilities(lattice)
	if len(sg.V) == 1 {
		return 1.0/float64(vp), nil
	}
	Q := matrix.Zeros(len(lattice.V)-1, len(lattice.V)-1)
	I := matrix.Eye(Q.Rows())
	R := matrix.Zeros(Q.Rows(), 1)
	u := matrix.Zeros(1, Q.Cols())
	for i, x := range lattice.V {
		if len(x.V) == 1 && len(x.E) == 0 && i < u.Cols() {
			u.Set(0, i, 1.0/float64(vp))
		}
	}
	for _, e := range lattice.E {
		if e.Targ >= Q.Cols() {
			R.Set(e.Src, 0, 1.0/float64(p[e.Src]))
		} else {
			Q.Set(e.Src, e.Targ, 1.0/float64(p[e.Src]))
		}
	}
	IQ, err := I.Minus(Q)
	if err != nil {
		log.Fatal(err)
	}
	N := matrix.Inverse(IQ)
	B, err := N.Times(R)
	if err != nil {
		log.Fatal(err)
	}
	P, err := u.Times(B)
	if err != nil {
		log.Fatal(err)
	}
	if P.Rows() != P.Cols() && P.Rows() != 1 {
		log.Fatal("Unexpected P shape", P.Rows(), P.Cols())
	}
	x := P.Get(0, 0)
	if x > 1.0 || x != x {
		return 0, fmt.Errorf("could not accurately compute p")
	}
	return x, nil
}

func (m *RandomWalkMiner) probabilities(lattice *goiso.Lattice) (int, []int) {
	P := make([]int, len(lattice.V))
	embeddings := m.initial()
	startingPoints := 0
	for _, next := embeddings.keys()(); next != nil; _, next = next() {
		startingPoints++
	}
	// log.Println(startingPoints, "start")
	for i, sg := range lattice.V {
		key := sg.ShortLabel()
		part := m.partition(key, embeddings)
		// This is incorrect, I am doing multiple extensions of the SAME graph
		// ending up with wierdness. I need to make sure I only extend a graph
		// ONCE.
		keys := m.extendInto(part, embeddings)
		count := 0
		maxSup := 0
		for k, next := keys.Items()(); next != nil; k, next = next() {
			p := m.partition([]byte(k.(types.ByteSlice)), embeddings)
			if len(p) > maxSup {
				maxSup = len(p)
			}
			if len(p) >= m.Support - 2 {
				count++
			}
		}
		if i + 1 == len(lattice.V) {
			P[i] = -1
		} else if count == 0 {
			// this case can happen occasionally, we need to ensure the
			// absorbing node will still be reachable
			// log.Println("err", keys.Size(), maxSup, part[0].Label())
			P[i] = 1
		} else {
			P[i] = count
			// log.Println(P[i], part[0].Label())
		}
	}
	embeddings.close()
	embeddings.delete()
	return startingPoints, P
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
	for len(next) >= m.Support {
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
	return groups
}

func (m *RandomWalkMiner) extendInto(sgs []*goiso.SubGraph, exts *Collectors) *set.SortedSet {
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
		add := func(sg *goiso.SubGraph, e *goiso.Edge) {
			if m.Graph.ColorFrequency(e.Color) < m.Support {
				return
			} else if m.Graph.ColorFrequency(m.Graph.V[e.Src].Color) < m.Support {
				return
			} else if m.Graph.ColorFrequency(m.Graph.V[e.Targ].Color) < m.Support {
				return
			}
			if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
				extend<-extension{sg, e}
			}
		}
		for u, next := allVertices(sgs[0])(); next != nil; u, next = next() {
			for _, sg := range sgs {
				if u.Idx >= len(sg.V) {
					continue
				}
				v := sg.V[u.Idx]
				for _, e := range m.Graph.Kids[v.Id] {
					add(sg, e)
				}
				for _, e := range m.Graph.Parents[v.Id] {
					add(sg, e)
				}
			}
		}
		close(extend)
	}()
	keys := set.NewSortedSet(10)
	for esg := range extended {
		keys.Add(types.ByteSlice(esg.ShortLabel()))
		exts.send(esg)
	}
	return keys
}

func (m *RandomWalkMiner) extensions(sgs []*goiso.SubGraph) *Collectors {
	exts := m.makeCollectors(m.PLevel)
	m.extendInto(sgs, exts)
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
	return m.partition(key, c)
}

func (m *RandomWalkMiner) randomInitialPartition(c *Collectors) partition {
	keys := make([][]byte, 0, 10)
	for key, next := c.keys()(); next != nil; key, next = next() {
		keys = append(keys, key)
	}
	key := keys[rand.Intn(len(keys))]
	return m.partition(key, c)
}

func (m *RandomWalkMiner) partition(key []byte, c *Collectors) partition {
	part := make(partition, 0, 10)
	for _, e, next := c.partitionIterator(key)(); next != nil; _, e, next = next() {
		part = append(part, e)
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
	group := make(sortableIsoGroup, 0, len(sgs))
	for _, sg := range sgs {
		group = append(group, &isoGroupWithSet{
			sg: sg,
			vertices: vertexSet(sg),
		})
	}
	sort.Sort(group)
	vids := set.NewSortedSet(10)
	non_overlapping := make(partition, 0, len(sgs))
	for _, sg := range group {
		s := sg.vertices
		if !vids.Overlap(s) {
			non_overlapping = append(non_overlapping, sg.sg)
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

