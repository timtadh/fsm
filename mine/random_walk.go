package mine

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"runtime/debug"
	"sort"
)

import (
	"github.com/timtadh/data-structures/hashtable"
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
	StoreMaker func() store.SubGraphs
	startingPoints *set.SortedSet
	allEmbeddings *Collectors
	extended *hashtable.LinearHash
	supportedExtensions *hashtable.LinearHash
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
	StoreMaker func() store.SubGraphs,
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
		StoreMaker: StoreMaker,
		extended: hashtable.NewLinearHash(),
		supportedExtensions: hashtable.NewLinearHash(),
	}
	go m.sample(sampleSize)
	return m
}

type SparseEntry struct {
	Row, Col int
	Value float64
	Inverse int
}

type Sparse struct {
	Rows, Cols int
	Entries []*SparseEntry
}

func (m *RandomWalkMiner) PrMatrices(sg *goiso.SubGraph) (vp int, Q, R, u Sparse, err error) {
	defer func() {
		if e := recover(); e != nil {
			stack := string(debug.Stack())
			err = fmt.Errorf("%v\n%v", e, stack)
		}
	}()
	lattice := sg.Lattice()
	log.Printf("lattice size %d %v", len(lattice.V), sg.Label())
	p := m.probabilities(lattice)
	log.Println("got transistion probabilities", p)
	vp = m.startingPoints.Size()
	Q = Sparse{
		Rows: len(lattice.V)-1,
		Cols: len(lattice.V)-1,
		Entries: make([]*SparseEntry, 0, len(lattice.V)-1),
	}
	R = Sparse{
		Rows: len(lattice.V)-1,
		Cols: 1,
		Entries: make([]*SparseEntry, 0, len(lattice.V)-1),
	}
	u = Sparse{
		Rows: 1,
		Cols: len(lattice.V)-1,
		Entries: make([]*SparseEntry, 0, len(lattice.V)-1),
	}
	for i, x := range lattice.V {
		if len(x.V) == 1 && len(x.E) == 0 && i < len(lattice.V)-1 {
			u.Entries = append(u.Entries, &SparseEntry{0, i, 1.0/float64(vp), vp})
		}
	}
	for _, e := range lattice.E {
		if e.Targ >= len(lattice.V)-1 {
			R.Entries = append(R.Entries, &SparseEntry{e.Src, 0, 1.0/float64(p[e.Src]), p[e.Src]})
		} else {
			Q.Entries = append(Q.Entries, &SparseEntry{e.Src, e.Targ, 1.0/float64(p[e.Src]), p[e.Src]})
		}
	}
	return vp, Q, R, u, nil
}

func (m *RandomWalkMiner) SelectionProbability(sg *goiso.SubGraph) (float64, error) {
	lattice := sg.Lattice()
	log.Printf("lattice size %d %v", len(lattice.V), sg.Label())
	p := m.probabilities(lattice)
	log.Println("got transistion probabilities", p)
	vp := m.startingPoints.Size()
	if len(sg.V) == 1 {
		return 1.0/float64(vp), nil
	}
	Q := matrix.Zeros(len(lattice.V)-1, len(lattice.V)-1)
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
	I := matrix.Eye(Q.Rows())
	IQ, err := I.Minus(Q)
	if err != nil {
		log.Fatal(err)
	}
	N := matrix.Inverse(IQ)
	// IQ := matrix.Eye(Q.Rows())
	// err := IQ.Add(Q)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// N := sumpow(Q, len(sg.E))
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

func sumpow(A *matrix.DenseMatrix, exponent int) *matrix.DenseMatrix {
	var An matrix.Matrix = matrix.Eye(A.Rows())
	var S matrix.Matrix = matrix.Zeros(A.Rows(), A.Cols())
	var err error
	for i := 0; i < exponent; i++ {
		An, err = An.Times(A)
		if err != nil {
			log.Fatal(err)
		}
		err = S.Add(An)
		if err != nil {
			log.Fatal(err)
		}
	}
	return S.DenseMatrix()
}

func (m *RandomWalkMiner) probabilities(lattice *goiso.Lattice) []int {
	P := make([]int, len(lattice.V))
	// log.Println(startingPoints, "start")
	for i, sg := range lattice.V {
		key := sg.ShortLabel()
		part := m.partition(key)
		// This is incorrect, I am doing multiple extensions of the SAME graph
		// ending up with wierdness. I need to make sure I only extend a graph
		// ONCE.
		keys := m.extensions(part)
		count := m.supportedKeys(key, keys).Size()
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
	return P
}

func (m *RandomWalkMiner) sample(size int) {
	WORKERS := 1
	if m.allEmbeddings == nil {
		m.allEmbeddings, m.startingPoints = m.initial()
	}
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
					part := m.walk()
					if len(part) < m.Support {
						log.Println("found mfsg but it did not have enough support", part[0].Label())
						continue
					} else if len(part[0].V) < m.MinVertices {
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
	close(m.AllReport)
	close(m.MaxReport)
}

func (m *RandomWalkMiner) walk() partition {
	node := m.randomInitialPartition()
	exts := m.extensions(node)
	log.Printf("start node (%v) (%d) %v", exts.Size(), len(node), node[0].Label())
	next := m.randomPartition(node[0].ShortLabel(), exts)
	for len(next) >= m.Support {
		node = next
		exts = m.extensions(node)
		log.Printf("cur node (%v) (%d) %v", exts.Size(), len(node), node[0].Label())
		next = m.randomPartition(node[0].ShortLabel(), exts)
		if len(next) >= m.Support && len(next[0].E) == len(node[0].E) {
			break
		}
	}
	return node
}

func (m *RandomWalkMiner) initial() (*Collectors, *set.SortedSet) {
	groups := m.makeCollectors(m.PLevel)
	for i := range m.Graph.V {
		v := &m.Graph.V[i]
		if m.Graph.ColorFrequency(v.Color) >= m.Support {
			sg := m.Graph.SubGraph([]int{v.Idx}, nil)
			groups.send(sg)
		}
	}
	startingPoints := set.NewSortedSet(10)
	for key, next := groups.keys()(); next != nil; key, next = next() {
		startingPoints.Add(types.ByteSlice(key))
	}
	return groups, startingPoints
}

func (m *RandomWalkMiner) extend(sgs []*goiso.SubGraph, send func(*goiso.SubGraph)) {
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
	for esg := range extended {
		send(esg)
	}
}

func (m *RandomWalkMiner) extensions(sgs []*goiso.SubGraph) *set.SortedSet {
	if len(sgs) == 0 {
		return set.NewSortedSet(10)
	}
	label := types.ByteSlice(sgs[0].ShortLabel())
	if m.extended.Has(label) {
		keys, err := m.extended.Get(label)
		if err != nil {
			log.Fatal(err)
		}
		return keys.(*set.SortedSet)
	}
	keys := set.NewSortedSet(10)
	m.extend(sgs, func(sg *goiso.SubGraph) {
		m.allEmbeddings.send(sg)
		keys.Add(types.ByteSlice(sg.ShortLabel()))
	})
	m.extended.Put(label, keys)
	return keys
}

func (m *RandomWalkMiner) supportedKeys(from []byte, keys *set.SortedSet) *set.SortedSet {
	key := types.ByteSlice(from)
	if m.supportedExtensions.Has(key) {
		supKeys, err := m.supportedExtensions.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		return supKeys.(*set.SortedSet)
	}
	keysCh := make(chan []byte)
	partKeys := make(chan []byte)
	done := make(chan bool)
	for i := 0; i < m.PLevel; i++ {
		go func() {
			for key := range keysCh {
				if len(m.getPartition(key)) >= m.Support {
					partKeys<-key
				}
			}
			done<-true
		}()
	}
	go func() {
		for k, next := keys.Items()(); next != nil; k, next = next() {
			keysCh<-[]byte(k.(types.ByteSlice))
		}
		close(keysCh)
	}()
	go func() {
		for i := 0; i < m.PLevel; i++ {
			<-done
		}
		close(partKeys)
		close(done)
	}()
	supKeys := set.NewSortedSet(10)
	for partKey := range partKeys {
		supKeys.Add(types.ByteSlice(partKey))
	}
	m.supportedExtensions.Put(key, supKeys)
	return supKeys
}

func (m *RandomWalkMiner) getPartition(key []byte) partition {
	part := make(partition, 0, 10)
	for _, sg, next := m.allEmbeddings.partitionIterator(key)(); next != nil; _, sg, next = next() {
		part = append(part, sg)
	}
	return m.nonOverlapping(part)
}

func (m *RandomWalkMiner) randomPartition(from []byte, keys *set.SortedSet) partition {
	supKeys := m.supportedKeys(from, keys)
	if supKeys.Size() <= 0 {
		return nil
	}
	key, err := supKeys.Random()
	if err != nil {
		log.Fatal(err)
	}
	return m.partition(key.(types.ByteSlice))
}

func (m *RandomWalkMiner) randomInitialPartition() partition {
	key, err := m.startingPoints.Random()
	if err != nil {
		log.Fatal(err)
	}
	return m.partition(key.(types.ByteSlice))
}

func (m *RandomWalkMiner) partition(key []byte) partition {
	part := make(partition, 0, 10)
	for _, e, next := m.allEmbeddings.partitionIterator(key)(); next != nil; _, e, next = next() {
		part = append(part, e)
	}
	return m.nonOverlapping(part)
}

func (m *RandomWalkMiner) makeCollectors(N int) *Collectors {
	trees := make([]store.SubGraphs, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		tree := m.StoreMaker()
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

