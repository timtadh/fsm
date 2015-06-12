package mine

import (
	"encoding/binary"
	"io"
	"log"
	"math/rand"
	"os"
)

import (
	"github.com/antzucaro/matchr"
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
	seenLabels [][]byte
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
		Report: make(chan *goiso.SubGraph), MakeStore: makeStore,
		seen: makeStore(),
		seenLabels: make([][]byte, 0, 1000),
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
		if m.Graph.ColorFrequency(v.Color) >= m.Support && m.Graph.ColorFrequency(v.Color) < m.MaxSupport {
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
			if s >= m.Support && s < m.MaxSupport {
				queue = append(queue, &labeledPartition{part[0].ShortLabel(), part})
				break
			}
		}
	}
	addInitial()
	i := 0
	for len(queue) > 0 {
		var item *labeledPartition
		item, queue = m.takeOne(queue)
		// if i % 100 == 0 {
			log.Println("process:", i, m.seen.Size(), len(queue), len(item.part), item.part[0].Label())
		// }
		m.process(item, func(lp *labeledPartition) {
			queue = append(queue, lp)
			for len(queue) > N {
				queue = m.dropOne(queue)
			}
		})
		if len(queue) == 0 {
			addInitial()
		}
		i++
	}
}

func (m *DepthMiner) takeOne(queue []*labeledPartition) (*labeledPartition, []*labeledPartition) {
	i, _ := max(sample(10, len(queue)), func(i int) int { return m.score(queue[i], queue) })
	return m.takeAt(queue, i)
}

func (m *DepthMiner) dropOne(queue []*labeledPartition) ([]*labeledPartition) {
	i, _ := min(sample(10, len(queue)), func(i int) int { return m.score(queue[i], queue) })
	_, queue = m.takeAt(queue, i)
	return queue
}

func (m *DepthMiner) takeAt(queue []*labeledPartition, i int) (*labeledPartition, []*labeledPartition) {
	item := queue[i]
	copy(queue[i:], queue[i+1:])
	queue = queue[:len(queue)-1]
	return item, queue
}

func min(items []int, f func(item int) int) (arg, min int) {
	arg = -1
	for _, i := range items {
		d := f(i)
		if d < min || arg < 0 {
			min = d
			arg = i
		}
	}
	return arg, min
}

func max(items []int, f func(item int) int) (arg, max int) {
	arg = -1
	for _, i := range items {
		d := f(i)
		if d > max || arg < 0 {
			max = d
			arg = i
		}
	}
	return arg, max
}

func sample(size, populationSize int) (sample []int) {
	if size >= populationSize {
		sample = make([]int, 0, populationSize)
		for i := 0; i < populationSize; i++ {
			sample = append(sample, i)
		}
		return sample
	}
	in := func(x int, items []int) bool {
		for _, y := range items {
			if x == y {
				return true
			}
		}
		return false
	}
	sample = make([]int, 0, size)
	for i := 0; i < size; i++ {
		j := rand.Intn(populationSize)
		for in(j, sample) {
			j = rand.Intn(populationSize) 
		}
		sample = append(sample, j)
	}
	return sample
}

func (m *DepthMiner) score(item *labeledPartition, population []*labeledPartition) int {
	return rand.Intn(100)
}

func (m *DepthMiner) scoreA(item *labeledPartition, population []*labeledPartition) int {
	_, seenNN := min(sample(10, len(m.seenLabels)), func(i int) int {
		return matchr.Levenshtein(string(m.seenLabels[i]), string(item.label))
	})
	_, popNN := min(sample(10, len(population)), func(i int) int {
		return matchr.Levenshtein(string(population[i].label), string(item.label))
	})
	return (seenNN + (popNN/2))/len(item.label)
}

func (m *DepthMiner) process(lp *labeledPartition, send func(*labeledPartition)) {
	if m.seen.Has(lp.label) {
		return
	}
	m.seen.Add(lp.label, lp.part[0])
	m.seenLabels = append(m.seenLabels, lp.label)
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
		if s >= m.Support && s < m.MaxSupport && !m.seen.Has(label) {
			send(&labeledPartition{label, extended})
		}
	}
}

func (m *DepthMiner) extensions(sgs []*goiso.SubGraph) store.SubGraphs {
	type extension struct {
		sg *goiso.SubGraph
		e *goiso.Edge
	}
	extend := make(chan extension)
	extended := make(chan *goiso.SubGraph)
	done := make(chan bool)
	WORKERS := 10
	for i := 0; i < WORKERS; i++ {
		go func() {
			for ext := range extend {
				// log.Println("to extend", ext)
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
		for u, next := leftMost(sgs[0])(); next != nil; u, next = next() {
			for _, sg := range sgs {
				v := sg.V[u.Idx]
				for _, e := range m.Graph.Kids[v.Id] {
					if m.Graph.ColorFrequency(m.Graph.V[e.Targ].Color) < m.Support {
						continue
					}
					if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
						// log.Println("would have extended", e)
						extend<-extension{sg, e}
					}
				}
				for _, e := range m.Graph.Parents[v.Id] {
					if m.Graph.ColorFrequency(m.Graph.V[e.Src].Color) < m.Support {
						continue
					}
					if !sg.HasEdge(goiso.ColoredArc{e.Arc, e.Color}) {
						// log.Println("would have extended", e)
						extend<-extension{sg, e}
					}
				}
			}
		}
		close(extend)
	}()
	// log.Println("computing extensions of ", sgs[0].Label())
	exts := m.MakeStore()
	for esg := range extended {
		// log.Println(esg.Label())
		label := esg.ShortLabel()
		if m.seen.Has(label) {
			continue
		}
		if exts.Count(label) > m.MaxSupport {
			continue
		}
		exts.Add(label, esg)
	}
	// log.Println("finished computing extensions of ", sgs[0].Label())
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
