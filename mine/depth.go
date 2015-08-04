package mine

import (
	"fmt"
	"io"
	"log"
)

import (
)

import (
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
	"github.com/timtadh/fsm/store"
)

type Scorer interface {
	Score([]byte, Samplable) float64
	Kernel(Samplable, []int) Kernel
}

type partition []*goiso.SubGraph

type isoGroup struct {
	label []byte
	part partition
}

type DepthMiner struct {
	Graph *goiso.Graph
	ScoreName string
	Scorer Scorer
	Support int
	MaxSupport int
	MinVertices int
	MaxQueueSize int
	Report chan *goiso.SubGraph
	MakeStore func() store.SubGraphs
	processed, queued *Seen
}

func Depth(
	G *goiso.Graph,
	scoreName string,
	support, maxSupport, minVertices, maxQueueSize int,
	makeStore func() store.SubGraphs,
	memProf io.Writer,
) (
	<-chan *goiso.SubGraph,
) {
	m := &DepthMiner{
		Graph: G,
		ScoreName: scoreName,
		Support: support,
		MaxSupport: maxSupport,
		MinVertices: minVertices,
		MaxQueueSize: maxQueueSize,
		Report: make(chan *goiso.SubGraph), MakeStore: makeStore,
		processed: NewSeen(),
		queued: NewSeen(),
	}
	switch scoreName {
	case "random": m.Scorer = &RandomScore{}
	case "pq": m.Scorer = &PQScore{m}
	case "queue": m.Scorer = &QueueScore{}
	case "rand-queue": m.Scorer = &RandomQueueScore{}
	case "processed": m.Scorer = &ProcessedScore{m}
	case "size-random": m.Scorer = &SizeRandomScore{}
	default: panic(fmt.Errorf("Unknown Score Function"))
	}

	go m.mine()

	return m.Report
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
	// queue := make([]*isoGroup, 0, N)
	queue := NewQueue(m.Graph)
	initial := m.initial()
	addInitial := func() {
		for part := range initial {
			s := m.support(part)
			if s >= m.Support && s < m.MaxSupport {
				g := &isoGroup{part[0].ShortLabel(), part}
				// queue = append(queue, g)
				queue.Add(g)
				m.queued.Add(g.label)
				break
			}
		}
	}
	addInitial()
	i := 0
	for queue.Size() > 0 {
		var item *isoGroup
		item = m.takeOne(queue)
		// if i % 100 == 0 {
			log.Println("process:", i, m.processed.Size(), queue.Size(), len(item.part), item.part[0].Label())
		// }
		m.process(item, func(lp *isoGroup) {
			m.queued.Add(lp.label)
			queue.Add(lp)
			// queue = append(queue, lp)
			for queue.Size() > N {
				m.dropOne(queue)
			}
		})
		if queue.Size() == 0 {
			addInitial()
		}
		i++
	}
}

func (m *DepthMiner) takeOne(queue *Queue) *isoGroup {
	s := sample(10, queue.Size())
	k := m.Scorer.Kernel(queue, s)
	var i int
	var ms float64
	if len(k) > 0 {
		i, ms = max(srange(len(k)), func(i int) float64 { return k.Mean(i) })
		i = s[i]
	} else {
		i, ms = max(s, func(i int) float64 { return m.Scorer.Score(queue.Get(i), queue) })
	}
	log.Println("max score", ms)
	return queue.Pop(i)
}

func (m *DepthMiner) dropOne(queue *Queue) {
	s := sample(10, queue.Size())
	k := m.Scorer.Kernel(queue, s)
	var i int
	var ms float64
	if len(k) > 0 {
		i, ms = min(srange(len(k)), func(i int) float64 { return k.Mean(i) })
		i = s[i]
	} else {
		i, ms = min(s, func(i int) float64 { return m.Scorer.Score(queue.Get(i), queue) })
	}
	log.Println("min score", ms)
	queue.Pop(i)
}


func (m *DepthMiner) process(lp *isoGroup, send func(*isoGroup)) {
	if m.processed.Has(lp.label) {
		return
	}
	m.processed.Add(lp.label)
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
		if s >= m.Support && s < m.MaxSupport && !m.queued.Has(label) {
			send(&isoGroup{label, extended})
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
		if m.queued.Has(label) {
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
		s := VertexSet(sg)
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
