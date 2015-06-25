package mine

import (
	"encoding/binary"
	"math/rand"
)

import (
	"github.com/timtadh/fsm/subgraph"
)


type RandomScore struct{}
func (r *RandomScore) Score(label []byte, population Samplable) float64 { return rand.Float64() }
func (r *RandomScore) Kernel(population Samplable) Kernel { return nil }


type SizeRandomScore struct{}
func (r *SizeRandomScore) Score(label []byte, population Samplable) float64 {
	E := int(binary.BigEndian.Uint32(label[0:4]))
	V := int(binary.BigEndian.Uint32(label[4:8]))
	return (1/float64(V*E))*(100*rand.Float64())
}
func (r *SizeRandomScore) Kernel(population Samplable) Kernel { return nil }


type QueueScore struct{}
func (q *QueueScore) Score(label []byte, population Samplable) float64 {
	return subgraphScore(label, population)
}
func (q *QueueScore) Kernel(population Samplable) Kernel {
	return subgraphKernel(population)
}


type ProcessedScore struct{
	m *DepthMiner
}
func (p *ProcessedScore) Score(label []byte, population Samplable) float64 {
	return subgraphScore(label, p.m.processed)
}
func (p *ProcessedScore) Kernel(population Samplable) Kernel {
	return subgraphKernel(p.m.processed)
}

type PQScore struct{
	m *DepthMiner
}
func (p *PQScore) Score(label []byte, population Samplable) float64 {
	return subgraphScore(label, population)*.5 + subgraphScore(label, p.m.processed)
}
func (p *PQScore) Kernel(population Samplable) Kernel {
	a := subgraphKernel(population)
	b := subgraphKernel(p.m.processed)
	return kernel(srange(len(a)), func(i, j int) float64 {
		return a[i][j]*.5 + b[i][j]
	})
}


type RandomQueueScore struct{}
func (q *RandomQueueScore) Score(label []byte, population Samplable) float64 {
	sampleSize := 10
	L := subgraph.FromShortLabel(label)
	mean, _ := mean(sample(sampleSize, population.Size()), func(i int) float64 {
		if rand.Float64() < (3/float64(sampleSize)) {
			O := subgraph.FromShortLabel(population.Get(i))
			return L.Metric(O)
		} else {
			return rand.Float64()
		}
	})
	return mean
}
func (q *RandomQueueScore) Kernel(population Samplable) Kernel {
	s := sample(10, population.Size())
	sgs := make(map[int]*subgraph.Subgraph, len(s))
	return kernel(s, func(i, j int) float64 {
		if rand.Float64() < (3/float64(len(s))) {
			if _, has := sgs[i]; !has {
				sgs[i] = subgraph.FromShortLabel(population.Get(i))
			}
			if _, has := sgs[j]; !has {
				sgs[j] = subgraph.FromShortLabel(population.Get(j))
			}
			return sgs[i].Metric(sgs[j])
		} else {
			return rand.Float64()
		}
	})
}


func subgraphKernel(population Samplable) Kernel {
	s := sample(10, population.Size())
	sgs := make(map[int]*subgraph.Subgraph, len(s))
	for _, i := range s {
		sgs[i] = subgraph.FromShortLabel(population.Get(i))
	}
	return kernel(s, func(i, j int) float64 {
		return sgs[i].Metric(sgs[j])
	})
}

func subgraphScore(label []byte, queue Samplable) float64 {
	sampleSize := 10
	L := subgraph.FromShortLabel(label)
	mean, _ := mean(sample(sampleSize, queue.Size()), func(i int) float64 {
		O := subgraph.FromShortLabel(queue.Get(i))
		return L.Metric(O)
	})
	return mean
}

