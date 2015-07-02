package subgraph

import (
	"encoding/binary"
	"log"
	"math"
)

import (
	"github.com/timtadh/matrix"
)


type Subgraph struct {
	V []uint32
	E []Edge
}

type Edge struct {
	Src int
	Targ int
	Color uint32
}

func FromShortLabel(label []byte) *Subgraph {
	E := int(binary.BigEndian.Uint32(label[0:4]))
	V := int(binary.BigEndian.Uint32(label[4:8]))
	sg := &Subgraph{
		V: make([]uint32, V),
		E: make([]Edge, E),
	}
	off := 8
	for i := 0; i < V; i++ {
		j := off + i*4
		label := binary.BigEndian.Uint32(label[j:j+4])
		sg.V[i] = label
	}
	off += int(V)*4
	for i := 0; i < E; i++ {
		j := off + i*12
		sg.E[i].Src = int(binary.BigEndian.Uint32(label[j:j+4]))
		sg.E[i].Targ = int(binary.BigEndian.Uint32(label[j+4:j+8]))
		sg.E[i].Color = binary.BigEndian.Uint32(label[j+8:j+12])
	}
	return sg
}


func (sg *Subgraph) Metric(o *Subgraph) float64 {
	labels := make(map[uint32]int, len(sg.V)+len(o.V))
	rlabels := make([]uint32, 0, len(sg.V)+len(o.V))
	addLabel := func(label uint32) {
		if _, has := labels[label]; !has {
			labels[label] = len(rlabels)
			rlabels = append(rlabels, label)
		}
	}
	for _, color := range sg.V {
		addLabel(color)
	}
	for _, color := range o.V {
		addLabel(color)
	}
	for i := range sg.E {
		addLabel(sg.E[i].Color)
	}
	for i := range o.E {
		addLabel(o.E[i].Color)
	}
	W := sg.Walks(labels)
	err := W.Subtract(o.Walks(labels))
	if err != nil {
		log.Fatal(err)
	}
	W2, err := W.DenseMatrix().ElementMult(W)
	if err != nil {
		log.Fatal(err)
	}
	norm := W2.DenseMatrix().TwoNorm()
	denom := float64(len(rlabels))
	mean := norm/denom
	metric := math.Sqrt(mean)
	return metric
}

func (sg *Subgraph) LE(labels map[uint32]int) (L, E matrix.Matrix) {
	V := len(sg.V)
	VE := V + len(sg.E)
	L = matrix.Zeros(len(labels), VE)
	E = matrix.Zeros(VE, VE)
	for i, color := range sg.V {
		L.Set(labels[color], i, 1)
	}
	for i := range sg.E {
		L.Set(labels[sg.E[i].Color], V + i, 1)
	}
	for i := range sg.E {
		E.Set(sg.E[i].Src, V + i, 1)
		E.Set(V + i, sg.E[i].Targ, 1)
	}
	return L, E
}

func (sg *Subgraph) Walks(labels map[uint32]int) (W matrix.Matrix) {
	var err error
	L, E := sg.LE(labels)
	LT := matrix.Transpose(L)
	var En matrix.Matrix = matrix.Ones(E.Rows(), E.Cols())
	var SEn matrix.Matrix = matrix.Zeros(E.Rows(), E.Cols())
	for i := 0; i < len(sg.V); i++ {
		En, err = En.Times(E)
		if err != nil {
			log.Fatal(err)
		}
		err = SEn.Add(En)
		if err != nil {
			log.Fatal(err)
		}
	}
	LE, err := L.Times(SEn)
	if err != nil {
		log.Fatal(err)
	}
	LELT, err := LE.Times(LT)
	if err != nil {
		log.Fatal(err)
	}
	return LELT
}

