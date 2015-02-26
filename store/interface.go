package store

import(
	"github.com/timtadh/goiso"
)

type SubGraphsIterable interface {
	Keys() BytesIterator
	Values() SGIterator
	Iterate() Iterator
}

type SubGraphsOperable interface {
	Has(key []byte) bool
	Count(key []byte) int
	Add(key []byte, value *goiso.SubGraph)
	Find(key []byte) Iterator
	Remove(key []byte, where func(*goiso.SubGraph) bool) error
}

type SubGraphs interface {
	SubGraphsIterable
	SubGraphsOperable
	Size() int
}

type SGIterator func() (*goiso.SubGraph, SGIterator)
type BytesIterator func() ([]byte, BytesIterator)
type Iterator func() ([]byte, *goiso.SubGraph, Iterator)

