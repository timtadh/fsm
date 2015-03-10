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
	Delete()
}

type SGIterator func() (*goiso.SubGraph, SGIterator)
type BytesIterator func() ([]byte, BytesIterator)
type Iterator func() ([]byte, *goiso.SubGraph, Iterator)

type Triple [3]uint32

type Triples interface {
	Size() int
	Has(*Triple) bool
	Inc(*Triple) bool
	Value(*Triple) int
}

type Keys interface {
	Put(key []byte)
	Keys() BytesIterator
}

func JoinIterators(its []Iterator) Iterator {
	var make_it func(int) Iterator
	make_it = func(i int) (it Iterator) {
		if i >= len(its) {
			return nil
		}
		var next Iterator = its[i]
		it = func() (k []byte, sg *goiso.SubGraph, _ Iterator) {
			k, sg, next = next()
			if next == nil {
				it := make_it(i + 1)
				if it == nil {
					return nil, nil, nil
				} else {
					return it()
				}
			}
			return k, sg, it
		}
		return it
	}
	return make_it(0)
}

