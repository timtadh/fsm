package mine

import (
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/goiso"
)

var sliceRecycler chan []*goiso.SubGraph
var setRecycler chan *set.SortedSet

func init() {
	sliceRecycler = make(chan []*goiso.SubGraph, 50)
	setRecycler = make(chan *set.SortedSet, 50)
}

func getSlice() []*goiso.SubGraph {
	select {
	case slice := <-sliceRecycler: return slice
	default: return make([]*goiso.SubGraph, 0, 100)
	}
}

func releaseSlice(slice []*goiso.SubGraph) {
	slice = slice[0:0]
	select {
	case sliceRecycler<-slice: return
	default: return
	}
}

func getSet() *set.SortedSet {
	select {
	case set := <-setRecycler: return set
	default: return set.NewSortedSet(100)
	}
}

func releaseSet(set *set.SortedSet) {
	set.Clear()
	select {
	case setRecycler<-set: return
	default: return
	}
}
