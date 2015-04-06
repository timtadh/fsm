package store

import (
	"github.com/timtadh/data-structures/tree/bptree"
	"github.com/timtadh/data-structures/types"
)

type MemBpTree bptree.BpTree

func NewMemBpTree(nodeSize int) *MemBpTree {
	return (*MemBpTree)(bptree.NewBpTree(nodeSize))
}

func (self *MemBpTree) Size() int {
	bpt := (*bptree.BpTree)(self)
	return bpt.Size()
}

func (self *MemBpTree) Keys() (it BytesIterator) {
	bpt := (*bptree.BpTree)(self)
	keys := bpt.Keys()
	it = func() ([]byte, BytesIterator) {
		var key types.Equatable
		key, keys = keys()
		if keys == nil {
			return nil, nil
		}
		k := []byte(key.(types.ByteSlice))
		return k, it
	}
	return it
}

func (self *MemBpTree) Values() (it SGIterator) {
	bpt := (*bptree.BpTree)(self)
	vals := bpt.Values()
	it = func() (*ParentedSg, SGIterator) {
		var val interface{}
		val, vals = vals()
		if vals == nil {
			return nil, nil
		}
		v := val.(*ParentedSg)
		return v, it
	}
	return it
}

func (self *MemBpTree) Iterate() (it Iterator) {
	bpt := (*bptree.BpTree)(self)
	return self.kvIter(bpt.Iterate())
}

func (self *MemBpTree) Backward() (it Iterator) {
	bpt := (*bptree.BpTree)(self)
	return self.kvIter(bpt.Backward())
}

func (self *MemBpTree) Has(key []byte) bool {
	bpt := (*bptree.BpTree)(self)
	return bpt.Has(types.ByteSlice(key))
}

func (self *MemBpTree) Count(key []byte) int {
	bpt := (*bptree.BpTree)(self)
	return bpt.Count(types.ByteSlice(key))
}

func (self *MemBpTree) Add(key []byte, psg *ParentedSg) {
	bpt := (*bptree.BpTree)(self)
	err := bpt.Add(types.ByteSlice(key), psg)
	if err != nil {
		panic(err)
	}
}

func (self *MemBpTree) kvIter(kvi types.KVIterator) (it Iterator) {
	it = func() ([]byte, *ParentedSg, Iterator) {
		var key types.Equatable
		var val interface{}
		key, val, kvi = kvi()
		if kvi == nil {
			return nil, nil, nil
		}
		k := []byte(key.(types.ByteSlice))
		v := val.(*ParentedSg)
		return k, v, it
	}
	return it
}

func (self *MemBpTree) Find(key []byte) Iterator {
	bpt := (*bptree.BpTree)(self)
	return self.kvIter(bpt.Find(types.ByteSlice(key)))
}

func (self *MemBpTree) Remove(key []byte, where func(*ParentedSg) bool) error {
	bpt := (*bptree.BpTree)(self)
	return bpt.RemoveWhere(types.ByteSlice(key), func(val interface{}) bool {
		v := val.(*ParentedSg)
		return where(v)
	})
}

func (self *MemBpTree) Delete() {
	// nothing to do for the mem version
}

