package store

import (
	"log"
	"sync"
)

import(
	"github.com/timtadh/goiso"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)

func assert_ok(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func serializeValue(value *goiso.SubGraph) []byte {
	return value.Serialize()
}

func deserializeValue(g *goiso.Graph, bytes []byte) (value *goiso.SubGraph) {
	return goiso.DeserializeSubGraph(g, bytes)
}

type Fs2BpTree struct {
	g *goiso.Graph
	bf *fmap.BlockFile
	bpt *bptree.BpTree
	mutex sync.Mutex
}

func AnonFs2BpTree(g *goiso.Graph) *Fs2BpTree {
	bf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	assert_ok(err)
	return newFs2BpTree(g, bf)
}

func NewFs2BpTree(g *goiso.Graph, path string) *Fs2BpTree {
	bf, err := fmap.CreateBlockFile(path)
	assert_ok(err)
	return newFs2BpTree(g, bf)
}

func newFs2BpTree(g *goiso.Graph, bf *fmap.BlockFile) *Fs2BpTree {
	bpt, err := bptree.New(bf, -1, -1)
	assert_ok(err)
	return &Fs2BpTree {
		g: g,
		bf: bf,
		bpt: bpt,
	}
}

func (self *Fs2BpTree) Size() int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.bpt.Size()
}

func (self *Fs2BpTree) Keys() (it BytesIterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	raw, err := self.bpt.Keys()
	assert_ok(err)
	it = func() (k []byte, _ BytesIterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		var err error
		k, err, raw = raw()
		assert_ok(err)
		if raw == nil {
			return nil, nil
		}
		return k, it
	}
	return it
}

func (self *Fs2BpTree) Values() (it SGIterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Iterate()
	assert_ok(err)
	raw := self.kvIter(kvi)
	it = func() (v *ParentedSg, _ SGIterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		_, v, raw = raw()
		if raw == nil {
			return nil, nil
		}
		return v, it
	}
	return it
}

func (self *Fs2BpTree) Iterate() (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Iterate()
	assert_ok(err)
	return self.kvIter(kvi)
}

func (self *Fs2BpTree) Backward() (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Backward()
	assert_ok(err)
	return self.kvIter(kvi)
}

func (self *Fs2BpTree) Has(key []byte) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	has, err := self.bpt.Has(key)
	assert_ok(err)
	return has
}

func (self *Fs2BpTree) Count(key []byte) int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	count, err := self.bpt.Count(key)
	assert_ok(err)
	return count
}

func (self *Fs2BpTree) Add(key []byte, psg *ParentedSg) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	value := psg.Serialize()
	assert_ok(self.bpt.Add(key, value))
	has, err := self.bpt.Has(key)
	assert_ok(err)
	if !has {
		panic("didn't have key just added")
	}
	// assert_ok(self.bf.Sync())
}

func (self *Fs2BpTree) kvIter(kvi bptree.KVIterator) (it Iterator) {
	it = func() ([]byte, *ParentedSg, Iterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		var key []byte
		var bytes []byte
		var err error
		key, bytes, err, kvi = kvi()
		// log.Println("kv iter", bytes, err, kvi)
		assert_ok(err)
		if kvi == nil {
			return nil, nil, nil
		}
		psg := DeserializeParentedSg(self.g, bytes)
		return key, psg, it
	}
	return it
}

func (self *Fs2BpTree) Find(key []byte) (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Find(key)
	assert_ok(err)
	return self.kvIter(kvi)
}

func (self *Fs2BpTree) Remove(key []byte, where func(*ParentedSg) bool) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.bpt.Remove(key, func(bytes []byte) bool {
		sg := DeserializeParentedSg(self.g, bytes)
		return where(sg)
	})
}

func (self *Fs2BpTree) Delete() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	err := self.bf.Close()
	assert_ok(err)
	if self.bf.Path() != "" {
		err = self.bf.Remove()
		assert_ok(err)
	}
}

