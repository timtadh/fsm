package store

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"log"
	"sync"
)

import(
	"github.com/timtadh/data-structures/set"
	"github.com/timtadh/data-structures/types"
	"github.com/timtadh/goiso"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)

func assert_ok(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func hash(key []byte) uint64 {
	h := fnv.New64a()
	_, err := h.Write(key)
	assert_ok(err)
	return h.Sum64()
}

func bhash(key uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, key)
	return bytes
}

func serialize(key []byte, value *goiso.SubGraph) []byte {
	val := value.Serialize()
	bytes := make([]byte, 8 + len(key) + len(val))
	binary.LittleEndian.PutUint32(bytes[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(bytes[4:8], uint32(len(val)))
	off := 8
	{
		s := off
		e := s + len(key)
		copy(bytes[s:e], key)
	}
	off += len(key)
	{
		s := off
		e := s + len(val)
		copy(bytes[s:e], val)
	}
	return bytes
}

func deserializeKey(bytes []byte) (key []byte) {
	lenK := binary.LittleEndian.Uint32(bytes[0:4])
	off := 8
	key = make([]byte, lenK)
	s := off
	e := s + len(key)
	copy(key, bytes[s:e])
	return key
}

func deserialize(g *goiso.Graph, bytes []byte) (key []byte, value *goiso.SubGraph) {
	lenK := binary.LittleEndian.Uint32(bytes[0:4])
	lenV := binary.LittleEndian.Uint32(bytes[4:8])
	off := 8
	key = make([]byte, lenK)
	{
		s := off
		e := s + len(key)
		copy(key, bytes[s:e])
	}
	off += len(key)
	{
		s := off
		e := s + int(lenV)
		value = goiso.DeserializeSubGraph(g, bytes[s:e])
	}
	return key, value
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
	bpt, err := bptree.New(bf, 8)
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
	var curBKey []byte
	kset := set.NewSortedSet(10)
	kvi, err := self.bpt.Iterate()
	assert_ok(err)
	i := 0
	it = func() ([]byte, BytesIterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		var valBytes []byte
		var err error
		var bkey []byte
		var key []byte
		for key == nil || kset.Has(types.ByteSlice(key)) {
			bkey, valBytes, err, kvi = kvi()
			assert_ok(err)
			if kvi == nil {
				return nil, nil
			}
			key = deserializeKey(valBytes)
			if !bytes.Equal(bkey, curBKey) {
				curBKey = bkey
				kset = set.NewSortedSet(10)
				err := kset.Add(types.ByteSlice(key))
				assert_ok(err)
				i++
				// log.Println("get", self.bf.Path(), key, i)
				return key, it
			}
		}
		err = kset.Add(types.ByteSlice(key))
		assert_ok(err)
		i++
		// log.Println("get", self.bf.Path(), key, i)
		return key, it
	}
	return it
}

func (self *Fs2BpTree) Values() (it SGIterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Iterate()
	assert_ok(err)
	raw := self.kvIter(kvi)
	it = func() (v *goiso.SubGraph, _ SGIterator) {
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

func (self *Fs2BpTree) Has(key []byte) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	has, err := self.bpt.Has(bhash(hash(key)))
	assert_ok(err)
	return has
}

func (self *Fs2BpTree) Count(key []byte) int {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	count, err := self.bpt.Count(bhash(hash(key)))
	assert_ok(err)
	return count
}

func (self *Fs2BpTree) Add(key []byte, sg *goiso.SubGraph) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	bkey := bhash(hash(key))
	val := serialize(key, sg)
	assert_ok(self.bpt.Add(bkey, val))
}

func (self *Fs2BpTree) kvIter(kvi bptree.KVIterator) (it Iterator) {
	it = func() ([]byte, *goiso.SubGraph, Iterator) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		var bytes []byte
		var err error
		_, bytes, err, kvi = kvi()
		// log.Println("kv iter", bytes, err, kvi)
		assert_ok(err)
		if kvi == nil {
			return nil, nil, nil
		}
		key, value := deserialize(self.g, bytes)
		return key, value, it
	}
	return it
}

func (self *Fs2BpTree) Find(key []byte) (it Iterator) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	kvi, err := self.bpt.Find(bhash(hash(key)))
	assert_ok(err)
	raw := self.kvIter(kvi)
	it = func() (k []byte, v *goiso.SubGraph, _ Iterator) {
		for k == nil || !bytes.Equal(key, k) {
			k, v, raw = raw()
			// log.Println("found iter", key, k, raw)
			if raw == nil {
				return nil, nil, nil
			}
		}
		// log.Println("found", k, v)
		return k, v, it
	}
	return it
}

func (self *Fs2BpTree) Remove(key []byte, where func(*goiso.SubGraph) bool) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	bkey := bhash(hash(key))
	return self.bpt.Remove(bkey, func(bytes []byte) bool {
		_, sg := deserialize(self.g, bytes)
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




