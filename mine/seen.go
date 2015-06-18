package mine

import (
	"log"
	"sync"
)

import (
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/mmlist"
)

type Seen struct {
	lock sync.RWMutex
	setBf *fmap.BlockFile
	set *bptree.BpTree
	listBf *fmap.BlockFile
	list *mmlist.List
}

func NewSeen() *Seen {
	setBf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	if err != nil {
		log.Fatal(err)
	}
	listBf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	if err != nil {
		log.Fatal(err)
	}
	set, err := bptree.New(setBf, -1, 0)
	if err != nil {
		log.Fatal(err)
	}
	list, err := mmlist.New(listBf)
	if err != nil {
		log.Fatal(err)
	}
	return &Seen {
		setBf: setBf,
		set: set,
		listBf: listBf,
		list: list,
	}
}

func (s *Seen) Has(item []byte) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	has, err := s.set.Has(item)
	if err != nil {
		log.Fatal(err)
	}
	return has
}

func (s *Seen) Add(item []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	has, err := s.set.Has(item)
	if err != nil {
		log.Fatal(err)
	}
	if has {
		return
	}
	err = s.set.Add(item, []byte(""))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.list.Append(item)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Seen) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return int(s.list.Size())
}

func (s *Seen) Get(i int) []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()
	item, err := s.list.Get(uint64(i))
	if err != nil {
		log.Fatal(err)
	}
	return item
}
