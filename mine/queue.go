package mine

import (
	"log"
	"sync"
)

import (
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/mmlist"
	"github.com/timtadh/fsm/store"
	"github.com/timtadh/goiso"
)

type Queue struct {
	lock sync.RWMutex
	store store.SubGraphs
	keysBf *fmap.BlockFile
	keys  *mmlist.List
}

func NewQueue(G *goiso.Graph) *Queue {
	graphs := store.AnonFs2BpTree(G)
	keysBf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	if err != nil {
		log.Fatal(err)
	}
	keys, err := mmlist.New(keysBf)
	if err != nil {
		log.Fatal(err)
	}
	return &Queue {
		store: graphs,
		keysBf: keysBf,
		keys: keys,
	}
}

func (q *Queue) Close() {
	q.lock.Lock()
	defer q.lock.Unlock()
}

func (q *Queue) Delete() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.store.Delete()
	err := q.keysBf.Close()
	if err != nil {
		log.Fatal(err)
	}
	if q.keysBf.Path() != "" {
		err := q.keysBf.Remove()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (q *Queue) Add(group *isoGroup) {
	_, err := q.keys.Append(group.label)
	if err != nil {
		log.Fatal(err)
	}
	for _, sg := range group.part {
		q.store.Add(group.label, sg)
	}
}

func (q *Queue) Pop(i int) *isoGroup {
	key, err := q.keys.SwapDelete(uint64(i))
	if err != nil {
		log.Fatal(err)
	}
	part := make(partition, 0, 10)
	for _, sg, items := q.store.Find(key)(); items != nil; _, sg, items = items() {
		part = append(part, sg)
	}
	q.store.Remove(key, func(sg *goiso.SubGraph) bool { return true })
	return &isoGroup{key, part}
}

func (q *Queue) Get(i int) []byte {
	key, err := q.keys.Get(uint64(i))
	if err != nil {
		log.Fatal(err)
	}
	return key
}

func (q *Queue) Size() int {
	return int(q.keys.Size())
}

