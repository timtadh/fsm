package mine

import (
	"bytes"
	"hash/fnv"
)

import (
	"github.com/timtadh/fsm/store"
	"github.com/timtadh/goiso"
)

type Collectors interface {
	close()
	delete()
	send(sg *goiso.SubGraph)
	size() int
	keys() (kit store.BytesIterator)
	partsCh() <-chan store.Iterator
	partitionIterator(key []byte) (pit store.Iterator)
}

func (m *RandomWalkMiner) makeCollectors(N int) Collectors {
	// return MakeSerCollector(m.StoreMaker, m.collector)
	return MakeParCollector(N, m.StoreMaker, m.collector)
}

func (m *BreadthMiner) makeCollectors(N int) Collectors {
	return MakeParCollector(N, m.MakeStore, m.collector)
}

type SerialCollector struct {
	tree store.SubGraphs
	ch chan *labelGraph
	done chan bool
}

func MakeSerCollector(makeStore func()store.SubGraphs, collector func(store.SubGraphs, <-chan *labelGraph, chan<- bool)) Collectors {
	done := make(chan bool)
	tree := makeStore()
	ch := make(chan *labelGraph, 1)
	go collector(tree, ch, done)
	return &SerialCollector{tree, ch, done}
}

func (c *SerialCollector) close() {
	close(c.ch)
	<-c.done
}

func (c *SerialCollector) delete() {
	c.tree.Delete()
}

func (c *SerialCollector) send(sg *goiso.SubGraph) {
	c.ch<-&labelGraph{sg.ShortLabel(), sg}
}

func (c *SerialCollector) size() int {
	return c.tree.Size()
}

func (c *SerialCollector) keys() (kit store.BytesIterator) {
	return c.tree.Keys()
}

func (c *SerialCollector) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator, 100)
	go func() {
		for k, keys := c.keys()(); keys != nil; k, keys = keys() {
			out <- c.partitionIterator(k)
		}
		close(out)
	}()
	return out
}

func (c *SerialCollector) partitionIterator(key []byte) (pit store.Iterator) {
	return c.tree.Find(key)
}

type ParCollector struct {
	trees []store.SubGraphs
	chs []chan<- *labelGraph
	done chan bool
}

func MakeParCollector(N int, makeStore func()store.SubGraphs, collector func(store.SubGraphs, <-chan *labelGraph, chan<- bool)) Collectors {
	trees := make([]store.SubGraphs, 0, N)
	chs := make([]chan<- *labelGraph, 0, N)
	done := make(chan bool)
	for i := 0; i < N; i++ {
		tree := makeStore()
		ch := make(chan *labelGraph, 1)
		trees = append(trees, tree)
		chs = append(chs, ch)
		go collector(tree, ch, done)
	}
	return &ParCollector{trees, chs, done}
}

func (c *ParCollector) close() {
	for _, ch := range c.chs {
		close(ch)
	}
	for i := 0; i < len(c.chs); i++ {
		<-c.done
	}
}

func (c *ParCollector) delete() {
	for _, bpt := range c.trees {
		bpt.Delete()
	}
}

func hash(bytes []byte) int {
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

/*
func (c *ParCollector) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator)
	done := make(chan bool)
	for _, tree := range c.trees {
		go func(tree store.SubGraphs) {
			for part, next := makePartitions(tree)(); next != nil; part, next = next() {
				out <- part
			}
			done <- true
		}(tree)
	}
	go func() {
		for _ = range c.trees {
			<-done
		}
		close(out)
		close(done)
	}()
	return out
}

func (c *ParCollector) send(sg *goiso.SubGraph) {
	label := sg.ShortLabel()
	idx := hash(label) % len(c.chs)
	c.chs[idx] <- &labelGraph{label, sg}
}

func makePartitions(sgs store.SubGraphs) (p_it partitionIterator) {
	keys := sgs.Keys()
	p_it = func() (part store.Iterator, next partitionIterator) {
		var key []byte
		key, keys = keys()
		if keys == nil {
			return nil, nil
		}
		return bufferedIterator(sgs.Find(key), 10), p_it
	}
	return p_it
}

func (c *ParCollector) partitionIterator(key []byte) (pit store.Iterator) {
	idx := hash(key) % len(c.chs)
	t := c.trees[idx]
	return bufferedIterator(t.Find(key), 10)
}
*/

func (c *ParCollector) partsCh() <-chan store.Iterator {
	out := make(chan store.Iterator, 100)
	go func() {
		for k, keys := c.keys()(); keys != nil; k, keys = keys() {
			out <- c.partitionIterator(k)
		}
		close(out)
	}()
	return out
}

func (c *ParCollector) send(sg *goiso.SubGraph) {
	label := sg.ShortLabel()
	lg := &labelGraph{label, sg}
	bkt := hash(label) % len(c.chs)
	next := bkt
	for i := 0; i < len(c.chs); i++ {
		select {
		case c.chs[next]<-lg:
			return
		default:
			next = (next + 1) % len(c.chs)
		}
	}
	c.chs[bkt]<-lg
}


func (c *ParCollector) partitionIterator(key []byte) (pit store.Iterator) {
	its := make([]store.Iterator, len(c.trees))
	for i, tree := range c.trees {
		its[i] = bufferedIterator(tree.Find(key), 10)
	}
	j := 0
	pit = func() (k []byte, sg *goiso.SubGraph, _ store.Iterator) {
		for j < len(its) {
			if its[j] == nil {
				j++
			} else {
				k, sg, its[j] = its[j]()
				if its[j] != nil {
					return k, sg, pit
				}
			}
		}
		return nil, nil, nil
	}
	return pit
}

func (c *ParCollector) keys() (kit store.BytesIterator) {
	its := make([]store.BytesIterator, len(c.trees))
	peek := make([][]byte, len(c.trees))
	for i, tree := range c.trees {
		its[i] = tree.Keys()
		peek[i], its[i] = its[i]()
	}
	getMin := func() int {
		min := -1
		for i := range peek {
			if peek[i] == nil {
				continue
			}
			if min == -1 || bytes.Compare(peek[i], peek[min]) <= 0 {
				min = i
			}
		}
		return min
	}
	var last []byte = nil
	kit = func() (item []byte, _ store.BytesIterator) {
		item = last
		for bytes.Equal(item, last) {
			min := getMin()
			if min == -1 {
				return nil, nil
			}
			item = peek[min]
			peek[min], its[min] = its[min]()
		}
		last = item
		return item, kit
	}
	return kit
}

func (c *ParCollector) size() int {
	sum := 0
	for _, tree := range c.trees {
		sum += tree.Size()
	}
	return sum
}

