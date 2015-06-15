package mine

import (
	"encoding/binary"
	"math/rand"
	"os"
)

func init() {
	if urandom, err := os.Open("/dev/urandom"); err != nil {
		panic(err)
	} else {
		seed := make([]byte, 8)
		if _, err := urandom.Read(seed); err == nil {
			rand.Seed(int64(binary.BigEndian.Uint64(seed)))
		}
		urandom.Close()
	}
}

func sample(size, populationSize int) (sample []int) {
	if size >= populationSize {
		sample = make([]int, 0, populationSize)
		for i := 0; i < populationSize; i++ {
			sample = append(sample, i)
		}
		return sample
	}
	in := func(x int, items []int) bool {
		for _, y := range items {
			if x == y {
				return true
			}
		}
		return false
	}
	sample = make([]int, 0, size)
	for i := 0; i < size; i++ {
		j := rand.Intn(populationSize)
		for in(j, sample) {
			j = rand.Intn(populationSize) 
		}
		sample = append(sample, j)
	}
	return sample
}

func min(items []int, f func(item int) int) (arg, min int) {
	arg = -1
	for _, i := range items {
		d := f(i)
		if d < min || arg < 0 {
			min = d
			arg = i
		}
	}
	return arg, min
}

func max(items []int, f func(item int) int) (arg, max int) {
	arg = -1
	for _, i := range items {
		d := f(i)
		if d > max || arg < 0 {
			max = d
			arg = i
		}
	}
	return arg, max
}

