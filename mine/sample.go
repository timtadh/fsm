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

type Samplable interface {
	Size() int
	Get(i int) []byte
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

func replacingSample(size, populationSize int) (sample []int) {
	if size >= populationSize {
		sample = make([]int, 0, populationSize)
		for i := 0; i < populationSize; i++ {
			sample = append(sample, i)
		}
		return sample
	}
	sample = make([]int, 0, size)
	for i := 0; i < size; i++ {
		j := rand.Intn(populationSize)
		sample = append(sample, j)
	}
	return sample
}

func populationTotal(popSize, sampleSize, mean, variance float64) (total, totalVar float64) {
	diff := popSize - sampleSize
	if diff < 0 {
		diff = 0
	}
	return popSize*mean, popSize * (diff)*(variance/sampleSize)
}

func mean(items []int, f func(item int) float64) (mean, variance float64) {
	if len(items) == 0 {
		return -1, -1
	}
	F := make([]float64, len(items))
	var sum float64
	for j, i := range items {
		F[j] = f(i)
		sum += F[j]
	}
	mean = sum / float64(len(items))
	var s2 float64
	for _, f := range F {
		d := f - mean
		s2 += d*d
	}
	if len(items) > 1 {
		variance = (1/(float64(len(items))-1))*s2
	} else {
		variance = 0
	}
	return mean, variance
}

func min(items []int, f func(item int) float64) (arg int, min float64) {
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

func max(items []int, f func(item int) float64) (arg int, max float64) {
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

