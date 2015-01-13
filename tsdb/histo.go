package tsdb

import (
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/samuel/go-metrics/metrics"
)

type HistoSample interface {
	Clear()
	Len() int
	Values() []float64
	Update(value float64)
}

type floatSampledHistogram struct {
	sample HistoSample
	min    float64
	max    float64
	sum    float64
	count  uint64
	lock   sync.RWMutex
}

func NewSampledHistogram(sample HistoSample) *floatSampledHistogram {
	return &floatSampledHistogram{sample: sample}
}

// NewUnbiasedHistogram returns a histogram that uses a uniform sample
// of 1028 elements, which offers a 99.9%
// confidence level with a 5% margin of error assuming a normal
// distribution.
func NewUnbiasedHistogram() *floatSampledHistogram {
	return NewSampledHistogram(NewUniformSample(1028))
}

func (h *floatSampledHistogram) Clear() {
	h.lock.Lock()
	h.sample.Clear()
	h.min = 0
	h.max = 0
	h.sum = 0
	h.count = 0
	h.lock.Unlock()
}

func (h *floatSampledHistogram) Update(value float64) {
	h.lock.Lock()
	h.count++
	h.sum += value
	h.sample.Update(value)
	if h.count == 1 {
		h.min = value
		h.max = value
	} else {
		if value < h.min {
			h.min = value
		}
		if value > h.max {
			h.max = value
		}
	}
	h.lock.Unlock()
}

func (h *floatSampledHistogram) Distribution() metrics.DistributionValue {
	h.lock.RLock()
	v := metrics.DistributionValue{
		Count: h.count,
		Sum:   h.sum,
	}
	if h.count > 0 {
		v.Min = h.min
		v.Max = h.max
	}
	h.lock.RUnlock()
	return v
}

func (h *floatSampledHistogram) Percentiles(percentiles []float64) []float64 {
	scores := make([]float64, len(percentiles))
	values := float64Slice(h.SampleValues())
	if len(values) == 0 {
		return scores
	}
	sort.Sort(values)

	for i, p := range percentiles {
		pos := p * float64(len(values)+1)
		ipos := int(pos)
		switch {
		case ipos < 1:
			scores[i] = values[0]
		case ipos >= len(values):
			scores[i] = values[len(values)-1]
		default:
			lower := values[ipos-1]
			upper := values[ipos]
			scores[i] = lower + (pos-math.Floor(pos))*(upper-lower)
		}
	}

	return scores
}

func (h *floatSampledHistogram) SampleValues() []float64 {
	h.lock.RLock()
	samples := h.sample.Values()
	h.lock.RUnlock()
	return samples
}

// Float64Slice attaches the methods of sort.Interface to []float64, sorting in increasing order.
type float64Slice []float64

func (s float64Slice) Len() int {
	return len(s)
}

func (s float64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s float64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type uniformSample struct {
	reservoirSize int
	values        []float64
}

// NewUniformSample returns a sample randomly selects from a stream. Uses Vitter's
// Algorithm R to produce a statistically representative sample.
//
// http://www.cs.umd.edu/~samir/498/vitter.pdf - Random Sampling with a Reservoir
func NewUniformSample(reservoirSize int) HistoSample {
	return &uniformSample{
		reservoirSize: reservoirSize,
		values:        make([]float64, 0, reservoirSize),
	}
}

func (s *uniformSample) Clear() {
	s.values = s.values[:0]
}

func (s *uniformSample) Len() int {
	return len(s.values)
}

func (s *uniformSample) Update(value float64) {
	if len(s.values) < s.reservoirSize {
		s.values = append(s.values, value)
	} else {
		r := int(rand.Float64() * float64(len(s.values)))
		if r < s.reservoirSize {
			s.values[r] = value
		}
	}
}

func (s *uniformSample) Values() []float64 {
	return s.values
}
