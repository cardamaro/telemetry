package tsdb

// Package tsdb implements a simple, many-dimensional, in-memory timeseries
// database with some basic query operations.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/couchbaselabs/go-slab"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cardamaro/telemetry"
)

var (
	percentiles  = []float64{0.05, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999}
	bucketLabels []string
)

func init() {
	for _, p := range percentiles {
		bucketLabels = append(bucketLabels,
			strings.Replace(
				strings.TrimSuffix(fmt.Sprintf("p%.1f", p*100), ".0"),
				".", "", -1))
	}
	bucketLabels = append(bucketLabels, "max")
}

const (
	TsdbOverheadMetric        = "tsdb.arena.overhead"
	TsdbArenaMallocSizeMetric = "tsdb.arena.malloc"
)

type TimeseriesDatabase interface {
	Metrics() []string
	Record(metric string, tags map[string]string, timestamp time.Time, value float64)
	Fetch(metric string, tags map[string]string) []*Row
	Do(op Op, metric string, filterTags map[string]string, groupBy []string) []*Row
}

// Row defines a single var and the associated samples. If the Row is
// returned as the result of an operation, Max and Min will be defined.
type Row struct {
	Var       string
	Samples   []*Sample
	Max, Min  *Sample `json:",omitempty"`
	histogram *telemetry.Histogram
}

func (r *Row) Histogram() map[string]int64 {
	if r.histogram == nil {
		return nil
	}
	return r.histogram.Counts()
}

func (r *Row) String() string {
	return fmt.Sprintf("%s: %v", r.Var, r.Samples)
}

// Sample defined a single discrete measurement. The timestamp is
// truncated to epoch seconds.
type Sample struct {
	Timestamp time.Time
	Value     float64
}

func (s *Sample) String() string {
	return fmt.Sprintf("<%.3f @ %s>", s.Value, s.Timestamp)
}

type Tsdb struct {
	sync.RWMutex
	s          map[string]uint32
	ss         map[uint32]string
	metrics    map[string][][]byte
	tagsets    map[uint32][]byte
	stagsets   map[string]uint32
	arena      *slab.Arena
	allocsChan chan int
	tagCounter *telemetry.AtomicUint32
}

const (
	maxStringId uint32 = 1<<32 - 1
	sampleWidth        = 16
)

// NewTsdb returns a new timeseries database.
func NewTsdb() *Tsdb {
	t := &Tsdb{
		s:          make(map[string]uint32),
		ss:         make(map[uint32]string),
		metrics:    make(map[string][][]byte),
		tagsets:    make(map[uint32][]byte),
		stagsets:   make(map[string]uint32),
		allocsChan: make(chan int, 64),
		tagCounter: new(telemetry.AtomicUint32),
	}

	// 1mb slab size, with doubling
	t.arena = slab.NewArena(sampleWidth, 1024*1024, 2, t.malloc)

	return t
}

func (t *Tsdb) malloc(size int) []byte {
	select {
	case t.allocsChan <- size:
	default:
	}
	return make([]byte, size)
}

// Metrics returns a string slice containing the names of all metrics currently
// tracked in the database. The slice will be sorted.
func (t *Tsdb) Metrics() []string {
	t.RLock()
	defer t.RUnlock()
	out := make([]string, 0, len(t.metrics))
	for m, _ := range t.metrics {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// Stats currently prints some arbitrary and nearly useless information.
func (t *Tsdb) Stats() {
	m := make(map[string]int64)
	t.arena.Stats(m)
	var used int64
	sc := m["numSlabClasses"]
	for i := int64(0); i < sc; i++ {
		prefix := fmt.Sprintf("slabClass-%06d-", i)
		used += m[prefix+"chunkSize"] * m[prefix+"numChunksInUse"]
	}
	fmt.Printf("Total size: %d\n", used)
	fmt.Printf("Stats: %+v\n", m)
}

// Record adds a new measurement to the timeseries database. The timestamp is
// truncated to epoch seconds.
//
// Example:
//   t.Record("foo.bar.baz", map[string]string{"user": "bob", "ip": "10.0.1.2"}, time.Now(), 456.234)
func (t *Tsdb) Record(metric string, tags map[string]string, timestamp time.Time, value float64) {
	t.Lock()
	defer t.Unlock()

	t.internalRecord(metric, t.getIdForTags(tags), timestamp, value)

	for i := 0; i < 2; i++ {
		select {
		case size := <-t.allocsChan:
			t.internalRecord(TsdbArenaMallocSizeMetric, 0, time.Now(), float64(size))
		default:
		}
	}
}

func (t *Tsdb) internalRecord(metric string, tagsId uint32, timestamp time.Time, value float64) {
	b := t.arena.Alloc(sampleWidth)
	binary.BigEndian.PutUint32(b, uint32(timestamp.Unix()))
	binary.BigEndian.PutUint32(b[4:], tagsId)
	binary.BigEndian.PutUint64(b[8:], math.Float64bits(value))
	t.metrics[metric] = append(t.metrics[metric], b)
}

// Op defines a kind of operation to be performed on the matching rows.
type Op int

const (
	Sum Op = iota + 1
	Count
	Distribution
)

// Do applies an operation Op to each matching measurement in the database and
// returns, if any rows match, a slice of Rows. The Var name of each row will
// contain tags matched by filterTags, as well as any tags defined in the
// group by argument.
//
// Example:
//   t.Record("foo.bar", map[string]string{"user": "bob", "ip": "10.0.1.2"}, time.Now(), 3)
//   t.Record("foo.bar", map[string]string{"user": "bob", "ip": "10.0.1.2"}, time.Now(), 4)
//   t.Record("foo.bar", map[string]string{"user": "sam", "ip": "10.0.1.2"}, time.Now(), 5)
//   t.Record("foo.bar", map[string]string{"user": "sam", "ip": "10.0.1.2"}, time.Now(), 6)
//
//   rows := t.Do(Sum, "foo.bar", nil, []string{"user"})
//   // returns:
//   // foo.bar{user=bob} = 7.0
//   // foo.bar{user=sam} = 11.0
//
//   rows := t.Do(Count, "foo.bar", nil, []string{"user"})
//   // returns:
//   // foo.bar{user=bob} = 2.0
//   // foo.bar{user=sam} = 2.0
//
//   rows := t.Do(Count, "foo.bar", nil, []string{"ip"})
//   // returns:
//   // foo.bar{ip=10.0.1.2} = 4.0
func (t *Tsdb) Do(op Op, metric string, filterTags map[string]string, groupBy []string) []*Row {
	var (
		varname string
		tags    []string
	)
	out := make(map[string]*Row)

	for k, v := range filterTags {
		tags = append(tags, k+"="+v)
	}

	t.RLock()
	defer t.RUnlock()

	// build up the set of groupBy tags
	hasGroupBy := len(groupBy) > 0
	groupByIds := make(map[uint32]struct{})
	if hasGroupBy {
		for _, v := range groupBy {
			groupByIds[t.getIdForString(v)] = struct{}{}
		}
	} else {
		varname = metric + "{" + strings.Join(tags, ",") + "}"
	}

	// set all result samples to the same time
	now := time.Now()

	t.op(metric, filterTags, func(tagBytes []byte, value float64, timestamp time.Time) {
		if hasGroupBy {
			// build up a string containing the tags that match the groupBy tags
			gvtags := tags
			matched := 0
			for off := 0; off < len(tagBytes); off += 8 {
				tt := binary.BigEndian.Uint32(tagBytes[off : off+4])
				if _, ok := groupByIds[tt]; ok {
					// only add this tag if it doesn't exist in the filterTags map
					if filterTags[t.ss[tt]] == "" {
						tv := binary.BigEndian.Uint32(tagBytes[off+4 : off+8])
						gvtags = append(gvtags, t.ss[tt]+"="+t.ss[tv])
					}
					matched += 1
				}
			}
			// did we match all of the groupBy tags?
			if matched != len(groupByIds) {
				return
			}
			sort.Strings(gvtags)
			varname = metric + "{" + strings.Join(gvtags, ",") + "}"
		}

		// initialize a row, setting the timestamp to be the same across all result rows
		if out[varname] == nil {
			out[varname] = &Row{
				Var: varname,
				Samples: []*Sample{
					&Sample{Timestamp: now},
				},
				Max: &Sample{Timestamp: now, Value: math.Inf(-1)},
				Min: &Sample{Timestamp: now, Value: math.Inf(1)},
			}
		}

		switch op {
		case Sum:
			out[varname].Samples[0].Value += value
		case Count:
			out[varname].Samples[0].Value += 1
		case Distribution:
			out[varname].Samples = append(out[varname].Samples, &Sample{now, value})
		}

		// keep a few extra stats
		if value < out[varname].Min.Value {
			out[varname].Min.Value = value
		}
		if value > out[varname].Max.Value {
			out[varname].Max.Value = value
		}
	})

	rows := make([]*Row, 0, len(out))
	for _, v := range out {
		if op == Distribution {
			var bucketCutoffs []int64
			max := v.Max.Value

			for _, p := range percentiles {
				bucketCutoffs = append(bucketCutoffs, int64((max*p)*1e9))
			}

			h := telemetry.NewGenericHistogram("", bucketCutoffs, bucketLabels, "Count", "Total")
			for _, sample := range v.Samples {
				h.Add(int64(sample.Value * 1e9))
			}
			v.Samples = nil
			v.histogram = h
		}
		rows = append(rows, v)
	}

	return rows
}

// Fetch returns all discrete varnames and all associated samples. If filterTags
// is non-nil, rows will be filtered by all filter tag values (implicit AND).
func (t *Tsdb) Fetch(metric string, filterTags map[string]string) []*Row {
	var rows []*Row
	out := make(map[string]*Row)

	t.RLock()
	defer t.RUnlock()

	t.op(metric, filterTags, func(tagBytes []byte, value float64, timestamp time.Time) {
		r, ok := out[string(tagBytes)]
		if !ok {
			v := metric
			for off := 0; off < len(tagBytes); off += 8 {
				if off == 0 {
					v += "{"
				}
				tag := t.ss[binary.BigEndian.Uint32(tagBytes[off:off+4])]
				val := t.ss[binary.BigEndian.Uint32(tagBytes[off+4:off+8])]
				v += tag + "=" + val
				if off+8 == len(tagBytes) {
					v += "}"
				} else {
					v += ","
				}
			}
			r = &Row{Var: v}
			out[string(tagBytes)] = r
			rows = append(rows, r)
		}
		r.Samples = append(r.Samples, &Sample{timestamp, value})
	})

	return rows
}

// Yields the value associated with the string, or the sentinel value.
func (t *Tsdb) getIdForString(s string) uint32 {
	if v, ok := t.s[s]; ok {
		return v
	}
	return maxStringId
}

// Interns the string and then returns or generates and returns a unique
// value associated with it.
func (t *Tsdb) getOrAddIdForString(s string) uint32 {
	if v, ok := t.s[s]; ok {
		return v
	}
	v := t.tagCounter.Add(1)
	if v == maxStringId {
		panic("maxStringId reached")
	}
	t.internalRecord(TsdbOverheadMetric, 0, time.Now(), float64(len(s)+4))
	t.s[s] = v
	t.ss[v] = s
	return v
}

// Canonicalizes the tags into a discrete value.
func (t *Tsdb) getIdForTags(tags map[string]string) uint32 {
	keys := sortAndFilterTags(tags)
	tbuf := make([]byte, 8*len(keys))
	for i, k := range keys {
		binary.BigEndian.PutUint32(tbuf[i*8:], t.getOrAddIdForString(k))
		binary.BigEndian.PutUint32(tbuf[4+(i*8):], t.getOrAddIdForString(tags[k]))
	}
	if v, ok := t.stagsets[string(tbuf)]; ok {
		return v
	}
	v := t.tagCounter.Add(1)
	if v == maxStringId {
		panic("maxStringId reached")
	}
	t.internalRecord(TsdbOverheadMetric, 0, time.Now(), float64(len(tbuf)+4))
	t.tagsets[v] = tbuf
	t.stagsets[string(tbuf)] = v
	return v
}

// Sort and remove tags with empty ("") values.
func sortAndFilterTags(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		if v == "" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type rowProcessor func(tagBytes []byte, value float64, timestamp time.Time)

// Iterate over metric values, calling rowProcessor. Skips rows that match tags in
// filterTags.
func (t *Tsdb) op(metric string, filterTags map[string]string, proc rowProcessor) {
	entry, ok := t.metrics[metric]
	if !ok {
		return
	}

	keys := sortAndFilterTags(filterTags)
	lenKeys := len(keys)
	filter := make([]byte, 8*len(keys))

	for _, k := range keys {
		binary.BigEndian.PutUint32(filter, t.getIdForString(k))
		binary.BigEndian.PutUint32(filter[4:], t.getIdForString(filterTags[k]))
	}

	for _, b := range entry {
		timestamp := time.Unix(int64(binary.BigEndian.Uint32(b[0:4])), 0)
		tagBytes := t.tagsets[binary.BigEndian.Uint32(b[4:8])]

		include := true

		for off := 0; off < 8*lenKeys; off += 8 {
			filterBytes := filter[off : off+8]
			idx := bytes.Index(tagBytes, filterBytes)
			if idx == -1 || idx%8 != 0 {
				include = false
				break
			}
			//fmt.Printf("found % x at %d\n", filter[off:off+8], idx)
		}

		if include {
			value := math.Float64frombits((binary.BigEndian.Uint64(b[8:16])))
			proc(tagBytes, value, timestamp)
		}
	}
}
