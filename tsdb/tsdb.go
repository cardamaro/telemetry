package tsdb

// Package tsdb implements a simple, many-dimensional, in-memory timeseries
// database with some basic query operations.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/go-slab"
)

// Metric names for internal state tracking counters
const (
	TsdbOverheadMetric        = "tsdb.arena.overhead" // string overhead counter
	TsdbArenaMallocSizeMetric = "tsdb.arena.malloc"   // malloc bytes counter
)

var (
	defaultPercentiles     = []float64{0.5, 0.75, 0.9, 0.95, 0.99, 0.999}
	defaultPercentileNames = []string{"p50", "p75", "p90", "p95", "p99", "p999"}
)

// TimeseriesDatabase describes a simple timeseries database interface for
// recording and querying accumulators.
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
	histogram *floatSampledHistogram
}

// Histogram returns a JSON string representation of the distribution of values in the row.
func (r *Row) Histogram() string {
	if r.histogram == nil {
		return ""
	}
	v := r.histogram.Distribution()
	b := &bytes.Buffer{}
	fmt.Fprintf(b, `{"count":%d,"sum":%f,"min":%f,"max":%f,"mean":%s`,
		v.Count, v.Sum, v.Min, v.Max,
		strconv.FormatFloat(v.Mean(), 'g', -1, 64))
	perc := r.histogram.Percentiles(defaultPercentiles)
	for i, p := range perc {
		fmt.Fprintf(b, `,"%s":%f`, defaultPercentileNames[i], p)
	}
	fmt.Fprintf(b, "}")
	return b.String()
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

// Tsdb implements a TimeseriesDatabase
type Tsdb struct {
	allocsChan chan int

	sync.RWMutex
	ss      map[uint32][]byte
	metrics map[string][][]byte
	arena   *slab.Arena
}

const (
	maxStringID      uint32 = 1<<32 - 1
	allocsChanBuffer        = 64
	sampleWidth             = 16
	slabSize                = 1024 * 1024 // 1mb slab size
	slabGrowthFactor        = 2           // double slab alloc
)

// NewTsdb returns a new timeseries database.
func NewTsdb() *Tsdb {
	t := &Tsdb{
		ss:         make(map[uint32][]byte),
		metrics:    make(map[string][][]byte),
		allocsChan: make(chan int, 64),
	}

	t.arena = slab.NewArena(sampleWidth, slabSize, slabGrowthFactor, t.malloc)

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
	for m := range t.metrics {
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

	t.internalRecord(metric, t.getIDForTags(tags), timestamp, value)

	for i := 0; i < 2; i++ {
		select {
		case size := <-t.allocsChan:
			t.internalRecord(TsdbArenaMallocSizeMetric, 0, time.Now(), float64(size))
		default:
		}
	}
}

func (t *Tsdb) internalRecord(metric string, tagsID uint32, timestamp time.Time, value float64) {
	b := t.arena.Alloc(sampleWidth)
	binary.BigEndian.PutUint32(b, uint32(timestamp.Unix()))
	binary.BigEndian.PutUint32(b[4:], tagsID)
	binary.BigEndian.PutUint64(b[8:], math.Float64bits(value))
	t.metrics[metric] = append(t.metrics[metric], b)
}

// Op defines a kind of operation to be performed on the matching rows.
type Op int

const (
	// Sum computes the arithmetic sum of all samples
	Sum Op = iota + 1
	// Count of all samples
	Count
	// Distribution of values for all samples
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
			groupByIds[t.getID([]byte(v))] = struct{}{}
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
					if filterTags[string(t.ss[tt])] == "" {
						tv := binary.BigEndian.Uint32(tagBytes[off+4 : off+8])
						gvtags = append(gvtags, fmt.Sprintf("%s=%s", t.ss[tt], t.ss[tv]))
					}
					matched++
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
			out[varname].Samples[0].Value++
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
			h := NewUnbiasedHistogram()
			for _, sample := range v.Samples {
				h.Update(sample.Value)
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
				v += fmt.Sprintf("%s=%s", tag, val)
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
func (t *Tsdb) getID(s []byte) uint32 {
	return crc32.ChecksumIEEE(s)
}

// Interns the string and then returns or generates and returns a unique
// value associated with it.
func (t *Tsdb) getOrAddID(s []byte) uint32 {
	hash := t.getID(s)
	if _, ok := t.ss[hash]; ok {
		return hash
	}
	t.ss[hash] = s
	t.internalRecord(TsdbOverheadMetric, 0, time.Now(), float64(len(s)+4))
	return hash
}

// Canonicalizes the tags into a discrete value.
func (t *Tsdb) getIDForTags(tags map[string]string) uint32 {
	keys := sortAndFilterTags(tags)
	tbuf := make([]byte, 8*len(keys))
	for i, k := range keys {
		binary.BigEndian.PutUint32(tbuf[i*8:], t.getOrAddID([]byte(k)))
		binary.BigEndian.PutUint32(tbuf[4+(i*8):], t.getOrAddID([]byte(tags[k])))
	}
	return t.getOrAddID(tbuf)
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
		binary.BigEndian.PutUint32(filter, t.getID([]byte(k)))
		binary.BigEndian.PutUint32(filter[4:], t.getID([]byte(filterTags[k])))
	}

	for _, b := range entry {
		timestamp := time.Unix(int64(binary.BigEndian.Uint32(b[0:4])), 0)
		tagBytes := t.ss[binary.BigEndian.Uint32(b[4:8])]

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
