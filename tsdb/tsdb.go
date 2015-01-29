package tsdb

// Package tsdb implements a simple, many-dimensional, in-memory timeseries
// database with some basic query operations.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/adler32"
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

// RowSlice is a sortable wrapper, mostly useful for testing
type RowSlice []*Row

func (a RowSlice) Len() int           { return len(a) }
func (a RowSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a RowSlice) Less(i, j int) bool { return a[i].Var < a[j].Var }

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
	allocsChanBuffer = 64
	slabSize         = 1024 * 1024 // 1mb slab size
	slabGrowthFactor = 2           // double slab alloc
	timestampWidth   = 8
	tagsWidth        = 4
	valueWidth       = 8
	sampleWidth      = timestampWidth + tagsWidth + valueWidth
)

// NewTsdb returns a new timeseries database.
func NewTsdb() *Tsdb {
	t := &Tsdb{
		ss:         make(map[uint32][]byte),
		metrics:    make(map[string][][]byte),
		allocsChan: make(chan int, allocsChanBuffer),
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

	select {
	case size := <-t.allocsChan:
		t.internalRecord(TsdbArenaMallocSizeMetric, 0, time.Now(), float64(size))
	default:
	}
}

func (t *Tsdb) internalRecord(metric string, tagsID uint32, timestamp time.Time, value float64) {
	b := t.arena.Alloc(sampleWidth)
	binary.BigEndian.PutUint64(b, uint64(timestamp.UnixNano()))
	binary.BigEndian.PutUint32(b[timestampWidth:], tagsID)
	binary.BigEndian.PutUint64(b[timestampWidth+tagsWidth:], math.Float64bits(value))
	t.metrics[metric] = append(t.metrics[metric], b)
}

// Op defines a kind of operation to be performed on the matching rows.
type Op interface {
	Perform(row *Row, operand float64, timestamp time.Time)
}

type op struct {
	opFunc func(row *Row, operand float64, timestamp time.Time)
}

func (o *op) Perform(row *Row, operand float64, timestamp time.Time) {
	o.opFunc(row, operand, timestamp)
}

var (
	// Fetch returns all samples per metric
	Fetch Op = &op{func(row *Row, operand float64, timestamp time.Time) {
		row.Samples = append(row.Samples, &Sample{timestamp, operand})
	}}
	// Sum computes the arithmetic sum of all samples
	Sum Op = &op{func(row *Row, operand float64, timestamp time.Time) {
		row.Samples[0].Value += operand
	}}
	// Count of all samples
	Count Op = &op{func(row *Row, operand float64, timestamp time.Time) {
		row.Samples[0].Value++
	}}
	// Distribution of values for all samples
	Distribution Op = &op{func(row *Row, operand float64, timestamp time.Time) {
		row.histogram.Update(operand)
	}}
)

// WindowInterpolation defines what method the Window operation will use
// to fill gaps in the timeseries.
type WindowInterpolation int

const (
	// ZeroInterpolation fills gaps with a zero.
	ZeroInterpolation WindowInterpolation = iota
	// LinearInterpolation fills gaps by curve fitting between existing values.
	LinearInterpolation
	// LastValueInterpolation uses the last value seen to fill gaps.
	LastValueInterpolation
)

// WindowAggregation defines what method the Window operation will use
// to combine values in a bin.
type WindowAggregation int

const (
	// SumAggregation sums all values in the bin.
	SumAggregation WindowAggregation = iota
	// MinAggregation uses the minumum value for the bin.
	MinAggregation
	// MaxAggregation uses the maximum value for the bin.
	MaxAggregation
	// MeanAggregation uses the mean of all values in the bin.
	MeanAggregation
)

type window struct {
	freq         time.Duration
	interpolator WindowInterpolation
	aggregator   WindowAggregation
	last         *Sample
	coalesce     int
}

// Window creates a window operation to operate over the data. It can be used
// to resample the samples in the series, and to fill in missing values.
func Window(freq time.Duration, interp WindowInterpolation, agg WindowAggregation) Op {
	return &window{
		freq:         freq,
		interpolator: interp,
		aggregator:   agg,
	}
}

func (w *window) Perform(row *Row, operand float64, timestamp time.Time) {
	rounded := timestamp.Round(w.freq)

	// coalesce values that lie in the same bin
	var lastSample *Sample
	l := len(row.Samples)
	if l > 0 {
		lastSample = row.Samples[l-1]
	}
	// if this sample is in the same bin as the last sample, combine it
	if lastSample != nil {
		if rounded == lastSample.Timestamp {
			w.coalesce++
			switch w.aggregator {
			case SumAggregation, MeanAggregation:
				lastSample.Value += operand
			case MinAggregation:
				if operand < lastSample.Value {
					lastSample.Value = operand
				}
			case MaxAggregation:
				if operand > lastSample.Value {
					lastSample.Value = operand
				}
			}
			// skip this sample and move on
			return
		} else if w.coalesce > 0 {
			// coalesce using the mean
			if w.aggregator == MeanAggregation {
				lastSample.Value /= float64(w.coalesce + 1)
			}
			w.coalesce = 0
		}
	}

	// interpolation
	if w.last != nil && rounded.Sub(w.last.Timestamp) > time.Duration(w.freq) {
		gap := int(rounded.Sub(w.last.Timestamp) / time.Duration(w.freq))
		var interpValue float64

		for k := 1; k < gap; k++ {
			switch w.interpolator {
			case LinearInterpolation:
				slope := (w.last.Value - operand) / float64(-1.0*gap)
				interpValue = w.last.Value + (float64(k) * slope)
			case LastValueInterpolation:
				interpValue = w.last.Value
			case ZeroInterpolation:
				interpValue = 0
			}

			row.Samples = append(
				row.Samples,
				&Sample{w.last.Timestamp.Add(time.Duration(k) * w.freq), interpValue})
		}
	}

	samp := &Sample{rounded, operand}
	row.Samples = append(row.Samples, samp)
	w.last = samp
}

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
func (t *Tsdb) Do(oper Op, metric string, filterTags map[string]string, groupBy []string) []*Row {
	// set all result samples to the same time
	now := time.Now()
	out := make(map[string]*Row)

	t.RLock()
	defer t.RUnlock()

	t.op(metric, filterTags, groupBy, func(varname string, value float64, timestamp time.Time) {
		// initialize a row, setting the timestamp to be the same across all result rows
		if out[varname] == nil {
			out[varname] = &Row{
				Var: varname,
				Max: &Sample{Timestamp: now, Value: math.Inf(-1)},
				Min: &Sample{Timestamp: now, Value: math.Inf(1)},
			}
			switch oper {
			case Sum, Count:
				out[varname].Samples = []*Sample{
					&Sample{Timestamp: now},
				}
			case Distribution:
				out[varname].histogram = NewUnbiasedHistogram()
			}
		}

		oper.Perform(out[varname], value, timestamp)

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
		rows = append(rows, v)
	}

	return rows
}

// Yields the value associated with the string, or the sentinel value.
func (t *Tsdb) getID(s []byte) uint32 {
	return adler32.Checksum(s)
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
	tbuf := make([]byte, tagsWidth*2*len(keys))
	for i, k := range keys {
		binary.BigEndian.PutUint32(tbuf[i*tagsWidth*2:], t.getOrAddID([]byte(k)))
		binary.BigEndian.PutUint32(tbuf[tagsWidth+(i*tagsWidth*2):], t.getOrAddID([]byte(tags[k])))
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

type rowProcessor func(varname string, value float64, timestamp time.Time)

// Iterate over metric values, calling rowProcessor. Skips rows that match tags in
// filterTags.
func (t *Tsdb) op(metric string, filterTags map[string]string, groupBy []string, proc rowProcessor) {
	entry, ok := t.metrics[metric]
	if !ok {
		return
	}

	var (
		varname string
		tags    []string
	)
	doubleWideTagWidth := tagsWidth * 2
	keys := sortAndFilterTags(filterTags)
	lenKeys := len(keys)
	filter := make([]byte, doubleWideTagWidth*len(keys))

	for _, k := range keys {
		binary.BigEndian.PutUint32(filter, t.getID([]byte(k)))
		binary.BigEndian.PutUint32(filter[tagsWidth:], t.getID([]byte(filterTags[k])))
	}

	for k, v := range filterTags {
		tags = append(tags, k+"="+v)
	}

	// build up the set of groupBy tags
	hasGroupBy := len(groupBy) > 0
	groupByIds := make(map[uint32]struct{})
	if hasGroupBy && groupBy[0] != "" {
		for _, v := range groupBy {
			groupByIds[t.getID([]byte(v))] = struct{}{}
		}
	}

L1:
	for _, b := range entry {
		timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(b[0:timestampWidth])))
		tagBytes := t.ss[binary.BigEndian.Uint32(b[timestampWidth:timestampWidth+tagsWidth])]

		for off := 0; off < doubleWideTagWidth*lenKeys; off += doubleWideTagWidth {
			filterBytes := filter[off : off+doubleWideTagWidth]
			idx := bytes.Index(tagBytes, filterBytes)
			if idx == -1 || idx%doubleWideTagWidth != 0 {
				continue L1
			}
		}

		value := math.Float64frombits((binary.BigEndian.Uint64(b[timestampWidth+tagsWidth:])))

		if hasGroupBy && groupBy[0] == "" {
			varname = metric + "{}"
		} else if hasGroupBy {
			// build up a string containing the tags that match the groupBy tags
			gvtags := tags
			matched := 0
			for off := 0; off < len(tagBytes); off += doubleWideTagWidth {
				tt := binary.BigEndian.Uint32(tagBytes[off : off+tagsWidth])
				if _, ok := groupByIds[tt]; ok {
					// only add this tag if it doesn't exist in the filterTags map
					if filterTags[string(t.ss[tt])] == "" {
						tv := binary.BigEndian.Uint32(tagBytes[off+tagsWidth : off+doubleWideTagWidth])
						gvtags = append(gvtags, fmt.Sprintf("%s=%s", t.ss[tt], t.ss[tv]))
					}
					matched++
				}
			}
			// did we match all of the groupBy tags?
			if matched != len(groupByIds) {
				continue
			}
			sort.Strings(gvtags)
			varname = metric + "{" + strings.Join(gvtags, ",") + "}"
		} else {
			gvtags := tags
			for off := 0; off < len(tagBytes); off += doubleWideTagWidth {
				tag := t.ss[binary.BigEndian.Uint32(tagBytes[off:off+tagsWidth])]
				val := t.ss[binary.BigEndian.Uint32(tagBytes[off+tagsWidth:off+doubleWideTagWidth])]
				if filterTags[string(tag)] == "" {
					gvtags = append(gvtags, fmt.Sprintf("%s=%s", tag, val))
				}
			}
			sort.Strings(gvtags)
			varname = metric + "{" + strings.Join(gvtags, ",") + "}"
		}

		proc(varname, value, timestamp)
	}
}
