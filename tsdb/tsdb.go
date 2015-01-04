package tsdb

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

type Row interface {
	Var() string
	Samples() []Sample
}

type row struct {
	varname          string
	samples          []Sample
	max, min, median Sample
}

func (r *row) Var() string       { return r.varname }
func (r *row) Samples() []Sample { return r.samples }
func (r *row) String() string {
	return fmt.Sprintf("%s: %v", r.varname, r.samples)
}

type Sample interface {
	Timestamp() time.Time
	Value() float64
}

type baseSample struct {
	timestamp time.Time
	value     float64
}

func (s *baseSample) Timestamp() time.Time { return s.timestamp }
func (s *baseSample) Value() float64       { return s.value }
func (s *baseSample) String() string {
	return fmt.Sprintf("<%.3f @ %s>", s.value, s.timestamp)
}

type TimeseriesDatabase interface {
	Record(metric string, tags map[string]string, value float64)
	Query(metric string, tags map[string]string) []*Row
	Sum(metric string, filterTags map[string]string, groupBy []string) []*Row
}

type tsdb struct {
	s          map[string]uint32
	ss         map[uint32]string
	metrics    map[string][][]byte
	tagsets    map[uint32][]byte
	stagsets   map[string]uint32
	arena      *slab.Arena
	mu         *sync.RWMutex
	tagCounter *telemetry.AtomicUint32
}

const MAX_STRING_ID uint32 = 1<<32 - 1

func NewTsdb() *tsdb {
	arena := slab.NewArena(64, 1024*1024, 2, nil)

	t := &tsdb{
		s:          make(map[string]uint32),
		ss:         make(map[uint32]string),
		metrics:    make(map[string][][]byte),
		tagsets:    make(map[uint32][]byte),
		stagsets:   make(map[string]uint32),
		arena:      arena,
		tagCounter: new(telemetry.AtomicUint32),
		mu:         new(sync.RWMutex),
	}
	t.tagCounter.Add(10000000)

	return t
}

var intern = make(map[string]string)

func (t *tsdb) internString(s string) string {
	if v, ok := intern[s]; ok {
		return v
	}
	intern[s] = s
	return s
}

func (t *tsdb) getOrAddIdForString(s string) uint32 {
	s = t.internString(s)
	if v, ok := t.s[s]; ok {
		return v
	}
	v := t.tagCounter.Add(1)
	if v == MAX_STRING_ID {
		panic("MAX_STRING_ID reached")
	}
	t.s[s] = v
	t.ss[v] = s
	return v
}

func (t *tsdb) getIdForString(s string) uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if v, ok := t.s[s]; ok {
		return v
	}
	return MAX_STRING_ID
}

func (t *tsdb) getIdForTags(tags map[string]string) uint32 {
	keys := sortAndFilterTags(tags)
	tbuf := t.arena.Alloc(8 * len(keys))
	for i, k := range keys {
		binary.BigEndian.PutUint32(tbuf[i*8:], t.getOrAddIdForString(k))
		binary.BigEndian.PutUint32(tbuf[4+(i*8):], t.getOrAddIdForString(tags[k]))
	}
	strtbuf := t.internString(string(tbuf))
	if v, ok := t.stagsets[strtbuf]; ok {
		t.arena.DecRef(tbuf)
		return v
	}
	v := t.tagCounter.Add(1)
	if v == MAX_STRING_ID {
		panic("MAX_STRING_ID reached")
	}
	t.tagsets[v] = tbuf
	t.stagsets[strtbuf] = v
	return v
}

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

func (t *tsdb) Stats() {
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

func (t *tsdb) Record(metric string, tags map[string]string, timestamp time.Time, value float64) {
	b := t.arena.Alloc(4 + 4 + 8)
	t.mu.Lock()
	defer t.mu.Unlock()
	binary.BigEndian.PutUint32(b, uint32(timestamp.Unix()))
	binary.BigEndian.PutUint32(b[4:], t.getIdForTags(tags))
	binary.BigEndian.PutUint64(b[8:], math.Float64bits(value))
	t.metrics[metric] = append(t.metrics[metric], b)
	//fmt.Printf("recorded metric %s [%d] @ %s:\n% x\n", metric, t.getIdForString(metric), timestamp, b)
}

type rowProcessor func(tagBytes []byte, value float64, timestamp time.Time)

func (t *tsdb) op(metric string, filterTags map[string]string, proc rowProcessor) {
	t.mu.RLock()
	entry, ok := t.metrics[metric]
	t.mu.RUnlock()
	if !ok {
		return
	}

	keys := sortAndFilterTags(filterTags)

	//filter := make([]byte, 8 * len(keys))
	filter := t.arena.Alloc(8 * len(keys))
	defer t.arena.DecRef(filter)

	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, k := range keys {
		binary.BigEndian.PutUint32(filter, t.getIdForString(k))
		binary.BigEndian.PutUint32(filter[4:], t.getIdForString(filterTags[k]))
	}

	for _, b := range entry {
		timestamp := time.Unix(int64(binary.BigEndian.Uint32(b[0:4])), 0)
		tagBytes := t.tagsets[binary.BigEndian.Uint32(b[4:8])]

		include := true

		for off := 0; off < 8*len(keys); off += 8 {
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

func (t *tsdb) Sum(metric string, filterTags map[string]string, groupBy []string) []Row {
	var rows []Row
	var varname string
	out := make(map[string]Sample)

	var tags []string
	for k, v := range filterTags {
		tags = append(tags, k+"="+v)
	}

	hasGroupBy := len(groupBy) > 0
	groupByIds := make(map[uint32]struct{})
	if hasGroupBy {
		t.mu.RLock()
		for _, v := range groupBy {
			groupByIds[t.getIdForString(v)] = struct{}{}
		}
		t.mu.RUnlock()
	} else {
		varname = metric + "{" + strings.Join(tags, ",") + "}"
	}

	t.op(metric, filterTags, func(tagBytes []byte, value float64, timestamp time.Time) {
		if hasGroupBy {
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
			if matched != len(groupByIds) {
				return
			}
			sort.Strings(gvtags)
			gv := metric + "{" + strings.Join(gvtags, ",") + "}"
			if out[gv] == nil {
				out[gv] = new(baseSample)
			}
			out[gv].(*baseSample).value += value
			out[gv].(*baseSample).timestamp = timestamp
		} else {
			if out[varname] == nil {
				out[varname] = new(baseSample)
			}
			out[varname].(*baseSample).value += value
			out[varname].(*baseSample).timestamp = timestamp
		}
	})

	for k, v := range out {
		row := &row{varname: k, samples: []Sample{v}}
		rows = append(rows, row)
	}

	return rows
}

func (t *tsdb) Query(metric string, filterTags map[string]string) []Row {
	var rows []Row
	out := make(map[string]*row)

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
			r = &row{varname: v}
			out[string(tagBytes)] = r
			rows = append(rows, r)
		}
		r.samples = append(r.samples, &baseSample{timestamp, value})
	})

	return rows
}
