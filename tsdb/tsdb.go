package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/couchbaselabs/go-slab"
	"math"
	"sort"
	"time"

	"github.com/cardamaro/telemetry"
)

type Row struct {
	Var     string
	Samples []*Sample
}

type Sample struct {
	Timestamp time.Time
	Value     float64
}

func (s *Sample) String() string {
	return fmt.Sprintf("<%.3f @ %s>", s.Value, s.Timestamp)
}

type TimeseriesDatabase interface {
	Record(metric string, tags map[string]string, value float64)
	Query(metric string, tags map[string]string) []*Row
}

type tsdb struct {
	s          map[string]uint32
	ss         map[uint32]string
	metrics    map[uint32][][]byte
	tagsets    map[uint32][]byte
	stagsets   map[string]uint32
	arena      *slab.Arena
	tagCounter *telemetry.AtomicUint32
}

func NewTsdb() *tsdb {
	arena := slab.NewArena(24, 1024*1024, 2, nil)

	t := &tsdb{
		s:          make(map[string]uint32),
		ss:         make(map[uint32]string),
		metrics:    make(map[uint32][][]byte),
		tagsets:    make(map[uint32][]byte),
		stagsets:   make(map[string]uint32),
		arena:      arena,
		tagCounter: new(telemetry.AtomicUint32),
	}
	t.tagCounter.Add(10000000)
	return t
}

func (t *tsdb) getIdForString(tag string) uint32 {
	if v, ok := t.s[tag]; ok {
		return v
	}
	v := t.tagCounter.Add(1)
	t.s[tag] = v
	t.ss[v] = tag
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
		fmt.Println(prefix)
		used += m[prefix+"chunkSize"] * m[prefix+"numChunksInUse"]
	}
	fmt.Printf("Total size: %d\n", used/(1024*1024))
	fmt.Printf("Stats: %+v\n", m)
}

func (t *tsdb) Record(metric string, tags map[string]string, timestamp time.Time, value float64) {
	keys := sortAndFilterTags(tags)
	b := t.arena.Alloc(16 + (8 * len(keys)))

	binary.BigEndian.PutUint64(b, uint64(timestamp.UnixNano()))
	binary.BigEndian.PutUint64(b[8:], math.Float64bits(value))

	for i, k := range keys {
		binary.BigEndian.PutUint32(b[16+(i*8):], t.getIdForString(k))
		binary.BigEndian.PutUint32(b[20+(i*8):], t.getIdForString(tags[k]))
	}

	metricId := t.getIdForString(metric)
	t.metrics[metricId] = append(t.metrics[metricId], b)
	//fmt.Printf("recorded metric %s [%d] @ %s:\n% x\n", metric, t.getIdForString(metric), timestamp, b)
}

func (t *tsdb) Query(metric string, tags map[string]string) []*Row {
	entry, ok := t.metrics[t.getIdForString(metric)]
	if !ok {
		return nil
	}

	keys := sortAndFilterTags(tags)

	filter := t.arena.Alloc(8 * len(keys))
	for _, k := range keys {
		binary.BigEndian.PutUint32(filter, t.getIdForString(k))
		binary.BigEndian.PutUint32(filter[4:], t.getIdForString(tags[k]))
	}

	rows := make([]*Row, 0, 8)
	out := make(map[string]*Row)

	for _, b := range entry {
		var value float64
		include := true

		timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(b[0:8])))
		tagBytes := b[16:]

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
			value = math.Float64frombits((binary.BigEndian.Uint64(b[8:16])))
			row, ok := out[string(tagBytes)]
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
				row = &Row{Var: v}
				out[string(tagBytes)] = row
				rows = append(rows, row)
			}
			row.Samples = append(row.Samples, &Sample{timestamp, value})
		}
	}

	t.arena.DecRef(filter)

	return rows
}
