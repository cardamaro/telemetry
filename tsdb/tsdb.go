package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	tagCounter *telemetry.AtomicUint32
}

func NewTsdb() *tsdb {
	t := &tsdb{
		s:          make(map[string]uint32),
		ss:         make(map[uint32]string),
		metrics:    make(map[uint32][][]byte),
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

func (t *tsdb) Record(metric string, tags map[string]string, timestamp time.Time, value float64) {
	keys := sortAndFilterTags(tags)
	b := make([]byte, 8+(8*len(keys)), 16+(8*len(keys)))

	binary.BigEndian.PutUint64(b, uint64(timestamp.UnixNano()))

	for i, k := range keys {
		binary.BigEndian.PutUint32(b[8+(i*8):], t.getIdForString(k))
		binary.BigEndian.PutUint32(b[12+(i*8):], t.getIdForString(tags[k]))
	}

	buf := bytes.NewBuffer(b)
	binary.Write(buf, binary.BigEndian, value)

	metricId := t.getIdForString(metric)
	t.metrics[metricId] = append(t.metrics[metricId], buf.Bytes())
	//fmt.Printf("recorded metric %s [%d] @ %s:\n% x\n", metric, t.getIdForString(metric), timestamp, buf.Bytes())
}

func (t *tsdb) Query(metric string, tags map[string]string) []*Row {
	entry, ok := t.metrics[t.getIdForString(metric)]
	if !ok {
		return nil
	}

	keys := sortAndFilterTags(tags)

	filter := make([]byte, 8*len(keys))
	for _, k := range keys {
		binary.BigEndian.PutUint32(filter, t.getIdForString(k))
		binary.BigEndian.PutUint32(filter[4:], t.getIdForString(tags[k]))
	}

	rows := make([]*Row, 0, 8)
	out := make(map[string]*Row)

	for _, b := range entry {
		l := len(b)
		tagBytes := b[8 : l-8]
		var value float64
		include := true

		timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(b[0:8])))
		for off := 0; off < len(filter); off += 8 {
			filterBytes := filter[off : off+8]
			idx := bytes.Index(tagBytes, filterBytes)
			if idx == -1 || idx%8 != 0 {
				include = false
				break
			}
			//fmt.Printf("found % x at %d\n", filter[off:off+8], idx)
		}

		if include {
			binary.Read(bytes.NewReader(b[l-8:]), binary.BigEndian, &value)
			row, ok := out[string(tagBytes)]
			if !ok {
				v := metric
				for off := 8; off < l-8; off += 8 {
					if off == 8 {
						v += "{"
					}
					tag := t.ss[binary.BigEndian.Uint32(b[off:off+4])]
					val := t.ss[binary.BigEndian.Uint32(b[off+4:off+8])]
					v += tag + "=" + val
					if off+8 == l-8 {
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

	return rows
}
