package tsdb

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _intern = make(map[string]string)

func get(s string) string {
	if v, ok := _intern[s]; ok {
		return v
	}
	_intern[s] = s
	return s
}

type foo struct {
	x map[string]string
}

func TestMem(t *testing.T) {
	var m1, m2 runtime.MemStats
	db1 := NewTsdb()
	db2 := NewTsdb()
	//foo1 := &foo{make(map[string]string)}
	//foo2 := &foo{make(map[string]string)}
	iter := 50000

	runtime.GC()
	runtime.ReadMemStats(&m1)
	for i := 0; i < iter; i++ {
		//foo1.x[fmt.Sprintf("%d", i)] = "bar" + fmt.Sprintf("%d", i % 10)
		db1.Record(fmt.Sprintf("%d", i), map[string]string{"a": fmt.Sprintf("%d", i)}, time.Now(), 1)
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	t.Logf("Not interned:")
	t.Logf("HeapAlloc: %d -> %d = %d", m1.HeapAlloc, m2.HeapAlloc, m2.HeapAlloc-m1.HeapAlloc)
	t.Logf("HeapInuse: %d -> %d = %d", m1.HeapInuse, m2.HeapInuse, m2.HeapInuse-m1.HeapInuse)
	t.Logf("HeapObjects: %d -> %d = %d", m1.HeapObjects, m2.HeapObjects, m2.HeapObjects-m1.HeapObjects)

	runtime.GC()
	runtime.ReadMemStats(&m1)
	for i := 0; i < iter; i++ {
		//foo2.x[get(fmt.Sprintf("%d", i))] = get("bar" + fmt.Sprintf("%d", i % 10))
		db2.Record(fmt.Sprintf("%d", i), map[string]string{"a": fmt.Sprintf("%d", i)}, time.Now(), 1)
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	t.Logf("Interned:")
	t.Logf("HeapAlloc: %d -> %d = %d", m1.HeapAlloc, m2.HeapAlloc, m2.HeapAlloc-m1.HeapAlloc)
	t.Logf("HeapInuse: %d -> %d = %d", m1.HeapInuse, m2.HeapInuse, m2.HeapInuse-m1.HeapInuse)
	t.Logf("HeapObjects: %d -> %d = %d", m1.HeapObjects, m2.HeapObjects, m2.HeapObjects-m1.HeapObjects)

	db1.Stats()
	db2.Stats()
}

func TestFetch(t *testing.T) {
	db := NewTsdb()
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 1.0)
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 2.0)
	db.Record("foo.bar", map[string]string{"a": "c", "c": "d", "X": "Z"}, time.Now(), 3.0)
	db.Record("foo.bar", map[string]string{"a": "c", "c": "e"}, time.Now(), 4.0)

	t.Logf("== Sum")
	r := db.Do(Sum, "foo.bar", nil, nil)
	t.Logf("%v", r)
	assert.Equal(t, 1, len(r))
	assert.Equal(t, 1, len(r[0].Samples))
	assert.Equal(t, "foo.bar{}", r[0].Var)
	assert.Equal(t, 10.0, r[0].Samples[0].Value)

	t.Logf("== Count")
	r = db.Do(Count, "foo.bar", nil, nil)
	t.Logf("%v", r)
	assert.Equal(t, 1, len(r))
	assert.Equal(t, 1, len(r[0].Samples))
	assert.Equal(t, "foo.bar{}", r[0].Var)
	assert.Equal(t, 4.0, r[0].Samples[0].Value)

	t.Logf("== Sum w/ Filter {a=b}")
	r = db.Do(Sum, "foo.bar", map[string]string{"a": "b"}, nil)
	t.Logf("%v", r)
	assert.Equal(t, 1, len(r))
	assert.Equal(t, 1, len(r[0].Samples))
	assert.Equal(t, "foo.bar{a=b}", r[0].Var)
	assert.Equal(t, 3.0, r[0].Samples[0].Value)

	t.Logf("== Sum w/ Group By [a, c]")
	r = db.Do(Sum, "foo.bar", nil, []string{"a", "c"})
	t.Logf("%v", r)
	assert.Equal(t, 2, len(r))
	assert.Equal(t, 1, len(r[0].Samples))
	assert.Equal(t, 1, len(r[1].Samples))
	assert.Equal(t, "foo.bar{a=c,c=d}", r[0].Var)
	assert.Equal(t, 3.0, r[0].Samples[0].Value)
	assert.Equal(t, "foo.bar{a=c,c=e}", r[1].Var)
	assert.Equal(t, 4.0, r[1].Samples[0].Value)

	t.Logf("== Count w/ Group By [a, c]")
	r = db.Do(Count, "foo.bar", nil, []string{"a", "c"})
	t.Logf("%v", r)
	assert.Equal(t, 2, len(r))
	assert.Equal(t, 1, len(r[0].Samples))
	assert.Equal(t, 1, len(r[1].Samples))
	assert.Equal(t, "foo.bar{a=c,c=d}", r[0].Var)
	assert.Equal(t, 1.0, r[0].Samples[0].Value)
	assert.Equal(t, "foo.bar{a=c,c=e}", r[1].Var)
	assert.Equal(t, 1.0, r[1].Samples[0].Value)

	t.Logf("== Sum w/ Filter {c=d} and Group By [a, c]")
	r = db.Do(Sum, "foo.bar", map[string]string{"c": "d"}, []string{"a", "c"})
	t.Logf("%v", r)
	assert.Equal(t, 1, len(r))
	assert.Equal(t, 1, len(r[0].Samples))
	assert.Equal(t, "foo.bar{a=c,c=d}", r[0].Var)
	assert.Equal(t, 3.0, r[0].Samples[0].Value)
}

func TestDoDistribution(t *testing.T) {
	db := NewTsdb()
	for i := 1; i <= 10000; i++ {
		v := rand.Float64() * 100
		db.Record("foo.bar", nil, time.Now(), v)
	}
	r := db.Do(Distribution, "foo.bar", nil, nil)
	fmt.Printf("Max: %f\nMin: %f\n%+v\n", r[0].Max.Value, r[0].Min.Value, r[0].Histogram())
}

func TestFetchLarge(t *testing.T) {
	db := NewTsdb()
	start := time.Now()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	for i := 0; i < 100000; i++ {
		nt := fmt.Sprintf("%d", i%100)
		db.Record("foo.bar", map[string]string{"foo": nt}, time.Now(), float64(i))
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	t.Logf("HeapAlloc: %d", m2.HeapAlloc)
	t.Logf("HeapInuse: %d", m2.HeapInuse)
	t.Logf("HeapObjects: %d", m2.HeapObjects)
	t.Logf("duration: %s", time.Since(start))
	db.Stats()

	start = time.Now()
	db.Fetch("foo.bar", map[string]string{"9": "99"})
	t.Logf("duration: %s", time.Since(start))
}

func BenchmarkRecord(b *testing.B) {
	db := NewTsdb()
	for n := 0; n < b.N; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Record("foo.bar", map[string]string{nt: nt + nt}, time.Now(), float64(n))
	}
}

func benchmarkFetch(size int, b *testing.B) {
	db := NewTsdb()
	for n := 0; n < size; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Record("foo.bar", map[string]string{nt: nt + nt}, time.Now(), float64(n))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Fetch("foo.bar", map[string]string{nt: nt + nt})
	}
}

func BenchmarkFetch10(b *testing.B)   { benchmarkFetch(10, b) }
func BenchmarkFetch100(b *testing.B)  { benchmarkFetch(100, b) }
func BenchmarkFetch1000(b *testing.B) { benchmarkFetch(1000, b) }

func benchmarkDo(size int, groupBy bool, b *testing.B) {
	db := NewTsdb()
	for n := 0; n < size; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Record("foo.bar", map[string]string{nt: nt + nt}, time.Now(), float64(n))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		nt := fmt.Sprintf("%d", n)
		if groupBy {
			db.Do(Sum, "foo.bar", map[string]string{nt: nt + nt}, []string{nt})
		} else {
			db.Do(Sum, "foo.bar", map[string]string{nt: nt + nt}, nil)
		}
	}
}

func BenchmarkDo10(b *testing.B)   { benchmarkDo(10, false, b) }
func BenchmarkDo100(b *testing.B)  { benchmarkDo(100, false, b) }
func BenchmarkDo1000(b *testing.B) { benchmarkDo(1000, false, b) }

func BenchmarkDoGroupBy10(b *testing.B)   { benchmarkDo(10, true, b) }
func BenchmarkDoGroupBy100(b *testing.B)  { benchmarkDo(100, true, b) }
func BenchmarkDoGroupBy1000(b *testing.B) { benchmarkDo(1000, true, b) }
