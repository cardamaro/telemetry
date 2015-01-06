package tsdb

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestFetch(t *testing.T) {
	db := NewTsdb()
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 1.0)
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 2.0)
	db.Record("foo.bar", map[string]string{"a": "c", "c": "d", "X": "Z"}, time.Now(), 3.0)
	db.Record("foo.bar", map[string]string{"a": "c", "c": "e"}, time.Now(), 4.0)
	r := db.Fetch("foo.bar", nil)
	for _, row := range r {
		fmt.Println(row.Var)
		for _, s := range row.Samples {
			fmt.Println(s)
		}
	}

	fmt.Println("== Sum")
	r = db.Do(Sum, "foo.bar", nil, nil)
	fmt.Println(r)
	fmt.Println("== Count")
	r = db.Do(Count, "foo.bar", nil, nil)
	fmt.Println(r)

	fmt.Println("== Sum w/ Filter {a=b}")
	r = db.Do(Sum, "foo.bar", map[string]string{"a": "b"}, nil)
	fmt.Println(r)

	fmt.Println("== Sum w/ Group By [a, c]")
	r = db.Do(Sum, "foo.bar", nil, []string{"a", "c"})
	fmt.Println(r)

	fmt.Println("== Count w/ Group By [a, c]")
	r = db.Do(Count, "foo.bar", nil, []string{"a", "c"})
	fmt.Println(r)

	fmt.Println("== Sum w/ Filter {c=d} and Group By [a, c]")
	r = db.Do(Sum, "foo.bar", map[string]string{"c": "d"}, []string{"a", "c"})
	fmt.Println(r)
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
