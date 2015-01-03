package tsdb

import (
	"fmt"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	db := NewTsdb()
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 1.0)
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 2.0)
	db.Record("foo.bar", map[string]string{"a": "b"}, time.Now(), 3.0)
	db.Record("foo.bar", map[string]string{"a": "b", "c": "d"}, time.Now(), 4.0)
	r := db.Query("foo.bar", map[string]string{"c": "d"})
	for _, row := range r {
		fmt.Println(row.Var)
		for _, s := range row.Samples {
			fmt.Println(s)
		}
	}
}

func TestQueryLarge(t *testing.T) {
	db := NewTsdb()
	start := time.Now()
	for i := 0; i < 100000; i++ {
		nt := fmt.Sprintf("%d", i)
		db.Record("foo.bar", map[string]string{nt: nt + nt}, time.Now(), float64(i))
	}
	t.Logf("duration: %s", time.Since(start))
	start = time.Now()
	r := db.Query("foo.bar", map[string]string{"99": "9999"})
	t.Logf("duration: %s", time.Since(start))
	t.Logf("result: %+v", r[0])
}

func BenchmarkRecord(b *testing.B) {
	db := NewTsdb()
	for n := 0; n < b.N; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Record("foo.bar", map[string]string{nt: nt + nt}, time.Now(), float64(n))
	}
}

func benchmarkQuery(size int, b *testing.B) {
	db := NewTsdb()
	for n := 0; n < size; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Record("foo.bar", map[string]string{nt: nt + nt}, time.Now(), float64(n))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		nt := fmt.Sprintf("%d", n)
		db.Query("foo.bar", map[string]string{nt: nt + nt})
	}
}

func BenchmarkQuery10(b *testing.B)   { benchmarkQuery(10, b) }
func BenchmarkQuery100(b *testing.B)  { benchmarkQuery(100, b) }
func BenchmarkQuery1000(b *testing.B) { benchmarkQuery(1000, b) }
