package telemetry

import (
	"bytes"
	"expvar"
	"fmt"
	"sync"
)

type MergeVar interface {
	expvar.Var
	Merge(other expvar.Var) error
}

type NewRollupMapEntryFunc func() map[string]MergeVar

type RollupMapEntry struct {
	name string
	vars map[string]MergeVar
}

func (e *RollupMapEntry) Get(name string) MergeVar {
	return e.vars[name]
}

func (e *RollupMapEntry) String() string {
	return convertToString(e.vars)
}

func convertToString(vars map[string]MergeVar) string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "{")
	first := true
	for key, val := range vars {
		if !first {
			fmt.Fprintf(&b, ", ")
		}
		fmt.Fprintf(&b, "\"%s\": %v", key, val)
		first = false
	}
	fmt.Fprintf(&b, "}")
	return b.String()
}

type RollupMap struct {
	mu      sync.Mutex
	f       NewRollupMapEntryFunc
	totals  RollupMapEntry
	entries map[*RollupMapEntry]bool
}

func NewRollupMap(name string, f NewRollupMapEntryFunc) *RollupMap {
	c := &RollupMap{
		f:       f,
		totals:  RollupMapEntry{name: name + "Totals", vars: f()},
		entries: make(map[*RollupMapEntry]bool),
	}
	expvar.Publish(name, c)
	expvar.Publish(name+"Totals", &c.totals)
	return c
}

func (c *RollupMap) AddEntry(entry string) *RollupMapEntry {
	e := &RollupMapEntry{name: entry, vars: c.f()}
	c.mu.Lock()
	c.entries[e] = true
	c.mu.Unlock()
	return e
}

func (c *RollupMap) RemoveEntry(entry *RollupMapEntry) {
	c.mu.Lock()
	for key, val := range entry.vars {
		c.totals.vars[key].Merge(val)
	}
	delete(c.entries, entry)
	c.mu.Unlock()
}

func (c *RollupMap) String() string {
	var b bytes.Buffer
	firstValue := true

	fmt.Fprintf(&b, "{")
	for k, _ := range c.entries {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(&b, ", ")
		}
		fmt.Fprintf(&b, "\"%v\": %v", k.name, k)
	}
	fmt.Fprintf(&b, "}")
	return b.String()
}
