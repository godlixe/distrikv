package storage

import (
	"errors"
	"strings"
	"time"

	"github.com/godlixe/skiplist"
)

type MemtableState int

// Memtable States
//
// [MEMTABLE_ACTIVE] -> (1) -> [MEMTABLE_FLUSHING] -> (2) -> [MEMTABLE_FLUSHED]
//
// 1. Memtable is full. LSM will then change the state of it to MEMTABLE_FLUSHING,
// send it to flushing queue and inactive memtables to be queried.
//
// 2. Memtable has been flushed. The LSM will mark it as MEMTABLE_FLUSHED and remove it from the inactive memtables.
const (
	MEMTABLE_ACTIVE MemtableState = iota

	MEMTABLE_FLUSHING

	MEMTABLE_FLUSHED
)

// MemtableIterator is a wrapper for the
// underlying skiplist iterator.
type MemtableIterator struct {
	curr *skiplist.Iterator[MemtableEntry]
}

func (i *MemtableIterator) Valid() bool {
	return i.curr.Valid()
}

func (i *MemtableIterator) Next() {
	i.curr.Next()
}

func (i *MemtableIterator) Data() MemtableEntry {
	return i.curr.Data()
}

// Memtable is the core memtable implementation.
// Memtable stores data in memory before flushing it into SSTables.
type Memtable struct {
	Store skiplist.SkipList[MemtableEntry]

	State MemtableState
}

// MemtableEntry is a struct for objects stored
// inside the memtable.
type MemtableEntry struct {
	Key       string
	Value     string
	Timestamp time.Time
	Deleted   bool
}

func cmpMemtableEntry(a, b MemtableEntry) int {
	return strings.Compare(a.Key, b.Key)
}

func New() *Memtable {
	return &Memtable{
		Store: skiplist.NewDefault[MemtableEntry](
			cmpMemtableEntry,
		),
		State: MEMTABLE_ACTIVE,
	}
}

func (m *Memtable) Set(key string, value string, deleted bool) {
	m.Store.Set(MemtableEntry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
		Deleted:   deleted,
	})
}

func (m *Memtable) Get(key string) (MemtableEntry, error) {
	res, err := m.Store.Search(MemtableEntry{
		Key: key,
	})
	if err != nil && !errors.Is(err, skiplist.ErrTargetNotFound) {
		return MemtableEntry{}, err
	}

	if errors.Is(err, skiplist.ErrTargetNotFound) {
		return MemtableEntry{
			Key:   key,
			Value: "",
		}, nil
	}

	return res, nil
}

func (m *Memtable) Delete(key string) {
	m.Store.Set(MemtableEntry{
		Key:       key,
		Timestamp: time.Now(),
		Deleted:   true,
	})
}

func (m *Memtable) Decode() []MemtableEntry {
	var res []MemtableEntry

	for i := m.Store.Iterate(); i.Valid(); i.Next() {
		res = append(res, i.Data())
	}

	return res
}

func (m *Memtable) Size() int {
	return m.Store.Len()
}

func (m *Memtable) Iterate() MemtableIterator {
	return MemtableIterator{
		curr: m.Store.Iterate(),
	}
}

func NewMemtable() *Memtable {
	return &Memtable{
		Store: skiplist.NewDefault(cmpMemtableEntry),
	}
}
