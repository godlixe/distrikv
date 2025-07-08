package storage

import (
	"log"
	"sync"
)

var baseDir = "data"

// MemtableSizeThreshold in records
var MemtableSizeThreshold = 5

type KVData struct {
	Key       string
	Value     string
	IsDeleted bool
}

// LSM is a struct for Log-Structured Merge Tree.
// The implemented LSM will have 2 layers, memtables and SSTables.
// Memtables will be implemented with sync.Map to ensure concurrency safety.
// SSTables will be implemented with SSTables.
type LSM struct {
	mu sync.RWMutex

	// Memtable is the current active memtable
	// that stores the data in memory.
	Memtable *Memtable

	flushingMemtables []*Memtable

	flushQueue chan *Memtable

	sstManager *SSTManager
}

func NewLSM(sstManager *SSTManager) *LSM {
	lsm := &LSM{
		Memtable:   NewMemtable(),
		sstManager: sstManager,
		flushQueue: make(chan *Memtable),
	}

	lsm.StartFlusher(lsm.flushQueue, sstManager)

	return lsm
}

func (l *LSM) Set(key string, value string) {
	l.Memtable.Set(key, value, false)
	l.checkFlush()
}

func (l *LSM) Get(key string) (*KVData, error) {
	var kvData KVData

	data, err := l.Memtable.Get(key)
	if err != nil {
		return nil, err
	}

	kvData.Key = data.Key
	kvData.Value = data.Value

	// TODO: add a marker to show if the data doesn't exist in memtable
	// currently, if data is just an empty string, or is deleted in memtable
	// it will query in the ssts

	if kvData.Value == "" {
		res, err := l.sstManager.QueryKey(key)
		if err != nil {
			return nil, err
		}

		kvData = *res
	}

	return &kvData, nil
}

func (l *LSM) Delete(key string) {
	l.Memtable.Set(key, "", false)
	l.checkFlush()
}

func (l *LSM) checkFlush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Memtable.Size() >= MemtableSizeThreshold {
		old := l.Memtable

		l.flushingMemtables = append(l.flushingMemtables, old)
		l.Memtable = NewMemtable()

		l.flushQueue <- old
	}
}

func (l *LSM) StartFlusher(flushQueue <-chan *Memtable, sstManager *SSTManager) {
	go func() {
		for mt := range flushQueue {

			// for now, only print error to log if there is a problem flushing
			if err := l.sstManager.FlushSST(mt); err != nil {
				log.Print(err)
			}

			// remove flushed memtable from flushingMemtables
			l.mu.Lock()
			for i := len(l.flushingMemtables) - 1; i >= 0; i-- {
				if l.flushingMemtables[i] == mt {
					l.flushingMemtables = append(l.flushingMemtables[0:i], l.flushingMemtables[i+1:]...)
					break
				}
			}
			l.mu.Unlock()
		}
	}()
}
