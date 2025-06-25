package storage

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	"github.com/google/uuid"
)

var baseDir = "data"

// MemtableSizeThreshold in records
var MemtableSizeThreshold = 5

type KVData struct {
	Key   string
	Value string
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
	data, err := l.Memtable.Get(key)
	if err != nil {
		return nil, err
	}
	return &KVData{
		Key:   data.Key,
		Value: data.Value,
	}, nil
}

func (l *LSM) Delete(key string) {
	l.Memtable.Set(key, "", false)
	l.checkFlush()
}

// Test flush if the record exceeds size threshold
func Flush(memtable *Memtable) error {
	filename := uuid.NewString()

	// will flush as level 0, other levels
	// are handled by compaction.
	sstFullName := fmt.Sprintf("%s_%s.%s", "0", filename, SSTFileFormat)
	// err := updateManifestFile(FLUSH, BEGIN, sstFullName)

	// if err != nil {
	// 	return err
	// }

	f, err := os.OpenFile(
		path.Join(baseDir, sstFullName),
		os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_RDWR,
		0744,
	)
	if err != nil {
		return err
	}

	defer f.Close()

	var data string

	for i := memtable.Iterate(); i.Valid(); i.Next() {
		data += fmt.Sprintf(
			"%s:%s\n",
			i.Data().Key, i.Data().Value,
		)
	}

	_, err = f.Write([]byte(data))
	if err != nil {
		return err
	}

	// err = updateManifestFile(FLUSH, DONE, sstFullName)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (l *LSM) checkFlush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Println("checking, ", l.Memtable.Size(), MemtableSizeThreshold)

	if l.Memtable.Size() >= MemtableSizeThreshold {
		// create new memtable as the new one
		old := l.Memtable
		l.Memtable = NewMemtable()

		// flush the old memtable,
		// TODO: Needs error handling in case flushing fails
		fmt.Println("sending")
		l.flushQueue <- old
		fmt.Println("sent")
	}
}

func (l *LSM) StartFlusher(flushQueue <-chan *Memtable, sstManager *SSTManager) {
	go func() {
		for mt := range flushQueue {
			if err := l.sstManager.Flush(mt); err != nil {
				log.Print(err)
			}
		}
	}()
}
