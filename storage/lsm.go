package storage

import (
	"fmt"
	"os"
	"path"

	"github.com/google/uuid"
)

var baseDir = "data"
var SSTFileFormat = ".sst"

type KVData struct {
	Key   string
	Value string
}

// LSM is a struct for Log-Structured Merge Tree.
// The implemented LSM will have 2 layers, memtables and SSTables.
// Memtables will be implemented with sync.Map to ensure concurrency safety.
// SSTables will be implemented with SSTables.
type LSM struct {
	Memtable *Memtable
}

func (l *LSM) Set(key string, value string) {
	l.Memtable.Set(key, value)
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
	l.Memtable.Delete(key)
}

// Test flush if the record exceeds 500
func (l *LSM) Flush() error {
	filename := uuid.NewString()

	f, err := os.OpenFile(
		path.Join(baseDir, filename+SSTFileFormat),
		os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_RDWR,
		0744,
	)
	if err != nil {
		return err
	}

	defer f.Close()

	var data string

	for i := l.Memtable.Iterate(); i.Valid(); i.Next() {
		data += fmt.Sprintf(
			"%s:%s\n",
			i.Data().Key, i.Data().Value,
		)
	}

	_, err = f.Write([]byte(data))
	if err != nil {
		return err
	}

	return nil
}

func NewLSM() *LSM {
	return &LSM{
		Memtable: NewMemtable(),
	}
}
