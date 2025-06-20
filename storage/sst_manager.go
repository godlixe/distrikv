package storage

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"
)

var SSTMANIFESTFileName = "MANIFEST"

type SSTAction string

const (
	FLUSH      SSTAction = "FLUSH"
	COMPACTION SSTAction = "COMPACTION"
)

type SSTState string

const (
	BEGIN SSTState = "BEGIN"
	DONE  SSTState = "DONE"
)

type SST struct {
	FileName string
	Level    int
}

// SSTManager handles sst operations
// that are used for compaction.
type SSTManager struct {
	mu sync.RWMutex

	file *os.File

	closeFn func() error
}

func NewManifestManager() (*SSTManager, error) {
	f, err := os.OpenFile(
		path.Join(baseDir, SSTMANIFESTFileName),
		os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_RDWR,
		0744,
	)
	if err != nil {
		return nil, err
	}

	return &SSTManager{
		file:    f,
		closeFn: f.Close,
	}, nil
}

func (m *SSTManager) updateManifest(
	action SSTAction,
	state SSTState,
	sstFileName string,
) error {
	m.mu.Lock()
	// append file status in a whitespace separated string
	// FLUSH BEGIN [sst_filename]
	// FLUSH DONE [sst_filename]
	// COMPACTION BEGIN [sst_filename]
	// COMPACTION DONE [sst_filename]
	cmd := fmt.Sprintf("%s %s %s\n", action, state, sstFileName)

	_, err := m.file.Write([]byte(cmd))
	if err != nil {
		return err
	}

	return nil
}

// ListUncompacted lists uncompacted sst files
func (m *SSTManager) ListUncompacted(
	level int,
) []string {
	m.mu.RLock()

	r, err := m.file.Seek(0, io.SeekStart)

	return nil
}

func (m *SSTManager) cleanup() error {
	return m.closeFn()
}
