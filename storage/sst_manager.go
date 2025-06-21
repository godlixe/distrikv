package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var SSTMANIFESTFileName = "MANIFEST"

type SSTState int

const (
	// initial sst state
	FLUSHING SSTState = iota

	// in this state, the sst is ready to be compacted
	FLUSHED

	// state of a compacting sst
	COMPACTING

	// in this state, the sst is ready to be deleted
	COMPACTED
)

// SST File Format
// [KeyLength][Key][ValLength][Val]
// ...
// ...
// <Metadata>
// level [level]
// timestamp [creation timestamp]
// <sst_done> (just a marker for marking that a sst is done made)

type SST struct {
	FileName  string
	Level     int
	Timestamp time.Time
	Status    SSTState
}

// SSTManager handles sst operations
// that are used for compaction.
type SSTManager struct {
	// mutex here will lock the whole manager and
	// sst map even if updates are done on different levels.
	// will probably have a better solution later.
	mu sync.RWMutex

	// SST are stored in a map of [int][]*SST.
	// the int is the level of the SST, each level
	// contains the sst on corresponding level.
	// the slice on each level is guaranteed to be
	// sorted by insertion timestamp, because SST are
	// appended to the slice on insertion.
	ssts map[int][]*SST
}

func NewSSTManager() (*SSTManager, error) {
	// Load ssts here
	files, err := filepath.Glob(fmt.Sprintf("%s/*%s", baseDir, SSTFileFormat))
	if err != nil {
		return nil, err
	}

	ssts := parseSSTFiles(files)

	sstm := make(map[int][]*SST)
	sstm[1] = ssts

	return &SSTManager{
		ssts: sstm,
	}, nil
}

type SSTUpdateQuery struct {
	FileName string
	State    SSTState
}

func (m *SSTManager) updateBatch(
	level int,
	sstUpdatequeries []SSTUpdateQuery,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// build map for fast query
	var queries map[string]SSTState
	for _, q := range sstUpdatequeries {
		queries[q.FileName] = q.State
	}

	for idx, sst := range m.ssts[level] {
		newState, ok := queries[sst.FileName]
		if ok {
			m.ssts[level][idx].Status = newState
		}
	}

	return nil
}

func (m *SSTManager) List(
	level int,
	state SSTState,
	count int,
) []*SST {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var res []*SST
	for _, sst := range m.ssts[level] {
		if sst.Status == state {
			res = append(res, sst)

			if len(res) == count {
				return res
			}
		}
	}

	return res
}

// SST file name format is
// level_uuid.sst
func parseSSTFiles(fileNames []string) []*SST {
	var res []*SST
	for _, n := range fileNames {

		// parse sst metadata
		sst, err := parseSSTMetadata(n)
		if err != nil {
			log.Printf("error parsing sst %s : %s\n", n, err)
			continue
		}
		fmt.Println(sst)
		res = append(res, sst)
	}

	return res
}

func parseSSTMetadata(filename string) (*SST, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	// seek to bottom of the file
	// to find metadata.

	maxMetadataSize := 512
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	readSize := int64(maxMetadataSize)
	if size < readSize {
		readSize = size
	}

	buf := make([]byte, readSize)

	_, err = f.Seek(-readSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	_, err = f.Read(buf)
	if err != nil {
		return nil, err
	}

	// parse metadata from buffer
	lines := strings.Split(string(buf), "\n")
	fmt.Println(lines)
	var level int
	var ts time.Time

	// TODO: Should find sst_done marker first to
	// check if sst is fully written.

	for _, line := range lines {
		if strings.HasPrefix(line, "level: ") {
			fmt.Sscanf(line, "level: %d", &level)
		} else if strings.HasPrefix(line, "timestamp: ") {
			var t string
			fmt.Sscanf(line, "timestamp: %s", &t)
			parsed, err := time.Parse(time.RFC3339, t)
			if err != nil {
				log.Println("error parsing sst")
			}

			ts = parsed
		}
	}

	return &SST{
		Level:     level,
		Timestamp: ts,
	}, nil
}
