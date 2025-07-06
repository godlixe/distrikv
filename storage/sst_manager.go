package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrSSTIncomplete error = errors.New("sst is incomplete")
)

var SSTFileFormat = ".sst"
var SSTMANIFESTFileName = "MANIFEST"
var SSTDoneMarker = "<sst_done>"

type SSTState int

// SST States
//
// [SST_FLUSHING] -> (1) -> [SST_FLUSHED]
const (
	// initial sst state
	SST_FLUSHING SSTState = iota

	// in this state, the sst is ready to be compacted
	SST_FLUSHED

	// state of a compacting sst
	SST_COMPACTING

	// in this state, the sst is ready to be deleted
	SST_COMPACTED
)

// SST File Format
// [KeyLength][Key][ValLength][Val]
// ...
// ...
// <metadata>
// level [level]
// timestamp [creation timestamp]
// <sst_done> (just a marker for marking that a sst is done made)

type SST struct {
	FileName  string
	Level     int
	Timestamp time.Time
	Status    SSTState
}

type SSTLevel struct {
	mu   sync.RWMutex
	ssts []*SST
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
	levels map[int]*SSTLevel
}

func NewSST(level int, state SSTState) *SST {

	// sstID is just a naming convention for SST Files.
	// UUID is used to ensure there are no conflicting SST Filename.
	sstID := uuid.New()
	return &SST{
		FileName:  fmt.Sprintf("%s%s", sstID, SSTFileFormat),
		Level:     level,
		Status:    state,
		Timestamp: time.Now(),
	}
}

func NewSSTManager() (*SSTManager, error) {
	// Load ssts here
	files, err := filepath.Glob(fmt.Sprintf("%s/*%s", baseDir, SSTFileFormat))
	if err != nil {
		return nil, err
	}

	ssts := parseSSTFiles(files)

	sstm := make(map[int]*SSTLevel)

	for _, sst := range ssts {

		if _, ok := sstm[sst.Level]; !ok {
			sstm[sst.Level] = &SSTLevel{
				ssts: make([]*SST, 0),
			}
		}

		sstm[sst.Level].ssts = append(sstm[sst.Level].ssts, sst)
	}

	return &SSTManager{
		levels: sstm,
	}, nil
}

func (m *SSTManager) updateBatch(
	level int,
	ssts []*SST,
	state SSTState,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// build map for fast query
	var queries map[string]SSTState = make(map[string]SSTState)
	for _, q := range ssts {
		queries[q.FileName] = state
	}

	m.levels[level].mu.Lock()
	for idx, sst := range m.levels[level].ssts {
		newState, ok := queries[sst.FileName]
		if ok {
			m.levels[level].ssts[idx].Status = newState
		}
	}

	return nil
}

func (m *SSTManager) ListSST(
	level int,
	states []SSTState,
	count int,
) []*SST {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var res []*SST
	for _, sst := range m.levels[level].ssts {
		if slices.Contains(states, sst.Status) {
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
		sst.FileName = n
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

	if lines[len(lines)-1] != "<sst_done>" {
		return nil, ErrSSTIncomplete
	}

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
		} else {
			break
		}
	}

	return &SST{
		FileName:  filename,
		Level:     level,
		Timestamp: ts,
		Status:    SST_FLUSHED,
	}, nil
}

// TODO: Restructure SST format to include tombstone and timestamp
func (s *SSTManager) FlushSST(memtable *Memtable) error {
	sst := NewSST(0, SST_FLUSHING)

	f, err := os.OpenFile(
		path.Join(baseDir, sst.FileName),
		os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_RDWR,
		0744,
	)
	if err != nil {
		return err
	}

	defer f.Close()

	var data string

	// add stored data
	for i := memtable.Iterate(); i.Valid(); i.Next() {
		data += fmt.Sprintf(
			"%s:%s\n",
			i.Data().Key, i.Data().Value,
		)
	}

	// add metadata
	data += fmt.Sprintf("<metadata>\nlevel: %d\ntimestamp: %s\n<sst_done>", 0, time.Now().Format(time.RFC3339))
	_, err = f.Write([]byte(data))
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTManager) GetLevels() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.levels)
}

func (s *SSTManager) AddLevel() {
	s.mu.Lock()
	// map[len]
}

func (s *SSTManager) QueryKey(key string) (*KVData, error) {
	s.mu.RLock()
	levels := s.levels
	s.mu.RUnlock()

	var data KVData
	for _, level := range levels {
		level.mu.RLock()

		for _, sst := range level.ssts {
			f, err := os.Open(sst.FileName)
			if err != nil {
				return nil, err
			}

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				// TODO: Binary search the file for key
				parts := strings.SplitN(scanner.Text(), ":", 2)
				if len(parts) == 2 {
					if parts[0] == key {
						data = KVData{
							Key:   parts[0],
							Value: parts[1],
						}
						break
					}
				}
			}

			if data.Key != "" {
				break
			}
		}

		level.mu.RUnlock()
	}

	return &data, nil
}
