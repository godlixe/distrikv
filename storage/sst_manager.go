package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
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

type SSTLevel struct {
	mu   sync.RWMutex
	ssts []*SST

	// counter is used to add incremental numbering for the SST files.
	// incremental numbering is used for sorting the sst files.
	counter atomic.Uint64
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

func (s *SSTManager) NewSST(level int, state SSTState) *SST {

	if level > s.GetLevels()-1 {
		s.levels[level] = &SSTLevel{
			ssts: make([]*SST, 0),
		}
	}

	// increment level counter
	s.levels[level].counter.Add(1)

	// sstID is just a naming convention for SST Files.
	// UUID is used to ensure there are no conflicting SST Filename.
	sstID := s.levels[level].counter.Load()
	sstUUID := uuid.New()
	sst := &SST{
		ID:        sstID,
		FileName:  fmt.Sprintf("%d_%d_%s%s", level, sstID, sstUUID, SSTFileFormat),
		Level:     level,
		Status:    state,
		Timestamp: time.Now(),
	}

	s.mu.Lock()
	s.levels[level].mu.Lock()
	s.levels[level].ssts = append(s.levels[level].ssts, sst)
	s.levels[level].mu.Unlock()

	s.mu.Unlock()
	return sst
}

func NewSSTManager() (*SSTManager, error) {
	// Load ssts here
	files, err := filepath.Glob(fmt.Sprintf("%s/*%s", baseDir, SSTFileFormat))
	if err != nil {
		return nil, err
	}

	ssts := parseSSTFiles(files)

	sstm := make(map[int]*SSTLevel)

	levelMaxID := make(map[int]uint64)

	for _, sst := range ssts {

		if _, ok := sstm[sst.Level]; !ok {
			sstm[sst.Level] = &SSTLevel{
				ssts: make([]*SST, 0),
			}
		}

		sstm[sst.Level].ssts = append(sstm[sst.Level].ssts, sst)
		levelMaxID[sst.Level] = max(levelMaxID[sst.Level], sst.ID)
	}

	for idx, level := range sstm {
		level.counter.Store(levelMaxID[idx])
		sort.Slice(level.ssts, func(a, b int) bool {
			return level.ssts[a].ID < level.ssts[b].ID
		})
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

	m.levels[level].mu.Unlock()

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
	m.levels[level].mu.RLock()
	defer m.levels[level].mu.RUnlock()
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

func (m *SSTManager) RemoveSST(
	level int,
	ssts []*SST,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.levels[level].mu.Lock()
	defer m.levels[level].mu.Unlock()

	var final []*SST
	for _, sst := range m.levels[level].ssts {
		if slices.Contains(ssts, sst) {
			continue
		}

		final = append(final, sst)
	}
	m.levels[level].ssts = final
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
		sst.FileName = path.Base(n)
		res = append(res, sst)
	}

	return res
}

// TODO: Restructure SST format to include tombstone and timestamp
func (s *SSTManager) FlushSST(memtable *Memtable) error {
	sst := s.NewSST(0, SST_FLUSHING)

	f, err := os.OpenFile(
		path.Join(baseDir, sst.FileName),
		os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_RDWR,
		0744,
	)
	if err != nil {
		return err
	}

	defer f.Close()

	writer := bufio.NewWriter(f)

	// add stored data
	for i := memtable.Iterate(); i.Valid(); i.Next() {
		err := encodeSSTEntry(writer, i.Data().Key, i.Data().Value, i.Data().Deleted)
		if err != nil {
			return err
		}
	}

	writeSSTMetadata(writer, sst.ID, 0, time.Now())

	err = writer.Flush()
	if err != nil {
		return err
	}

	err = s.updateBatch(0, []*SST{sst}, SST_FLUSHED)
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

func (s *SSTManager) QueryKey(key string) (*KVData, error) {
	s.mu.RLock()
	levels := s.levels
	s.mu.RUnlock()

	var data KVData
	for _, level := range levels {
		level.mu.RLock()

		// TODO: SSTs doesn't seem to be sorted in the intended way when flushed
		for _, sst := range level.ssts {
			data, err := sst.FindKey(key)
			if err != nil {
				level.mu.RUnlock()
				return nil, err
			}
			if data != nil {
				level.mu.RUnlock()
				return &KVData{
					Key:       data.Key,
					Value:     data.Value,
					IsDeleted: data.IsDeleted,
				}, nil
			}
		}

		level.mu.RUnlock()
	}

	return &data, nil
}

// Cleans compacted sst
func (s *SSTManager) StartCleaner(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			levels := s.levels
			s.mu.RUnlock()

			for level := range len(levels) {
				ssts := s.ListSST(
					level,
					[]SSTState{SST_COMPACTED},
					MAX_SST_PER_LEVEL,
				)

				if len(ssts) < MAX_SST_PER_LEVEL {
					break
				}

				s.RemoveSST(level, ssts)

				// cleanup files
				for _, sst := range ssts {
					err := os.Remove(path.Join(baseDir, sst.FileName))
					if err != nil {
						log.Print(err)
					}
				}
			}
		}
	}
}
