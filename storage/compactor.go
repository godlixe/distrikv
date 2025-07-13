package storage

import (
	"bufio"
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"os"
	"path"
	"slices"
	"time"
)

const MAX_SST_PER_LEVEL = 5

type kvEntry struct {
	key       string
	value     string
	isDeleted bool
	fileID    int
}

type kvHeap []*kvEntry

func (h kvHeap) Len() int {
	return len(h)
}

func (h kvHeap) Less(i, j int) bool {
	return h[i].key < h[j].key
}

func (h kvHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *kvHeap) Push(x any) {
	*h = append(*h, x.(*kvEntry))
}

func (h *kvHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type Compactor struct {
	logger     *slog.Logger
	Level      int
	sstManager *SSTManager
}

func NewCompactor(
	logger *slog.Logger,
	level int,
	sstManager *SSTManager,
) *Compactor {
	return &Compactor{
		logger:     logger,
		Level:      level,
		sstManager: sstManager,
	}
}

type CompactorManager struct {
	logger     *slog.Logger
	sstManager *SSTManager
	compactors []Compactor
}

func NewCompactorManager(
	logger *slog.Logger,
	sstManager *SSTManager,
) *CompactorManager {
	return &CompactorManager{
		logger:     logger,
		sstManager: sstManager,
	}
}

func (c *CompactorManager) StartCompactors(ctx context.Context) {
	c.logger.Info("starting compactors")
	// TODO: will query for how many levels (n) of sst
	// there currently is and start n numbers
	// of goroutine to monitor each level.
	// will also have a goroutine to poll the sst manager
	// about total levels and add more compactors

	levels := c.sstManager.GetLevels()

	for _, level := range levels {
		compactor := NewCompactor(c.logger, level, c.sstManager)
		c.compactors = append(c.compactors, *compactor)
		go compactor.startCompactor(ctx)
	}

	go c.startLevelChecker(ctx)
}

func (c *Compactor) startCompactor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ssts := c.sstManager.ListSST(
				c.Level,
				[]SSTState{SST_FLUSHED},
				MAX_SST_PER_LEVEL,
			)

			if len(ssts) < MAX_SST_PER_LEVEL {
				break
			}

			err := c.compact(ssts)
			if err != nil {
				c.logger.Error("error compacting SST", "err", err)
				break
			}

			// update sst to be deleted
			err = c.sstManager.updateBatch(
				c.Level,
				ssts,
				SST_COMPACTED,
			)
			if err != nil {
				c.logger.Error("error updating SST", "err", err)
				break
			}
		}
	}
}

func (c *CompactorManager) GetLevels() []int {
	var levels []int
	for _, compactor := range c.compactors {
		levels = append(levels, compactor.Level)
	}

	return levels
}

func (c *CompactorManager) startLevelChecker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			levels := c.sstManager.GetLevels()
			existingLevels := c.GetLevels()

			for _, level := range levels {
				if !slices.Contains(existingLevels, level) {
					compactor := NewCompactor(c.logger, level, c.sstManager)
					c.compactors = append(c.compactors, *compactor)
					go compactor.startCompactor(ctx)
				}
			}
		}
	}
}

func (c *Compactor) compact(ssts []*SST) error {
	var scanners []*bufio.Scanner
	var files []*os.File

	for _, sst := range ssts {
		f, err := os.Open(path.Join(baseDir, sst.FileName))
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(f)

		scanners = append(scanners, scanner)
		files = append(files, f)
	}

	defer func() {
		for _, f := range files {
			err := f.Close()
			if err != nil {
				c.logger.Error("error closing file", "file", f.Name(), "err", err)
			}
		}
	}()

	h := &kvHeap{}

	heap.Init(h)

	for idx, scanner := range scanners {
		if scanner.Scan() {
			entry, err := parseSSTLine(scanner.Bytes())
			if err != nil {
				return err
			}

			heap.Push(h, &kvEntry{
				key:    entry.Key,
				value:  entry.Value,
				fileID: idx,
			})
		}
	}

	outSST := c.sstManager.NewSST(c.Level+1, SST_COMPACTING)
	outFile, err := os.Create(path.Join(baseDir, outSST.FileName))
	if err != nil {
		return err
	}

	outWriter := bufio.NewWriter(outFile)

	var lastKey string

	for h.Len() > 0 {
		entry := heap.Pop(h).(*kvEntry)

		// FIFO setup, first unique key to be found is consider the latest
		if entry.key != lastKey {
			err := encodeSSTEntry(outWriter, entry.key, entry.value, entry.isDeleted)
			if err != nil {
				return err
			}
			lastKey = entry.key
		}

		// advance entry scanner
		scanner := scanners[entry.fileID]
		if scanner.Scan() {
			sstEntry, err := parseSSTLine(scanner.Bytes())
			if err != nil && !errors.Is(err, ErrSSTEntryEOF) {
				return err
			}

			if errors.Is(err, ErrSSTEntryEOF) {
				continue
			}

			heap.Push(h, &kvEntry{
				key:    sstEntry.Key,
				value:  sstEntry.Value,
				fileID: entry.fileID,
			})
		}
	}

	err = writeSSTMetadata(outWriter, outSST.ID, c.Level+1, time.Now())
	if err != nil {
		return err
	}

	err = outWriter.Flush()
	if err != nil {
		return err
	}

	err = outFile.Close()
	if err != nil {
		return err
	}

	return err
}
