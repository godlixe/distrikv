package storage

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

const MAX_SST_PER_LEVEL = 2

type kvEntry struct {
	key    string
	value  string
	fileID int
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
	Level      int
	sstManager *SSTManager
}

func NewCompactor(level int, sstManager *SSTManager) *Compactor {
	return &Compactor{
		Level:      level,
		sstManager: sstManager,
	}
}

type CompactorManager struct {
	sstManager *SSTManager
	compactors []Compactor
}

func NewCompactorManager(
	sstManager *SSTManager,
) *CompactorManager {
	return &CompactorManager{
		sstManager: sstManager,
	}
}

func (c *CompactorManager) StartCompactors(ctx context.Context) {
	log.Print("starting compactors")
	// TODO: will query for how many levels (n) of sst
	// there currently is and start n numbers
	// of goroutine to monitor each level.
	// will also have a goroutine to poll the sst manager
	// about total levels and add more compactors

	totalLevels := c.sstManager.GetLevels()
	fmt.Println("total levels: ", totalLevels)

	for idx := range totalLevels {
		compactor := NewCompactor(idx, c.sstManager)
		c.compactors = append(c.compactors, *compactor)
		go compactor.startCompactor(ctx)
	}

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
				log.Print("error compacting sst: ", err)
			}

			// update sst to be deleted
			err = c.sstManager.updateBatch(
				c.Level,
				ssts,
				SST_COMPACTED,
			)
			if err != nil {
				log.Print("error updating sst: ", err)
			}

		}
	}
}

func (c *Compactor) compact(ssts []*SST) error {
	var scanners []*bufio.Scanner
	var files []*os.File

	for _, sst := range ssts {
		f, err := os.Open(sst.FileName)
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
				log.Println("error closing file: ", err)
			}
		}
	}()

	h := &kvHeap{}

	heap.Init(h)

	for idx, scanner := range scanners {
		if scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), ":", 2)
			if len(parts) == 2 {
				heap.Push(h, &kvEntry{
					key:    parts[0],
					value:  parts[1],
					fileID: idx,
				})
			}
		}
	}

	outSST := NewSST(c.Level+1, SST_COMPACTING)
	outFile, err := os.Create(path.Join(baseDir, outSST.FileName))
	if err != nil {
		return err
	}
	defer outFile.Close()

	outWriter := bufio.NewWriter(outFile)

	defer func() {
		if err := outWriter.Flush(); err != nil {
			log.Println("flush failed: ", err)
		}
	}()

	var lastKey string

	for h.Len() > 0 {
		entry := heap.Pop(h).(*kvEntry)

		// FIFO setup, first unique key to be found is consider the latest
		if entry.key != lastKey {
			_, err := outWriter.WriteString(fmt.Sprintf("%s:%s\n", entry.key, entry.value))
			if err != nil {
				return err
			}
			lastKey = entry.key
		}

		scanner := scanners[entry.fileID]
		if scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), ":", 2)
			if len(parts) == 2 {
				heap.Push(h, &kvEntry{
					key:    parts[0],
					value:  parts[1],
					fileID: entry.fileID,
				})
			}
		}
	}

	_, err = outWriter.WriteString(fmt.Sprintf("<metadata>\nlevel: %d\ntimestamp: %s\n<sst_done>", c.Level+1, time.Now().Format(time.RFC3339)))
	if err != nil {
		return err
	}

	return err
}
