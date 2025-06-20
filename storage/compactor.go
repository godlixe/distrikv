package storage

type Compactor struct {
	Level      int
	sstManager *SSTManager
}

type CompactorManager struct {
	sstManager *SSTManager
	compactors []Compactor
}

func NewCompactorManager(
	sstManager *SSTManager,
) *CompactorManager {
	return &CompactorManager{}
}

func (c *CompactorManager) StartCompaction() {
	// TODO: will query for how many levels (n) of sst
	// there currently is and start n numbers
	// of goroutine to monitor each level.
	// will also have a goroutine to poll the sst manager
	// about total levels and add more compactors

}
