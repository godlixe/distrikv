package storage

// Store is expected to be
// a layer of abstraction to the core storage.
// The core storage will implement LSM, and should be
// accessed through the interface.
type Store struct {
	Backend *LSM
}

func (s *Store) Set(key string, value string) {
	s.Backend.Set(key, value)
}

func (s *Store) Get(key string) (*KVData, error) {
	return s.Backend.Get(key)
}

func (s *Store) Delete(key string) {
	s.Backend.Delete(key)
}

func NewStore() Store {
	return Store{
		Backend: NewLSM(),
	}
}
