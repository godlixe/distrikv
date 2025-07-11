package main

import (
	"context"
	"distrikv/api"
	"distrikv/storage"
)

func main() {
	sstManager, err := storage.NewSSTManager()
	if err != nil {
		panic(err)
	}

	go sstManager.StartCleaner(context.Background())

	compactorManager := storage.NewCompactorManager(sstManager)

	compactorManager.StartCompactors(context.Background())

	store := storage.NewStore(sstManager)

	api.Start(&store)
}
