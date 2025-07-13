package main

import (
	"context"
	"distrikv/api"
	"distrikv/storage"
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	sstManager, err := storage.NewSSTManager(logger)
	if err != nil {
		panic(err)
	}

	go sstManager.StartCleaner(context.Background())

	compactorManager := storage.NewCompactorManager(logger, sstManager)

	compactorManager.StartCompactors(context.Background())

	store := storage.NewStore(logger, sstManager)

	api.Start(&store)
}
