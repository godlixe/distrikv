package main

import (
	"distrikv/cli"
	"distrikv/storage"
	"fmt"
)

func main() {
	sstMgr, err := storage.NewSSTManager()
	if err != nil {
		panic(err)
	}

	fmt.Println(sstMgr)
	cli.Start()
}
