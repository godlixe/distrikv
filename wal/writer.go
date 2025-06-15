package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

type WAL struct {
	file *os.File
}

func New(baseDir string) (*WAL, error) {
	f, err := os.OpenFile(
		path.Join(baseDir, "walwal.wal"),
		os.O_APPEND|os.O_CREATE|os.O_SYNC|os.O_RDWR,
		0744,
	)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
	}, nil
}

// Write writes file to wal file
func (w *WAL) WriteBytes(entry *WALEntry) error {
	composed, err := entry.Encode()
	if err != nil {
		return err
	}

	_, err = w.file.Write(composed)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// fmt.Println("wrote: ", bn)
	return nil
}

// Read reads file per total bytes
func (w *WAL) ReadBytes() ([]WALEntry, error) {
	var entries []WALEntry

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	for {
		b := make([]byte, 36)

		_, err := io.ReadFull(w.file, b)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if errors.Is(err, io.EOF) {
			break
		}

		e, err := decodeWALEntry(b)
		if err != nil {
			return nil, err
		}

		entries = append(entries, *e)
	}

	return entries, nil
}
