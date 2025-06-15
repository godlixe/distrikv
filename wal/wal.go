package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type WALEntry struct {
	CRC     uint32
	Content [32]byte
}

func (e *WALEntry) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	crcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBytes, e.CRC)

	buf.Write(crcBytes)
	buf.Write(e.Content[:])

	return buf.Bytes(), nil
}

func decodeWALEntry(data []byte) (*WALEntry, error) {
	if len(data) < 36 {
		return nil, fmt.Errorf("data too short")
	}

	var e WALEntry

	e.CRC = binary.LittleEndian.Uint32(data[:4])

	copy(e.Content[:], data[4:])

	return &e, nil
}
