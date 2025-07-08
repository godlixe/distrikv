package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

var ErrSSTEntryEOF error = errors.New("sst eof reached")

// SST File Format
// [TotalLength][KeyLength][Key][ValLength][Val][IsDeleted]
// ...
// ...
// <metadata>
// level [level]
// timestamp [creation timestamp]
// <sst_done> (just a marker for marking that a sst is done made)

type SSTEntry struct {
	Key       string
	Value     string
	IsDeleted bool
}

type SST struct {
	ID        uint64
	FileName  string
	Level     int
	Timestamp time.Time
	Status    SSTState
}

func (s *SST) FindKey(key string) (*SSTEntry, error) {
	f, err := os.Open(path.Join(baseDir, s.FileName))
	if err != nil {
		return nil, err
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		// TODO: Binary search the file for key
		entry, err := parseSSTLine(scanner.Bytes())
		if err != nil && err != ErrSSTEntryEOF {
			return nil, err
		}

		// return if SST EOF reached
		if errors.Is(err, ErrSSTEntryEOF) {
			return nil, nil
		}

		if entry != nil && entry.Key == key {
			return entry, nil
		}
	}

	return nil, nil
}

// Writes the SST Content to w
func (s SST) DecodeSST(w io.Writer) error {
	return nil
}

func encodeSSTEntry(w io.Writer, key string, value string, isDeleted bool) error {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	var isDeletedByte byte = 0

	if isDeleted {
		isDeletedByte = 1
	}

	totalLength := 4 + 4 + 4 + 1 + len(keyBytes) + len(valBytes)

	if err := binary.Write(w, binary.LittleEndian, uint32(totalLength)); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return err
	}

	if _, err := w.Write(keyBytes); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, uint32(len(valBytes))); err != nil {
		return err
	}

	if _, err := w.Write(valBytes); err != nil {
		return err
	}

	if _, err := w.Write([]byte{isDeletedByte}); err != nil {
		return err
	}

	// TODO: should this have a newline?
	if _, err := w.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

func writeSSTMetadata(w io.Writer, id uint64, level int, timestamp time.Time) error {
	metadata := fmt.Sprintf("\n<metadata>\nlevel: %d\ntimestamp: %s\nid: %d\n<sst_done>", level, timestamp.Format(time.RFC3339), id)
	if _, err := w.Write([]byte(metadata)); err != nil {
		return err
	}

	return nil
}

func parseSSTLine(line []byte) (*SSTEntry, error) {
	if len(line) == 0 {
		return nil, ErrSSTEntryEOF
	}

	if len(line) < 13 {
		return nil, errors.New("line too short")
	}

	var totalLength uint32
	var keyLength uint32
	var valLength uint32
	var key string
	var value string
	var isDeletedByte byte

	// first 4 bytes is the key length
	totalLength = binary.LittleEndian.Uint32(line[0:4])

	if len(line) != int(totalLength) {
		return nil, errors.New("data length is incorrect")
	}
	// next 4 bytes is the key length
	keyLength = binary.LittleEndian.Uint32(line[4:8])

	// next keyLength bytes is the key
	key = string(line[8 : 8+keyLength])

	// next 4 bytes is the value length
	valLength = binary.LittleEndian.Uint32(line[8+keyLength : 12+keyLength])

	// next valLength bytes is the value length
	value = string(line[12+keyLength : 12+keyLength+valLength])

	// last byte is the isDeleted
	isDeletedByte = line[len(line)-1]

	var isDeleted bool = false
	if isDeletedByte == 1 {
		isDeleted = true
	}

	return &SSTEntry{
		Key:       key,
		Value:     value,
		IsDeleted: isDeleted,
	}, nil
}

func parseSSTMetadata(filename string) (*SST, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	// seek to bottom of the file
	// to find metadata.
	maxMetadataSize := 512
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	readSize := int64(maxMetadataSize)
	if size < readSize {
		readSize = size
	}

	buf := make([]byte, readSize)

	_, err = f.Seek(-readSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	_, err = f.Read(buf)
	if err != nil {
		return nil, err
	}

	// parse metadata from buffer
	lines := strings.Split(string(buf), "\n")
	var level int
	var ts time.Time
	var id uint64

	if lines[len(lines)-1] != "<sst_done>" {
		return nil, ErrSSTIncomplete
	}

	for _, line := range lines {
		if strings.HasPrefix(line, "level: ") {
			fmt.Sscanf(line, "level: %d", &level)
		} else if strings.HasPrefix(line, "timestamp: ") {
			var t string
			fmt.Sscanf(line, "timestamp: %s", &t)
			parsed, err := time.Parse(time.RFC3339, t)
			if err != nil {
				log.Println("error parsing sst")
			}
			ts = parsed
		} else if strings.HasPrefix(line, "id: ") {
			fmt.Sscanf(line, "id: %d", &id)
		} else {
			break
		}
	}

	return &SST{
		ID:        id,
		FileName:  filename,
		Level:     level,
		Timestamp: ts,
		Status:    SST_FLUSHED,
	}, nil
}
