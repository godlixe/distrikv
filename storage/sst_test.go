package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeAndParseSSTEntry(t *testing.T) {
	var buf bytes.Buffer

	original := SSTEntry{
		Key:       "foo",
		Value:     "bar",
		IsDeleted: true,
	}

	err := encodeSSTEntry(&buf, original.Key, original.Value, original.IsDeleted)
	assert.NoError(t, err)
	fmt.Println(buf)

	parsed, err := parseSSTLine(buf.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, original.Key, parsed.Key)
	assert.Equal(t, original.Value, parsed.Value)
	assert.Equal(t, original.IsDeleted, parsed.IsDeleted)
}
