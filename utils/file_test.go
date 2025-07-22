package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)
	assert.NoError(t, err)
	assert.Positive(t, dirSize)
}
