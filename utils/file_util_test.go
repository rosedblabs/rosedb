package utils

import (
	"os"
	"rosedb/storage"
	"testing"
)

func TestExist(t *testing.T) {
	t.Log(os.TempDir() + "ssds")

	exist := Exist(os.TempDir() + "ssds")
	t.Log(exist)

	if err := os.MkdirAll(os.TempDir()+"abcd", storage.FilePerm); err != nil {
		t.Error(err)
	}
}
