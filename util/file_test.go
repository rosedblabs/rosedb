package util

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestPathExist(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "path", "lotusdb-1"))
	assert.Nil(t, err)
	path2, err := filepath.Abs(filepath.Join("/tmp", "path", "lotusdb-2"))
	assert.Nil(t, err)

	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		err := os.RemoveAll(filepath.Join("/tmp", "path"))
		assert.Nil(t, err)
	}()

	existedFile, err := filepath.Abs(filepath.Join("/tmp", "path", "lotusdb-file1"))
	assert.Nil(t, err)
	noExistedFile, err := filepath.Abs(filepath.Join("/tmp", "path", "lotusdb-file2"))
	assert.Nil(t, err)
	f, err := os.OpenFile(existedFile, os.O_CREATE, 0644)
	assert.Nil(t, err)
	defer func() {
		_ = f.Close()
	}()

	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"path exist", args{path: path}, true,
		},
		{
			"path not exist", args{path: path2}, false,
		},
		{
			"file exist", args{path: existedFile}, true,
		},
		{
			"file not exist", args{path: noExistedFile}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathExist(tt.args.path); got != tt.want {
				t.Errorf("PathExist() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCopyDir(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "test-copy-path"))
	assert.Nil(t, err)
	destPath, err := filepath.Abs(filepath.Join("/tmp", "test-copy-path-dest"))
	assert.Nil(t, err)

	subpath1 := path + string(os.PathSeparator) + "sub1"
	subpath2 := path + string(os.PathSeparator) + "sub2"
	subFile := path + string(os.PathSeparator) + "sub-file"

	err = os.MkdirAll(subpath1, os.ModePerm)
	assert.Nil(t, err)
	err = os.MkdirAll(subpath2, os.ModePerm)
	assert.Nil(t, err)
	f, err := os.OpenFile(subFile, os.O_CREATE, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(path)
		_ = os.RemoveAll(destPath)
	}()

	err = CopyDir(path, destPath)
	assert.Nil(t, err)
}

func TestCopyFile(t *testing.T) {
	path := filepath.Join("/tmp", "path", "lotusdb-1")
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(filepath.Join("/tmp", "path"))
	}()

	file := filepath.Join(path, "001.vlog")
	f, err := os.OpenFile(file, os.O_CREATE, 0644)
	assert.Nil(t, err)

	err = CopyFile(file, path+"/001.vlog-bak")
	assert.Nil(t, err)

	_ = f.Close()
}
