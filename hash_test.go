package rosedb

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestRoseDB_HSet(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSet(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSet(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key   []byte
		field []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil", db, args{key: nil, field: nil, value: GetKey(123)}, false,
		},
		{
			"nil-value", db, args{key: GetKey(1), field: GetKey(11), value: nil}, false,
		},
		{
			"normal", db, args{key: GetKey(1), field: GetKey(11), value: GetValue16B()}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.HSet(tt.args.key, tt.args.field, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("HSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRoseDB_HGet(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHGet(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBHGet(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	err = db.HSet(setKey, GetKey(1), GetKey(111))
	assert.Nil(t, err)
	v1, err := db.HGet(setKey, GetKey(1))
	assert.Nil(t, err)
	assert.Equal(t, GetKey(111), v1)

	err = db.HSet(setKey, GetKey(1), GetKey(222))
	assert.Nil(t, err)

	v2, err := db.HGet(setKey, GetKey(1))
	assert.Nil(t, err)
	assert.Equal(t, GetKey(222), v2)
}

func TestRoseDB_HDel(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHDel(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBHDel(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHDel(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// not exist
	setKey := []byte("my_set")
	c1, err := db.HDel(setKey, GetKey(1), GetKey(2))
	assert.Nil(t, err)
	assert.Equal(t, 0, c1)

	err = db.HSet(setKey, GetKey(1), GetValue16B())
	assert.Nil(t, err)
	err = db.HSet(setKey, GetKey(2), GetValue16B())
	assert.Nil(t, err)
	err = db.HSet(setKey, GetKey(3), GetValue16B())
	assert.Nil(t, err)

	c2, err := db.HDel(setKey, GetKey(3))
	assert.Nil(t, err)
	assert.Equal(t, 1, c2)

	v1, err := db.HGet(setKey, GetKey(3))
	assert.Nil(t, err)
	assert.Nil(t, v1)
}
