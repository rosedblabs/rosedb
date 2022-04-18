package rosedb

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestRoseDB_SAdd(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBSAdd(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBSAdd(t, MMap, KeyOnlyMemMode)
	})
}

func testRoseDBSAdd(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key     []byte
		members [][]byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"normal-1", db, args{key: GetKey(1), members: [][]byte{GetValue16B()}}, false,
		},
		{
			"normal-2", db, args{key: GetKey(1), members: [][]byte{GetValue16B()}}, false,
		},
	}
	for _, tt := range tests {
		if err := tt.db.SAdd(tt.args.key, tt.args.members...); (err != nil) != tt.wantErr {
			t.Errorf("SAdd() error = %v, wantErr %v", err, tt.wantErr)
		}
	}
}

func TestRoseDB_SPop(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBSPop(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBSPop(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBSPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	err = db.SAdd(setKey, GetKey(1), GetKey(2), GetKey(3))
	assert.Nil(t, err)

	c1 := db.SCard(setKey)
	assert.Equal(t, 3, c1)

	p1, err := db.SPop(setKey, 4)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(p1))
	p2, err := db.SPop(setKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(p2))

	c2 := db.SCard(setKey)
	assert.Equal(t, 0, c2)
}

func TestRoseDB_SRem(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBSRem(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBSRem(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBSRem(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	err = db.SAdd(setKey, GetKey(1), GetKey(2), GetKey(3))
	assert.Nil(t, err)

	err = db.SRem(setKey, GetKey(1))
	assert.Nil(t, err)

	ok1 := db.SIsMember(setKey, GetKey(1))
	assert.False(t, ok1)

	ok2 := db.SIsMember(setKey, GetKey(2))
	assert.True(t, ok2)
}

func TestRoseDB_SIsMember(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBSIsMember(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBSIsMember(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBSIsMember(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	ok1 := db.SIsMember(setKey, GetKey(1))
	assert.False(t, ok1)

	err = db.SAdd(setKey, GetKey(1), GetKey(2), GetKey(3))
	assert.Nil(t, err)

	ok2 := db.SIsMember(setKey, GetKey(3))
	assert.True(t, ok2)
}
