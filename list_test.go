package rosedb

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestRoseDB_LPush(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPush(t, true, FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPush(t, true, MMap)
	})
}

func TestRoseDB_RPush(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPush(t, false, FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPush(t, false, MMap)
	})
}

func TestRoseDB_Push_UntilRotateFile(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	writeCount := 600000
	key := []byte("mylist")
	for i := 0; i <= writeCount; i++ {
		err := db.LPush(key, GetValue128B())
		assert.Nil(t, err)
	}
}

func testRoseDBPush(t *testing.T, isLush bool, ioType IOType) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *RoseDB
		args    args
		wantErr bool
	}{
		{
			"nil-value", db, args{key: GetKey(0), values: [][]byte{GetValue16B()}}, false,
		},
		{
			"one-value", db, args{key: GetKey(1), values: [][]byte{GetValue16B()}}, false,
		},
		{
			"multi-value", db, args{key: GetKey(2), values: [][]byte{GetValue16B(), GetValue16B(), GetValue16B()}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isLush {
				if err := tt.db.LPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err := tt.db.RPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
