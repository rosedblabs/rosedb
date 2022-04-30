package rosedb

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestRoseDB_LPush(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPush(t, true, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPush(t, true, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_RPush(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPush(t, false, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPush(t, false, MMap, KeyValueMemMode)
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

func testRoseDBPush(t *testing.T, isLush bool, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
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

func TestRoseDB_LPop(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBLPop(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBLPop(t, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_RPop(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBRPop(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBRPop(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBLPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	listKey := []byte("my_list")
	pop, err := db.LPop(listKey)
	assert.Nil(t, pop)
	assert.Nil(t, err)

	// one
	err = db.LPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v1, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v1)

	// rpush one
	err = db.RPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v2, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v2)

	//	multi
	err = db.LPush(listKey, GetKey(0), GetKey(1), GetKey(2))
	assert.Nil(t, err)

	var values [][]byte
	for db.LLen(listKey) > 0 {
		v, err := db.LPop(listKey)
		assert.Nil(t, err)
		values = append(values, v)
	}
	expected := [][]byte{GetKey(2), GetKey(1), GetKey(0)}
	assert.Equal(t, expected, values)
}

func testRoseDBRPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	listKey := []byte("my_list")
	pop, err := db.RPop(listKey)
	assert.Nil(t, pop)
	assert.Nil(t, err)

	// one
	err = db.RPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v1, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v1)

	// lpush one
	err = db.LPush(listKey, GetValue16B())
	assert.Nil(t, err)
	v2, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v2)

	//	multi
	err = db.RPush(listKey, GetKey(0), GetKey(1), GetKey(2))
	assert.Nil(t, err)

	var values [][]byte
	for db.LLen(listKey) > 0 {
		v, err := db.RPop(listKey)
		assert.Nil(t, err)
		values = append(values, v)
	}
	expected := [][]byte{GetKey(2), GetKey(1), GetKey(0)}
	assert.Equal(t, expected, values)
}

func TestRoseDB_LLen(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	listKey := []byte("my_list")
	err = db.LPush(listKey, GetValue16B(), GetValue16B(), GetValue16B())
	assert.Nil(t, err)
	assert.Equal(t, 3, db.LLen(listKey))

	// close and reopen
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	err = db2.LPush(listKey, GetValue16B(), GetValue16B(), GetValue16B())
	assert.Nil(t, err)
	assert.Equal(t, 6, db2.LLen(listKey))
}

func TestRoseDB_DiscardStat_List(t *testing.T) {
	helper := func(isDelete bool) {
		path := filepath.Join("/tmp", "rosedb")
		opts := DefaultOptions(path)
		opts.LogFileSizeThreshold = 64 << 20
		db, err := Open(opts)
		assert.Nil(t, err)
		defer destroyDB(db)

		listKey := []byte("my_list")
		writeCount := 800000
		for i := 0; i < writeCount; i++ {
			err := db.LPush(listKey, GetKey(i))
			assert.Nil(t, err)
		}

		for i := 0; i < writeCount/3; i++ {
			if i%2 == 0 {
				_, err := db.LPop(listKey)
				assert.Nil(t, err)
			} else {
				_, err := db.RPop(listKey)
				assert.Nil(t, err)
			}
		}

		_ = db.Sync()
		ccl, err := db.discards[List].getCCL(10, 0.2)
		t.Log(err)
		t.Log(ccl)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(ccl))
	}

	t.Run("delete", func(t *testing.T) {
		helper(true)
	})
}

func TestRoseDB_ListGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 64 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	listKey := []byte("my_list")
	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := db.LPush(listKey, GetKey(i))
		assert.Nil(t, err)
	}

	for i := 0; i < writeCount/3; i++ {
		if i%2 == 0 {
			_, err := db.LPop(listKey)
			assert.Nil(t, err)
		} else {
			_, err := db.RPop(listKey)
			assert.Nil(t, err)
		}
	}

	l1 := db.LLen(listKey)
	assert.Equal(t, writeCount-writeCount/3, l1)

	err = db.RunLogFileGC(List, 0, 0.3)
	assert.Nil(t, err)

	l2 := db.LLen(listKey)
	assert.Equal(t, writeCount-writeCount/3, l2)
}
