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
					t.Errorf("RPush() error = %v, wantErr %v", err, tt.wantErr)
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

func TestRoseDB_LMove(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBLMove(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBLMove(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBLMove(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	srcListKey := []byte("src_list")
	dstListKey := []byte("dst_list")
	v, err := db.LMove(srcListKey, dstListKey, true, true)
	assert.Nil(t, v)
	assert.Nil(t, err)

	err = db.RPush(srcListKey, GetKey(1), GetKey(2), GetKey(3), GetKey(4), GetKey(5))
	assert.Nil(t, err)

	// left-pop left-push
	v, err = db.LMove(srcListKey, dstListKey, true, true)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(1))
	// src[2, 3, 4, 5]	dst[1]

	// left-pop right-push
	v, err = db.LMove(srcListKey, dstListKey, true, false)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(2))
	// src[3, 4, 5]		dst[1, 2]

	// right-pop left-push
	v, err = db.LMove(srcListKey, dstListKey, false, true)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(5))
	// src[3, 4]		dst[5, 1, 2]

	// right-pop right-push
	v, err = db.LMove(srcListKey, dstListKey, false, false)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(4))
	// src[3]		dst[5, 1, 2, 4]

	v, err = db.LIndex(dstListKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(5))

	v, err = db.LIndex(dstListKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(1))

	v, err = db.LIndex(dstListKey, 2)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(2))

	v, err = db.LIndex(dstListKey, 3)
	assert.Nil(t, err)
	assert.Equal(t, v, GetKey(4))
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

func TestRoseDB_LPushX(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPushX(t, true, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPushX(t, true, MMap, KeyValueMemMode)
	})
}

func TestRoseDB_RPushX(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBPushX(t, false, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBPushX(t, false, MMap, KeyValueMemMode)
	})
}

func testRoseDBPushX(t *testing.T, isLPush bool, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.LPush(GetKey(1), []byte("1"))
	assert.Nil(t, err)
	err = db.LPush(GetKey(2), []byte("1"))
	assert.Nil(t, err)

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
			"nil-key", db, args{key: GetKey(0), values: [][]byte{GetValue16B()}}, true,
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
			if isLPush {
				if err := tt.db.LPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPushX() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err := tt.db.RPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("RPushX() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
func TestRoseDB_LIndex(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBRLIndex(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBRLIndex(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBRLIndex(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	listKey := []byte("my_list")
	v, err := db.LIndex(listKey, 0)
	assert.Nil(t, v)
	assert.Nil(t, err)

	// one
	err = db.RPush(listKey, GetKey(1))
	assert.Nil(t, err)

	lVal1, err := db.LIndex(listKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, lVal1, GetKey(1))

	rVal1, err := db.LIndex(listKey, -1)
	assert.Nil(t, err)
	assert.Equal(t, rVal1, GetKey(1))

	// out of right range with one
	rOut1, err := db.LIndex(listKey, 1)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, rOut1)

	// out of left range with one
	lOut1, err := db.LIndex(listKey, -2)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, lOut1)

	// two
	err = db.RPush(listKey, GetKey(2))
	assert.Nil(t, err)

	lVal1, err = db.LIndex(listKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, lVal1, GetKey(1))

	lVal2, err := db.LIndex(listKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, lVal2, GetKey(2))

	rVal1, err = db.LIndex(listKey, -2)
	assert.Nil(t, err)
	assert.Equal(t, rVal1, GetKey(1))

	rVal2, err := db.LIndex(listKey, -1)
	assert.Nil(t, err)
	assert.Equal(t, rVal2, GetKey(2))

	// out of right range with two
	rOut2, err := db.LIndex(listKey, 2)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, rOut2)

	// out of left range with two
	lOut2, err := db.LIndex(listKey, -3)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, lOut2)
}

func TestRoseDB_LSet(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBLSet(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBLSet(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBLSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	// none
	listKey := []byte("my_list")
	err = db.LSet(listKey, 0, GetKey(1))
	assert.Equal(t, err, ErrKeyNotFound)

	// one
	err = db.RPush(listKey, GetKey(1))
	assert.Nil(t, err)
	err = db.LSet(listKey, 0, GetKey(111))
	assert.Nil(t, err)
	lPop, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, GetKey(111), lPop)

	// three
	err = db.RPush(listKey, GetKey(1))
	assert.Nil(t, err)
	err = db.RPush(listKey, GetKey(2))
	assert.Nil(t, err)
	err = db.RPush(listKey, GetKey(3))
	assert.Nil(t, err)
	err = db.LSet(listKey, 0, GetKey(111))
	assert.Nil(t, err)
	err = db.LSet(listKey, 1, GetKey(222))
	assert.Nil(t, err)
	err = db.LSet(listKey, -1, GetKey(333))
	assert.Nil(t, err)
	lPop, err = db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, GetKey(111), lPop)
	lPop, err = db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, GetKey(222), lPop)
	lPop, err = db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, GetKey(333), lPop)

	// out of range
	err = db.RPush(listKey, GetKey(1))
	assert.Nil(t, err)
	err = db.LSet(listKey, 1, GetKey(111))
	assert.Equal(t, err, ErrWrongIndex)
	err = db.LSet(listKey, -2, GetKey(111))
	assert.Equal(t, err, ErrWrongIndex)
}
