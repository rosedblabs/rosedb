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

func TestRoseDB_SMembers(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBSMembers(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBSMembers(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBSMembers(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	mems1, err := db.SMembers(setKey)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mems1))

	err = db.SAdd(setKey, GetKey(0), GetKey(1), GetKey(2))
	assert.Nil(t, err)
	mems2, err := db.SMembers(setKey)
	assert.Equal(t, 3, len(mems2))

	err = db.SRem(setKey, GetKey(2))
	assert.Nil(t, err)
	mems3, err := db.SMembers(setKey)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mems3))
}

func TestRoseDB_SCard(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	c1 := db.SCard(setKey)
	assert.Equal(t, 0, c1)

	err = db.SAdd(setKey, GetKey(0), GetKey(1), GetKey(2))
	assert.Nil(t, err)

	c2 := db.SCard(setKey)
	assert.Equal(t, 3, c2)

	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	c3 := db2.SCard(setKey)
	assert.Equal(t, 3, c3)
}

func TestRoseDB_DiscardStat_Sets(t *testing.T) {
	helper := func(isDelete bool) {
		path := filepath.Join("/tmp", "rosedb")
		opts := DefaultOptions(path)
		opts.LogFileSizeThreshold = 64 << 20
		db, err := Open(opts)
		assert.Nil(t, err)
		defer destroyDB(db)

		setKey := []byte("my_set")
		writeCount := 500000
		for i := 0; i < writeCount; i++ {
			err := db.SAdd(setKey, GetKey(i))
			assert.Nil(t, err)
		}

		if isDelete {
			for i := 0; i < writeCount/2; i++ {
				err := db.SRem(setKey, GetKey(i))
				assert.Nil(t, err)
			}
		} else {
			for i := 0; i < writeCount/2; i++ {
				err := db.SAdd(setKey, GetKey(i))
				assert.Nil(t, err)
			}
		}
		_ = db.Sync()
		ccl, err := db.discards[Set].getCCL(10, 0.1)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(ccl))
	}

	t.Run("rewrite", func(t *testing.T) {
		helper(false)
	})

	t.Run("delete", func(t *testing.T) {
		helper(true)
	})
}

func TestRoseDB_SetGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 64 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	writeCount := 500000
	for i := 0; i < writeCount; i++ {
		err := db.SAdd(setKey, GetValue128B())
		assert.Nil(t, err)
	}

	_, err = db.SPop(setKey, uint(writeCount/2))
	assert.Nil(t, err)

	err = db.RunLogFileGC(Set, 0, 0.1)
	assert.Nil(t, err)

	c1 := db.SCard(setKey)
	assert.Equal(t, writeCount/2, c1)
}
