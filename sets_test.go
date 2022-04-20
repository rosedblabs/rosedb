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

func TestRoseDB_SDiff(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.SAdd([]byte("key-1"), []byte("value-1"), []byte("value-2"), []byte("value-3"))
	_ = db.SAdd([]byte("key-2"), []byte("value-4"), []byte("value-5"), []byte("value-6"), []byte("value-7"))
	_ = db.SAdd([]byte("key-3"), []byte("value-2"), []byte("value-5"), []byte("value-8"), []byte("value-9"))
	_ = db.SAdd([]byte("key-4"), []byte("value-1"), []byte("value-2"), []byte("value-3"))
	testCases := []struct {
		name       string
		db         *RoseDB
		keys       [][]byte
		expDiffSet [][]byte
		expErr     error
	}{
		{
			name:       "empty key parameters",
			db:         db,
			keys:       [][]byte{},
			expDiffSet: nil,
			expErr:     ErrWrongNumberOfArgs,
		},
		{
			name:       "one key parameter",
			db:         db,
			keys:       [][]byte{[]byte("key-2")},
			expDiffSet: [][]byte{[]byte("value-5"), []byte("value-4"), []byte("value-6"), []byte("value-7")}, // todo check
			expErr:     nil,
		},
		{
			name:       "two key parameters",
			db:         db,
			keys:       [][]byte{[]byte("key-1"), []byte("key-3")},
			expDiffSet: [][]byte{[]byte("value-3"), []byte("value-1")},
			expErr:     nil,
		},
		{
			name:       "multiple key parameters",
			db:         db,
			keys:       [][]byte{[]byte("key-1"), []byte("key-2"), []byte("key-3")},
			expDiffSet: [][]byte{[]byte("value-3"), []byte("value-1")},
			expErr:     nil,
		},
		{
			name:       "no diff",
			db:         db,
			keys:       [][]byte{[]byte("key-1"), []byte("key-4")},
			expDiffSet: [][]byte{},
			expErr:     nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			diffSet, err := db.SDiff(tc.keys...)
			assert.Equal(t, tc.expErr, err)
			assert.Equal(t, tc.expDiffSet, diffSet)
		})
	}
}
