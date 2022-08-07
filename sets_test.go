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
	defer func() {
		_ = db2.Close()
	}()
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
		err := db.SAdd(setKey, GetValue16B())
		assert.Nil(t, err)
	}

	_, err = db.SPop(setKey, uint(writeCount/2))
	assert.Nil(t, err)

	err = db.RunLogFileGC(Set, 0, 0.1)
	assert.Nil(t, err)

	c1 := db.SCard(setKey)
	assert.Equal(t, writeCount/2, c1)
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

func TestRoseDB_SDiffStore(t *testing.T) {
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
			name:       "two key parameter",
			db:         db,
			keys:       [][]byte{[]byte("destination1"), []byte("key-2")},
			expDiffSet: [][]byte{[]byte("value-5"), []byte("value-4"), []byte("value-6"), []byte("value-7")}, // todo check
			expErr:     nil,
		},
		{
			name:       "three key parameters",
			db:         db,
			keys:       [][]byte{[]byte("destination2"), []byte("key-1"), []byte("key-3")},
			expDiffSet: [][]byte{[]byte("value-3"), []byte("value-1")},
			expErr:     nil,
		},
		{
			name:       "four key parameters",
			db:         db,
			keys:       [][]byte{[]byte("destination3"), []byte("key-1"), []byte("key-2"), []byte("key-3")},
			expDiffSet: [][]byte{[]byte("value-3"), []byte("value-1")},
			expErr:     nil,
		},
		{
			name:       "no diff",
			db:         db,
			keys:       [][]byte{[]byte("destination4"), []byte("key-1"), []byte("key-4")},
			expDiffSet: nil,
			expErr:     nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			count, err := db.SDiffStore(tc.keys...)
			assert.Nil(t, err)
			actual, err := db.sMembers(tc.keys[0])
			assert.Equal(t, tc.expErr, err)
			assert.Equal(t, tc.expDiffSet, actual)
			assert.Equal(t, len(actual), count)
		})
	}
}

func TestRoseDB_SUnion(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.SAdd([]byte("key-1"), []byte("value-1"), []byte("value-2"), []byte("value-3"))
	_ = db.SAdd([]byte("key-2"), []byte("value-4"), []byte("value-5"), []byte("value-6"), []byte("value-7"))
	_ = db.SAdd([]byte("key-3"), []byte("value-2"), []byte("value-5"), []byte("value-8"), []byte("value-9"))
	testCases := []struct {
		name        string
		db          *RoseDB
		keys        [][]byte
		expUnionSet [][]byte
		expErr      error
	}{
		{
			name:        "empty key parameters",
			db:          db,
			keys:        [][]byte{},
			expUnionSet: nil,
			expErr:      ErrWrongNumberOfArgs,
		},
		{
			name:        "one key parameter",
			db:          db,
			keys:        [][]byte{[]byte("key-1")},
			expUnionSet: [][]byte{[]byte("value-3"), []byte("value-2"), []byte("value-1")},
			expErr:      nil,
		},
		{
			name: "two key parameters",
			db:   db,
			keys: [][]byte{[]byte("key-1"), []byte("key-2")},
			expUnionSet: [][]byte{[]byte("value-3"), []byte("value-2"), []byte("value-1"),
				[]byte("value-5"), []byte("value-4"), []byte("value-6"), []byte("value-7")},
			expErr: nil,
		},
		{
			name: "multiple key parameters",
			db:   db,
			keys: [][]byte{[]byte("key-1"), []byte("key-2"), []byte("key-3")},
			expUnionSet: [][]byte{[]byte("value-3"), []byte("value-2"), []byte("value-1"),
				[]byte("value-5"), []byte("value-4"), []byte("value-6"), []byte("value-7"),
				[]byte("value-8"), []byte("value-9")},
			expErr: nil,
		},
		{
			name:        "no union",
			db:          db,
			keys:        [][]byte{[]byte("key-10"), []byte("key-20")},
			expUnionSet: [][]byte{},
			expErr:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unionSet, err := db.SUnion(tc.keys...)
			assert.Equal(t, tc.expErr, err)
			assert.Equal(t, tc.expUnionSet, unionSet)
		})
	}
}
func TestRoseDB_SInter(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.SAdd([]byte("key-1"), []byte("value-1"), []byte("value-2"), []byte("value-3"))
	_ = db.SAdd([]byte("key-2"), []byte("value-1"), []byte("value-2"), []byte("value-3"), []byte("value-4"))
	_ = db.SAdd([]byte("key-3"), []byte("value-4"), []byte("value-5"), []byte("value-6"), []byte("value-7"))
	_ = db.SAdd([]byte("key-4"), []byte("value-4"), []byte("value-5"))
	testCases := []struct {
		name        string
		db          *RoseDB
		keys        [][]byte
		expInterSet [][]byte
		expErr      error
	}{
		{
			name:        "empty key parameters",
			db:          db,
			keys:        [][]byte{},
			expInterSet: nil,
			expErr:      ErrWrongNumberOfArgs,
		},
		{
			name:        "one key parameter",
			db:          db,
			keys:        [][]byte{[]byte("key-1")},
			expInterSet: [][]byte{[]byte("value-3"), []byte("value-2"), []byte("value-1")},
			expErr:      nil,
		},
		{
			name:        "two key parameters key-1 key-2",
			db:          db,
			keys:        [][]byte{[]byte("key-1"), []byte("key-2")},
			expInterSet: [][]byte{[]byte("value-3"), []byte("value-2"), []byte("value-1")},
			expErr:      nil,
		},
		{
			name:        "two key parameters key-1 key-3",
			db:          db,
			keys:        [][]byte{[]byte("key-1"), []byte("key-3")},
			expInterSet: [][]byte{},
			expErr:      nil,
		},
		{
			name:        "multiple key parameters key-1 key-2 key-3",
			db:          db,
			keys:        [][]byte{[]byte("key-1"), []byte("key-2"), []byte("key-3")},
			expInterSet: [][]byte{},
			expErr:      nil,
		},
		{
			name:        "multiple key parameters  key-2 key-3 key-4",
			db:          db,
			keys:        [][]byte{[]byte("key-4"), []byte("key-2"), []byte("key-3")},
			expInterSet: [][]byte{[]byte("value-4")},
			expErr:      nil,
		},
		{
			name:        "no Inter",
			db:          db,
			keys:        [][]byte{[]byte("key-10"), []byte("key-20")},
			expInterSet: [][]byte{},
			expErr:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unionSet, err := db.SInter(tc.keys...)
			assert.Equal(t, tc.expErr, err)
			assert.Equal(t, tc.expInterSet, unionSet)
		})
	}
}
