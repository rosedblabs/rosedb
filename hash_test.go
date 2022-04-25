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

func TestRoseDB_HSetNX(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSetNX(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHSetNX(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.HSet([]byte("key-1"), []byte("field-1"), []byte("value-1"))
	_ = db.HSet([]byte("key-1"), []byte("field-2"), []byte("value-2"))
	testCases := []struct {
		name   string
		db     *RoseDB
		key    []byte
		field  []byte
		value  []byte
		expRes []byte
		expErr error
	}{
		{
			name:   "Non-exist key",
			db:     db,
			key:    []byte("key-2"),
			field:  []byte("field-2"),
			value:  []byte("value-2"),
			expErr: nil,
		},
		{
			name:   "Exist key",
			db:     db,
			key:    []byte("key-1"),
			field:  []byte("field-3"),
			value:  []byte("value-3"),
			expErr: nil, // todo check
		},
		{
			name:   "Non-exist field",
			db:     db,
			key:    []byte("key-1"),
			field:  []byte("field-3"),
			value:  []byte("value-3"),
			expErr: nil,
		},
		{
			name:   "Exist field",
			db:     db,
			key:    []byte("key-1"),
			field:  []byte("field-2"),
			value:  []byte("value-3"),
			expErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

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

func TestRoseDB_HExists(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBHExists(t, FileIO, KeyOnlyMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testRoseDBHExists(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBHExists(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	setKey := []byte("my_set")
	err = db.HSet(setKey, GetKey(1), GetValue16B())
	assert.Nil(t, err)

	c1, err := db.HExists(setKey, GetKey(1))
	assert.Nil(t, err)
	assert.Equal(t, c1, true)

	c2, err := db.HExists(setKey, GetKey(2))
	assert.Nil(t, err)
	assert.Equal(t, c2, false)
}

func TestRoseDB_HLen(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	hashKey := []byte("my_hash")
	l1 := db.HLen(hashKey)
	assert.Equal(t, 0, l1)

	err = db.HSet(hashKey, GetKey(1), GetValue16B())
	assert.Nil(t, err)
	l2 := db.HLen(hashKey)
	assert.Equal(t, 1, l2)

	err = db.HSet(hashKey, GetKey(1), GetValue128B())
	assert.Nil(t, err)

	err = db.HSet(hashKey, GetKey(2), GetValue16B())
	assert.Nil(t, err)
	l3 := db.HLen(hashKey)
	assert.Equal(t, 2, l3)

	writeCount := 1000
	for i := 0; i < writeCount; i++ {
		err := db.HSet(hashKey, GetKey(i+100), GetValue16B())
		assert.Nil(t, err)
	}
	l4 := db.HLen(hashKey)
	assert.Equal(t, writeCount+2, l4)
}

func TestRoseDB_DiscardStat_Hash(t *testing.T) {
	helper := func(isDelete bool) {
		path := filepath.Join("/tmp", "rosedb")
		opts := DefaultOptions(path)
		opts.LogFileSizeThreshold = 64 << 20
		db, err := Open(opts)
		assert.Nil(t, err)
		defer destroyDB(db)

		hashKey := []byte("my_hash")
		writeCount := 500000
		for i := 0; i < writeCount; i++ {
			err := db.HSet(hashKey, GetKey(i), GetValue128B())
			assert.Nil(t, err)
		}

		if isDelete {
			for i := 0; i < writeCount/2; i++ {
				_, err := db.HDel(hashKey, GetKey(i))
				assert.Nil(t, err)
			}
		} else {
			for i := 0; i < writeCount/2; i++ {
				err := db.HSet(hashKey, GetKey(i), GetValue128B())
				assert.Nil(t, err)
			}
		}
		_ = db.Sync()
		ccl, err := db.discards[Hash].getCCL(10, 0.5)
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

func TestRoseDB_HashGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileSizeThreshold = 64 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	hashKey := []byte("my_hash")
	writeCount := 500000
	for i := 0; i < writeCount; i++ {
		err := db.HSet(hashKey, GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
	for i := 0; i < writeCount/2; i++ {
		_, err := db.HDel(hashKey, GetKey(i))
		assert.Nil(t, err)
	}

	err = db.RunLogFileGC(Hash, 0, 0.4)
	assert.Nil(t, err)

	l1 := db.HLen(hashKey)
	assert.Equal(t, writeCount/2, l1)
}

func TestRoseDB_HKeys(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	hashKey := []byte("my_hash")
	keys, err := db.HKeys(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))

	err = db.HSet(hashKey, GetKey(1), GetValue16B())
	assert.Nil(t, err)
	keys, err = db.HKeys(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, GetKey(1), keys[0])

	err = db.HSet(hashKey, GetKey(1), GetValue128B())
	assert.Nil(t, err)
	keys, err = db.HKeys(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, GetKey(1), keys[0])

	err = db.HSet(hashKey, GetKey(2), GetValue16B())
	assert.Nil(t, err)
	keys, err = db.HKeys(hashKey)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, [][]byte{GetKey(1), GetKey(2)}, keys)

	writeCount := 1000
	for i := 0; i < writeCount; i++ {
		err := db.HSet(hashKey, GetKey(i+100), GetValue16B())
		assert.Nil(t, err)
	}
	keys, err = db.HKeys(hashKey)
	assert.Nil(t, err)
	for i := 0; i < writeCount; i++ {
		assert.Equal(t, GetKey(i+100), keys[i+2])
	}
}

func TestRoseDB_HVals(t *testing.T) {
	cases := []struct {
		IOType
		DataIndexMode
	}{
		{FileIO, KeyValueMemMode},
		{FileIO, KeyOnlyMemMode},
		{MMap, KeyValueMemMode},
		{MMap, KeyOnlyMemMode},
	}

	oneRun := func(t *testing.T, opts Options) {
		db, err := Open(opts)
		assert.Nil(t, err)
		defer destroyDB(db)

		hashKey := []byte("my_hash")
		vals, err := db.HVals(hashKey)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(vals))

		val16B := GetValue16B()
		err = db.HSet(hashKey, GetKey(1), val16B)
		assert.Nil(t, err)
		vals, err = db.HVals(hashKey)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(vals))
		assert.Equal(t, val16B, vals[0])

		val128B := GetValue128B()
		err = db.HSet(hashKey, GetKey(1), val128B)
		assert.Nil(t, err)
		vals, err = db.HVals(hashKey)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(vals))
		assert.Equal(t, val128B, vals[0])

		err = db.HSet(hashKey, GetKey(2), val16B)
		assert.Nil(t, err)
		vals, err = db.HVals(hashKey)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(vals))
		assert.Equal(t, [][]byte{val128B, val16B}, vals)

		val16B = GetValue16B()
		writeCount := 1000
		for i := 0; i < writeCount; i++ {
			err := db.HSet(hashKey, GetKey(i+100), val16B)
			assert.Nil(t, err)
		}
		vals, err = db.HVals(hashKey)
		assert.Nil(t, err)
		for i := 0; i < writeCount; i++ {
			assert.Equal(t, val16B, vals[i+2])
		}
	}

	for _, c := range cases {
		path := filepath.Join("/tmp", "rosedb")
		opts := DefaultOptions(path)
		opts.IoType = c.IOType
		opts.IndexMode = c.DataIndexMode
		oneRun(t, opts)
	}
}
