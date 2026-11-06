package rosedb

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_Merge_1_Empty(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	err = db.Merge(false)
	assert.NoError(t, err)
}

func TestDB_Merge_2_All_Invalid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	stat := db2.Stat()
	assert.Equal(t, 0, stat.KeysNum)
}

func TestDB_Merge_3_All_Valid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.NoError(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge_4_Twice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)
	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.NoError(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge_5_Mixed(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, 200000, stat.KeysNum)
}

func TestDB_Merge_6_Appending(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.NoError(t, err)
	}

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				m.Store(string(key), struct{}{})
				e := db.Put(key, utils.RandomValue(128))
				assert.NoError(t, e)
			}
		}()
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	wg.Wait()

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	var count int
	m.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Equal(t, 200000+count, stat.KeysNum)
}

func TestDB_Multi_Open_Merge(t *testing.T) {
	options := DefaultOptions
	kvs := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		db, err := Open(options)
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(rand.Int())
			value := utils.RandomValue(128)
			kvs[string(key)] = value
			err = db.Put(key, value)
			assert.NoError(t, err)
		}

		err = db.Merge(false)
		assert.NoError(t, err)
		err = db.Close()
		assert.NoError(t, err)
	}
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for key, value := range kvs {
		v, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, value, v)
	}
	assert.Equal(t, len(kvs), db.index.Size())
}

func TestDB_Merge_ReopenAfterDone(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	kvs := make(map[string][]byte)
	for i := 0; i < 200000; i++ {
		key := utils.GetTestKey(i)
		value := utils.RandomValue(128)
		kvs[string(key)] = value
		err := db.Put(key, value)
		assert.NoError(t, err)
	}

	err = db.Merge(true)
	assert.NoError(t, err)
	_, err = os.Stat(mergeDirPath(options.DirPath))
	assert.True(t, os.IsNotExist(err))

	for key, value := range kvs {
		v, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, value, v)
	}
	assert.Equal(t, len(kvs), db.index.Size())
}

func TestDB_Merge_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(11)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				value := utils.RandomValue(128)
				m.Store(string(key), value)
				e := db.Put(key, value)
				assert.NoError(t, e)
			}
		}()
	}
	go func() {
		defer wg.Done()
		err = db.Merge(true)
		assert.NoError(t, err)
	}()
	wg.Wait()

	_, err = os.Stat(mergeDirPath(options.DirPath))
	assert.True(t, os.IsNotExist(err))

	var count int
	m.Range(func(key, value any) bool {
		v, err := db.Get([]byte(key.(string)))
		assert.NoError(t, err)
		assert.Equal(t, value, v)
		count++
		return true
	})
	assert.Equal(t, count, db.index.Size())
}

// TestDB_Merge_InterruptedInstall reopens the database after loadMergeFiles was
// interrupted, as by a crash, part way through installing the merged files.
// Opening the database runs loadMergeFiles again; it must not remove merged
// segments that the interrupted run already put in place.
func TestDB_Merge_InterruptedInstall(t *testing.T) {
	// installSegments performs the first n steps of the segment loop of
	// loadMergeFiles, the way it moves the files.
	installSegments := func(t *testing.T, dirPath string, n uint32) {
		t.Helper()
		mergeDir := mergeDirPath(dirPath)
		for fileId := uint32(1); fileId <= n; fileId++ {
			src := wal.SegmentFileName(mergeDir, dataFileNameSuffix, fileId)
			dest := wal.SegmentFileName(dirPath, dataFileNameSuffix, fileId)
			if stat, err := os.Stat(src); err == nil && stat.Size() > 0 {
				require.NoError(t, os.Rename(src, dest))
				continue
			}
			_ = os.Remove(dest)
			_ = os.Remove(src)
		}
	}

	tests := []struct {
		name string
		// steps is the number of segment ids installed before the interruption.
		steps func(mergeFinSegmentId uint32) uint32
	}{
		{"first segment installed", func(uint32) uint32 { return 1 }},
		{"all segments installed", func(id uint32) uint32 { return id }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := DefaultOptions
			options.DirPath = filepath.Join(t.TempDir(), "rosedb")
			options.SegmentSize = 64 * KB
			db, err := Open(options)
			require.NoError(t, err)
			// db is reassigned below, so close whichever instance is open at
			// the end. Windows cannot remove the temp dir while a file is open.
			defer func() {
				if db != nil {
					destroyDB(db)
				}
			}()

			kvs := make(map[string][]byte)
			for i := 0; i < 2000; i++ {
				key, value := utils.GetTestKey(i), utils.RandomValue(128)
				require.NoError(t, db.Put(key, value))
				kvs[string(key)] = value
			}
			for i := 0; i < 1000; i++ {
				key := utils.GetTestKey(i)
				require.NoError(t, db.Delete(key))
				delete(kvs, string(key))
			}
			require.NoError(t, db.Merge(false))
			require.NoError(t, db.Close())
			db = nil

			mergeFinSegmentId, err := getMergeFinSegmentId(mergeDirPath(options.DirPath))
			require.NoError(t, err)
			// the test needs more than one merged segment.
			_, err = os.Stat(wal.SegmentFileName(mergeDirPath(options.DirPath), dataFileNameSuffix, 2))
			require.NoError(t, err)
			installSegments(t, options.DirPath, tt.steps(mergeFinSegmentId))

			db, err = Open(options)
			require.NoError(t, err)
			for key, value := range kvs {
				v, err := db.Get([]byte(key))
				require.NoError(t, err, "key %s", key)
				require.Equal(t, value, v)
			}
			for i := 0; i < 1000; i++ {
				_, err := db.Get(utils.GetTestKey(i))
				require.ErrorIs(t, err, ErrKeyNotFound)
			}
			require.Equal(t, len(kvs), db.index.Size())
		})
	}
}
