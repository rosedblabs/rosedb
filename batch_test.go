package rosedb

import (
	"os"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

func destroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
	_ = os.RemoveAll(mergeDirPath(db.options.DirPath))
}

func TestBatch_Put_Normal(t *testing.T) {
	// value 128B
	batchPutAndIterate(t, 1*GB, 10000, 128)
	// value 1KB
	batchPutAndIterate(t, 1*GB, 10000, KB)
	// value 32KB
	batchPutAndIterate(t, 1*GB, 1000, 32*KB)
}

func TestBatch_Put_IncrSegmentFile(t *testing.T) {
	batchPutAndIterate(t, 64*MB, 2000, 32*KB)
	options := DefaultOptions
	options.SegmentSize = 64 * MB
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	generateData(t, db, 1, 2000, 32*KB)

	// write more data to rotate new segment file
	batch := db.NewBatch(DefaultBatchOptions)
	for i := 0; i < 1000; i++ {
		err := batch.Put(utils.GetTestKey(i*100), utils.RandomValue(32*KB))
		assert.Nil(t, err)
	}
	err = batch.Commit()
	assert.Nil(t, err)
}

func TestBatch_Get_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	batch1 := db.NewBatch(DefaultBatchOptions)
	err = batch1.Put(utils.GetTestKey(12), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := batch1.Get(utils.GetTestKey(12))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	_ = batch1.Commit()

	generateData(t, db, 400, 500, 4*KB)

	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Delete(utils.GetTestKey(450))
	assert.Nil(t, err)
	val, err := batch2.Get(utils.GetTestKey(450))
	assert.Nil(t, val)
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(12), true)
	assertKeyExistOrNot(t, db2, utils.GetTestKey(450), false)
}

func TestBatch_Delete_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.Delete([]byte("not exist"))
	assert.Nil(t, err)

	generateData(t, db, 1, 100, 128)
	err = db.Delete(utils.GetTestKey(99))
	assert.Nil(t, err)

	exist, err := db.Exist(utils.GetTestKey(99))
	assert.Nil(t, err)
	assert.False(t, exist)

	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Put(utils.GetTestKey(200), utils.RandomValue(100))
	assert.Nil(t, err)
	err = batch.Delete(utils.GetTestKey(200))
	assert.Nil(t, err)
	exist1, err := batch.Exist(utils.GetTestKey(200))
	assert.Nil(t, err)
	assert.False(t, exist1)
	_ = batch.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(200), false)
}

func TestBatch_Exist_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	generateData(t, db, 1, 100, 128)
	batch := db.NewBatch(DefaultBatchOptions)
	ok1, err := batch.Exist(utils.GetTestKey(99))
	assert.Nil(t, err)
	assert.True(t, ok1)
	ok2, err := batch.Exist(utils.GetTestKey(5000))
	assert.Nil(t, err)
	assert.False(t, ok2)
	_ = batch.Commit()

	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.GetTestKey(99), true)
}

func generateData(t *testing.T, db *DB, start, end, valueLen int) {
	for ; start < end; start++ {
		err := db.Put(utils.GetTestKey(start), utils.RandomValue(valueLen))
		assert.Nil(t, err)
	}
}

func batchPutAndIterate(t *testing.T, segmentSize int64, size, valueLen int) {
	options := DefaultOptions
	options.SegmentSize = segmentSize
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(BatchOptions{})

	for i := 0; i < size; i++ {
		err := batch.Put(utils.GetTestKey(i), utils.RandomValue(valueLen))
		assert.Nil(t, err)
	}
	err = batch.Commit()
	assert.Nil(t, err)

	for i := 0; i < size; i++ {
		value, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(utils.RandomValue(valueLen)), len(value))
	}

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	for i := 0; i < size; i++ {
		value, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(utils.RandomValue(valueLen)), len(value))
	}
}

func assertKeyExistOrNot(t *testing.T, db *DB, key []byte, exist bool) {
	val, err := db.Get(key)
	if exist {
		assert.Nil(t, err)
		assert.NotNil(t, val)
	} else {
		assert.Nil(t, val)
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

func TestBatch_Rollback(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("rosedb")
	value := []byte("val")

	batcher := db.NewBatch(DefaultBatchOptions)
	err = batcher.Put(key, value)
	assert.Nil(t, err)

	err = batcher.Rollback()
	assert.Nil(t, err)

	resp, err := db.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Empty(t, resp)
}

func TestBatch_SetTwice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(DefaultBatchOptions)
	key := []byte("rosedb")
	value1 := []byte("val1")
	value2 := []byte("val2")
	_ = batch.Put(key, value1)
	_ = batch.Put(key, value2)

	res, err := batch.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, res, value2)

	_ = batch.Commit()
	res2, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, res2, value2)
}
