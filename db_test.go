package rosedb

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

func TestDB_Put_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100; i++ {
		err := db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(128))
		assert.Nil(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(KB))
		assert.Nil(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(5*KB))
		assert.Nil(t, err)
	}

	// reopen
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, 300, stat.KeysNum)
}

func TestDB_Get_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// not exist
	val1, err := db.Get([]byte("not-exist"))
	assert.Nil(t, val1)
	assert.Equal(t, ErrKeyNotFound, err)

	generateData(t, db, 1, 100, 128)
	for i := 1; i < 100; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(val), len(utils.RandomValue(128)))
	}
	generateData(t, db, 200, 300, KB)
	for i := 200; i < 300; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(val), len(utils.RandomValue(KB)))
	}
	generateData(t, db, 400, 500, 4*KB)
	for i := 400; i < 500; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(val), len(utils.RandomValue(4*KB)))
	}
}

func TestDB_Close_Sync(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

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
				assert.Nil(t, e)
			}
		}()
	}
	wg.Wait()

	var count int
	m.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Equal(t, count, db.index.Size())
}
