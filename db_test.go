package rosedb

import (
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
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
