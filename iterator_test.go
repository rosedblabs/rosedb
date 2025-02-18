package rosedb

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIterator_Basic(t *testing.T) {
	options := DefaultOptions
	options.DirPath = filepath.Join(options.DirPath, "iterator_basic")
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// Test empty database
	iteratorOptions := DefaultIteratorOptions
	iter := db.NewIterator(iteratorOptions)
	assert.False(t, iter.Valid())
	iter.Close()

	// Put some key-value pairs
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		assert.Nil(t, err)
	}

	// Test forward iteration
	iter = db.NewIterator(iteratorOptions)
	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		assert.NotNil(t, key)
		assert.True(t, bytes.Equal(key, keys[i]))
		val := iter.Value()
		assert.NotNil(t, val)
		assert.True(t, bytes.Equal(val, values[i]))
		i++
	}
	assert.Equal(t, len(keys), i)
	iter.Close()

	// Test reverse iteration
	iteratorOptions.Reverse = true
	iter = db.NewIterator(iteratorOptions)

	i = len(keys) - 1
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		assert.NotNil(t, key)
		assert.True(t, bytes.Equal(key, keys[i]))
		val := iter.Value()
		assert.Nil(t, err)
		assert.True(t, bytes.Equal(val, values[i]))
		i--
	}
	assert.Equal(t, -1, i)
	iter.Close()
}

func TestIterator_Seek(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// Put some key-value pairs
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		assert.Nil(t, err)
	}

	iteratorOptions := DefaultIteratorOptions
	// Test seek in ascending order
	iter := db.NewIterator(iteratorOptions)

	iter.Seek([]byte("key3"))
	assert.True(t, iter.Valid())
	key := iter.Key()
	assert.NotNil(t, key)
	assert.True(t, bytes.Equal(key, []byte("key3")))
	val := iter.Value()
	assert.NotNil(t, val)
	assert.True(t, bytes.Equal(val, []byte("value3")))
	iter.Close()

	// Test seek in descending order
	iteratorOptions.Reverse = true
	iter = db.NewIterator(iteratorOptions)

	iter.Seek([]byte("key3"))
	assert.True(t, iter.Valid())
	key = iter.Key()
	assert.NotNil(t, key)
	assert.True(t, bytes.Equal(key, []byte("key3")))
	val = iter.Value()
	assert.NotNil(t, val)
	assert.True(t, bytes.Equal(val, []byte("value3")))
	iter.Close()
}

func TestIterator_Prefix(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// Put some key-value pairs with different prefixes
	keys := [][]byte{
		[]byte("a:1"),
		[]byte("a:2"),
		[]byte("b:1"),
		[]byte("b:2"),
		[]byte("c:1"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
		[]byte("value5"),
	}

	for i := 0; i < len(keys); i++ {
		err := db.Put(keys[i], values[i])
		assert.Nil(t, err)
	}

	iteratorOptions := DefaultIteratorOptions
	// Test prefix iteration
	iteratorOptions.Prefix = []byte("b:")
	iter := db.NewIterator(iteratorOptions)

	expectedKeys := [][]byte{[]byte("b:1"), []byte("b:2")}
	expectedValues := [][]byte{[]byte("value3"), []byte("value4")}
	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		assert.NotNil(t, key)
		assert.True(t, bytes.Equal(key, expectedKeys[i]))
		val := iter.Value()
		assert.NotNil(t, val)
		assert.True(t, bytes.Equal(val, expectedValues[i]))
		i++
	}
	assert.Equal(t, len(expectedKeys), i)
	iter.Close()
}

func TestIterator_Expired(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// Put a key-value pair with TTL
	err = db.PutWithTTL([]byte("key1"), []byte("value1"), time.Millisecond*10)
	assert.Nil(t, err)
	// Put a normal key-value pair
	err = db.Put([]byte("key2"), []byte("value2"))
	assert.Nil(t, err)

	// Wait for the first key to expire
	time.Sleep(time.Millisecond * 20)

	// Test iteration
	iter := db.NewIterator(DefaultIteratorOptions)

	iter.Rewind()
	assert.True(t, iter.Valid())
	key := iter.Key()
	assert.NotNil(t, key)
	assert.True(t, bytes.Equal(key, []byte("key2")))
	val := iter.Value()
	assert.NotNil(t, val)
	assert.True(t, bytes.Equal(val, []byte("value2")))
	iter.Next()
	assert.False(t, iter.Valid())
	iter.Close()
}
