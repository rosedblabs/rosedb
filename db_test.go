package rosedb

import (
	"math/rand"
	"sync"
	"testing"
	"time"

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

func TestDB_Ascend(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test Ascend function
	var result []string
	db.Ascend(func(k []byte, v []byte) (bool, error) {
		result = append(result, string(k))
		return true, nil // No error here
	})

	if err != nil {
		t.Errorf("Ascend returned an error: %v", err)
	}

	expected := []string{"key1", "key2", "key3"}
	if len(result) != len(expected) {
		t.Errorf("Unexpected number of results. Expected: %v, Got: %v", expected, result)
	} else {
		for i, val := range expected {
			if result[i] != val {
				t.Errorf("Unexpected result at index %d. Expected: %v, Got: %v", i, val, result[i])
			}
		}
	}
}

func TestDB_Descend(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test Descend function
	var result []string
	db.Descend(func(k []byte, v []byte) (bool, error) {
		result = append(result, string(k))
		return true, nil
	})

	if err != nil {
		t.Errorf("Descend returned an error: %v", err)
	}

	expected := []string{"key3", "key2", "key1"}
	if len(result) != len(expected) {
		t.Errorf("Unexpected number of results. Expected: %v, Got: %v", expected, result)
	} else {
		for i, val := range expected {
			if result[i] != val {
				t.Errorf("Unexpected result at index %d. Expected: %v, Got: %v", i, val, result[i])
			}
		}
	}
}

func TestDB_AscendRange(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test AscendRange
	var resultAscendRange []string
	db.AscendRange([]byte("banana"), []byte("grape"), func(k []byte, v []byte) (bool, error) {
		resultAscendRange = append(resultAscendRange, string(k))
		return true, nil
	})
	assert.Equal(t, []string{"banana", "cherry", "date"}, resultAscendRange)
}

func TestDB_DescendRange(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test DescendRange
	var resultDescendRange []string
	db.DescendRange([]byte("grape"), []byte("cherry"), func(k []byte, v []byte) (bool, error) {
		resultDescendRange = append(resultDescendRange, string(k))
		return true, nil
	})
	assert.Equal(t, []string{"grape", "date"}, resultDescendRange)
}

func TestDB_AscendGreaterOrEqual(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test AscendGreaterOrEqual
	var resultAscendGreaterOrEqual []string
	db.AscendGreaterOrEqual([]byte("date"), func(k []byte, v []byte) (bool, error) {
		resultAscendGreaterOrEqual = append(resultAscendGreaterOrEqual, string(k))
		return true, nil
	})
	assert.Equal(t, []string{"date", "grape", "kiwi"}, resultAscendGreaterOrEqual)
}

func TestDB_DescendLessOrEqual(t *testing.T) {
	// Create a test database instance
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test DescendLessOrEqual
	var resultDescendLessOrEqual []string
	db.DescendLessOrEqual([]byte("grape"), func(k []byte, v []byte) (bool, error) {
		resultDescendLessOrEqual = append(resultDescendLessOrEqual, string(k))
		return true, nil
	})
	assert.Equal(t, []string{"grape", "date", "cherry", "banana", "apple"}, resultDescendLessOrEqual)
}

func TestDB_PutWithTTL(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.PutWithTTL(utils.GetTestKey(1), utils.RandomValue(128), time.Millisecond*100)
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	time.Sleep(time.Millisecond * 200)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Nil(t, val2)

	err = db.PutWithTTL(utils.GetTestKey(2), utils.RandomValue(128), time.Millisecond*200)
	// rewrite
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(128))
	assert.Nil(t, err)
	time.Sleep(time.Millisecond * 200)
	val3, err := db.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(options)
	assert.Nil(t, err)

	val4, err := db2.Get(utils.GetTestKey(1))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Nil(t, val4)

	val5, err := db2.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	_ = db2.Close()
}

func TestDB_PutWithTTL_Merge(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)
	for i := 0; i < 100; i++ {
		err = db.PutWithTTL(utils.GetTestKey(i), utils.RandomValue(10), time.Second*2)
		assert.Nil(t, err)
	}
	for i := 100; i < 150; i++ {
		err = db.PutWithTTL(utils.GetTestKey(i), utils.RandomValue(10), time.Second*20)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second * 3)

	err = db.Merge(true)
	assert.Nil(t, err)

	for i := 0; i < 100; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, val)
		assert.Equal(t, err, ErrKeyNotFound)
	}
	for i := 100; i < 150; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
