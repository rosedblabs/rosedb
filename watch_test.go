package rosedb

import (
	"math/rand"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

func TestWatch_Insert_Scan(t *testing.T) {
	capacity := 1000
	// There are two spaces to determine whether the queue is full and overwrite the write.
	size := capacity - 2
	q := make([][2][]byte, 0, size)
	w := newWatcher(uint64(capacity), true)
	for i := 0; i < size; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		q = append(q, [2][]byte{key, value})
		w.putEvent(&Event{
			Action:  WatchActionPut,
			Key:     key,
			Value:   value,
			BatchId: 0,
		})
	}

	for i := 0; i < size; i++ {
		e := w.getEvent()
		assert.NotEmpty(t, e)
		key := q[i][0]
		assert.Equal(t, key, e.Key)
		value := q[i][1]
		assert.Equal(t, value, e.Value)
	}
}

func TestWatch_Rotate_Insert_Scan(t *testing.T) {
	capacity := 1000
	q := make([][2][]byte, capacity)
	w := newWatcher(uint64(capacity), true)
	for i := 0; i < 2500; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		w.putEvent(&Event{
			Action:  WatchActionPut,
			Key:     key,
			Value:   value,
			BatchId: 0,
		})
		sub := i % capacity
		q[sub] = [2][]byte{key, value}
	}

	sub := int(w.queue.Front)
	for {
		e := w.getEvent()
		if e == nil {
			break
		}
		key := q[sub][0]
		assert.Equal(t, key, e.Key)
		value := q[sub][1]
		assert.Equal(t, value, e.Value)
		sub = (sub + 1) % capacity
	}

}

func TestWatch_Put_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 10
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	w, err := db.Watcher()
	// w, err := db.Watch()
	assert.Nil(t, err)
	for i := 0; i < 50; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		err = db.Put(key, value)
		assert.Nil(t, err)
		event := <-w.Watch()
		assert.Equal(t, WatchActionPut, event.Action)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, value, event.Value)
	}
}

func TestWatch_Put_Delete_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 10
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	w, err := db.Watcher()
	assert.Nil(t, err)

	key := utils.GetTestKey(rand.Int())
	value := utils.RandomValue(128)
	err = db.Put(key, value)
	assert.Nil(t, err)
	err = db.Delete(key)
	assert.Nil(t, err)

	for i := 0; i < 2; i++ {
		event := <-w.Watch()
		assert.Equal(t, key, event.Key)
		if event.Action == WatchActionPut {
			assert.Equal(t, value, event.Value)
		} else if event.Action == WatchActionDelete {
			assert.Equal(t, 0, len(event.Value))
		}
	}
}

func TestWatch_Batch_Put_Watch(t *testing.T) {
	options := DefaultOptions
	options.WatchQueueSize = 1000
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	w, err := db.Watcher()
	assert.Nil(t, err)

	times := 100
	batch := db.NewBatch(DefaultBatchOptions)
	for i := 0; i < times; i++ {
		err = batch.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	err = batch.Commit()
	assert.Nil(t, err)

	var batchId uint64
	for i := 0; i < times; i++ {
		event := <-w.Watch()
		if i == 0 {
			batchId = event.BatchId
		}
		assert.Equal(t, batchId, event.BatchId)
	}
}
