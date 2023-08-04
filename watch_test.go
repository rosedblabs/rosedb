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
	w := NewWatcher(capacity)
	for i := 0; i < size; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		e := NewEvent(ActionPut, key, value, 0)
		q = append(q, [2][]byte{key, value})
		w.Insert(e)
	}

	for i := 0; i < size; i++ {
		e, isEmpty := w.Scan()
		assert.Equal(t, false, isEmpty)
		key := q[i][0]
		assert.Equal(t, key, e.Key)
		value := q[i][1]
		assert.Equal(t, value, e.Value)
	}
}

func TestWatch_Rotate_Insert_Scan(t *testing.T) {
	capacity := 1000
	q := make([][2][]byte, capacity, capacity)
	w := NewWatcher(capacity)
	for i := 0; i < 2500; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		e := NewEvent(ActionPut, key, value, 0)
		w.Insert(e)
		sub := i % capacity
		q[sub] = [2][]byte{key, value}
	}

	sub := w.queue.Front
	for {
		e, isEmpty := w.Scan()
		if isEmpty {
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
	options.Watchable = true
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	w, err := db.Watch()
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		err = db.Put(key, value)
		assert.Nil(t, err)
		select {
		case event := <-w:
			assert.Equal(t, ActionPut, event.Action)
			assert.Equal(t, key, event.Key)
			assert.Equal(t, value, event.Value)
		}
	}
}