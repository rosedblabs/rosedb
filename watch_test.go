package rosedb

import (
	"math/rand"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

func TestWatch_Insert_Scan(t *testing.T) {
	capacity := 1000
	q := make([][2][]byte, 0, capacity)
	w := NewWatcher(capacity)
	for i := 0; i < capacity; i++ {
		key := utils.GetTestKey(rand.Int())
		value := utils.RandomValue(128)
		e := NewEvent(ActionPut, key, value, 0)
		q = append(q, [2][]byte{key, value})
		w.Insert(e)
	}

	for i := 0; i < capacity; i++ {
		e, isEmpty := w.Scan()
		assert.Equal(t, false, isEmpty)
		key := q[i][0]
		assert.Equal(t, key, e.Key)
		value := q[i][1]
		assert.Equal(t, value, e.Value)
	}
}
