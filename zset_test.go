package rosedb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoseDB_ZAdd(t *testing.T) {
	opts := DefaultOptions("/tmp/rosedb")
	db, err := Open(opts)
	assert.Nil(t, err)

	zsetKey := []byte("my_zset")
	//writeCount := 10
	//for i := 0; i < writeCount; i++ {
	//	err := db.ZAdd(zsetKey, float64(i+100), GetKey(i))
	//	assert.Nil(t, err)
	//}

	//db.ZRem(zsetKey, GetKey(3))

	values, err := db.ZRange(zsetKey, 0, -1)
	assert.Nil(t, err)
	for _, v := range values {
		t.Log(string(v))
	}
}
