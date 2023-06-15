package rosedb

import (
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIterator_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// empty database
	iter1 := db.NewIterator(DefaultIteratorOptions)
	assert.False(t, iter1.Valid())
	iter2 := db.NewIterator(IteratorOptions{Reverse: true, Prefix: []byte("aa")})
	assert.False(t, iter2.Valid())

	// with data
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(4*KB))
		assert.Nil(t, err)
	}
	iter3 := db.NewIterator(DefaultIteratorOptions)
	t.Log(string(iter3.Key()))
	value, _ := iter3.Value()
	t.Log(string(value))
}
