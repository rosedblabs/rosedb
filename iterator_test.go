package rosedb

import (
	"bytes"
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
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(4*KB))
		assert.Nil(t, err)
	}
	iter3 := db.NewIterator(DefaultIteratorOptions)
	defer iter3.Close()
	var i = 0
	for ; iter3.Valid(); iter3.Next() {
		value, err := iter3.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		i++
	}
	assert.Equal(t, 10000, i)
}

func TestIterator_Validate(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	_ = db.Put([]byte("399023"), utils.RandomValue(100))
	_ = db.Put([]byte("011903"), utils.RandomValue(100))
	_ = db.Put([]byte("3321"), utils.RandomValue(100))
	_ = db.Put([]byte("09302"), utils.RandomValue(100))
	_ = db.Put([]byte("0099012"), utils.RandomValue(100))
	_ = db.Put([]byte("11232"), utils.RandomValue(100))
	_ = db.Put([]byte("55321"), utils.RandomValue(100))

	_ = db.Put([]byte("eacbd"), utils.RandomValue(100))
	_ = db.Put([]byte("bbdne"), utils.RandomValue(100))
	_ = db.Put([]byte("hunea"), utils.RandomValue(100))
	_ = db.Put([]byte("bbned"), utils.RandomValue(100))
	_ = db.Put([]byte("kkiem"), utils.RandomValue(100))
	_ = db.Put([]byte("qhuea"), utils.RandomValue(100))
	_ = db.Put([]byte("gfrss"), utils.RandomValue(100))

	t.Run("prefix", func(t *testing.T) {
		validate := func(reverse bool, prefix []byte, target int) {
			options := IteratorOptions{Prefix: prefix, Reverse: reverse}
			iter := db.NewIterator(options)
			defer iter.Close()

			var i = 0
			for ; iter.Valid(); iter.Next() {
				assert.True(t, bytes.HasPrefix(iter.Key(), prefix))
				value, err := iter.Value()
				assert.Nil(t, err)
				assert.NotNil(t, value)
				i++
			}
			assert.Equal(t, i, target)
		}

		validate(false, []byte("3"), 2)
		validate(true, []byte("3"), 2)
		validate(true, []byte("bb"), 2)
		validate(true, []byte("kk"), 1)
		validate(true, []byte("kkiem"), 1)
		validate(true, []byte("xxxxxxxx"), 0)
	})

	t.Run("rewind", func(t *testing.T) {
		validate := func(reverse bool, prefix []byte, target []byte) {
			options := IteratorOptions{Prefix: prefix, Reverse: reverse}
			iter := db.NewIterator(options)
			defer iter.Close()

			iter.Next()
			iter.Next()
			iter.Rewind()
			assert.Equal(t, target, iter.Key())
		}

		validate(false, []byte("xxxxxxxx"), nil)
		validate(false, []byte("bb"), []byte("bbdne"))
		validate(true, []byte("bb"), []byte("bbned"))
	})

	t.Run("seek", func(t *testing.T) {
		validate := func(reverse bool, prefix []byte, seek []byte, target []byte) {
			options := IteratorOptions{Prefix: prefix, Reverse: reverse}
			iter := db.NewIterator(options)
			defer iter.Close()

			iter.Seek(seek)
			assert.Equal(t, target, iter.Key())
		}

		validate(false, nil, []byte("a"), []byte("bbdne"))
		validate(false, nil, []byte("11"), []byte("11232"))
		validate(false, nil, []byte("zzzzz"), nil)
		validate(false, []byte("0"), []byte("06"), []byte("09302"))
		validate(true, []byte("0"), []byte("06"), []byte("011903"))
	})
}
