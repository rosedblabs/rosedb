package benchmark

import (
	"math/rand"
	"testing"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
)

var db *rosedb.DB

func init() {
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedbv2"

	var err error
	db, err = rosedb.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != rosedb.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
