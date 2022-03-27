package benchmark

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"path/filepath"
	"testing"
)

var roseDB *rosedb.RoseDB

func init() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rosedb.DefaultOptions(path)
	var err error
	roseDB, err = rosedb.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("open rosedb err: %v", err))
	}
	initDataForGet()
}

func initDataForGet() {
	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := roseDB.Set(getKey(i), getValue128B())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRoseDB_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := roseDB.Set(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkRoseDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := roseDB.Get(getKey(i))
		assert.Nil(b, err)
	}
}

func BenchmarkRoseDB_LPush(b *testing.B) {
	keys := [][]byte{
		[]byte("my_list-1"),
		[]byte("my_list-2"),
		[]byte("my_list-3"),
		[]byte("my_list-4"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := roseDB.LPush(keys[k], getValue128B())
		assert.Nil(b, err)
	}
}
