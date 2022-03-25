package benchmark

import (
	"fmt"
	"github.com/flower-corp/rosedb"
	"github.com/stretchr/testify/assert"
	"testing"
)

var roseDB *rosedb.RoseDB

func init() {
	opts := rosedb.DefaultOptions("/tmp/rosdb")
	var err error
	roseDB, err = rosedb.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("open rosedb err: %v", err))
	}
}

func BenchmarkRoseDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := roseDB.Set(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}
