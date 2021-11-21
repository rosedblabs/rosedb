package rosedb

import (
	"testing"
)

func BenchmarkRoseDB_Set(b *testing.B) {
	config := DefaultConfig()
	db := InitDB(config)
	defer DestroyDB(db)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRoseDB_Get(b *testing.B) {
	config := DefaultConfig()
	db := InitDB(config)
	defer DestroyDB(db)

	for i := 0; i < 500000; i++ {
		err := db.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var v interface{}
		err := db.Get(GetKey(i), &v)
		if err != nil {
			panic(err)
		}
	}
}
