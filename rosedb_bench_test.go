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

	// benchmark env and result:

	//goos: darwin
	//goarch: amd64
	//pkg: github.com/roseduan/rosedb
	//cpu: Intel(R) Core(TM) i5-1038NG7 CPU @ 2.00GHz
	//BenchmarkRoseDB_Set
	//BenchmarkRoseDB_Set-8   	  169148	      6898 ns/op	     716 B/op	      16 allocs/op
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

	//	goos: darwin
	//goarch: amd64
	//pkg: github.com/roseduan/rosedb
	//cpu: Intel(R) Core(TM) i5-1038NG7 CPU @ 2.00GHz
	//BenchmarkRoseDB_Get
	//BenchmarkRoseDB_Get-8   	  232638	      5630 ns/op	     384 B/op	      10 allocs/op
}

func BenchmarkRoseDB_HSet(b *testing.B) {
	config := DefaultConfig()
	db := InitDB(config)
	defer DestroyDB(db)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.HSet("my_hash", GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}

	//goos: darwin
	//goarch: amd64
	//pkg: github.com/roseduan/rosedb
	//cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
	//BenchmarkTxn_HSet
	//BenchmarkTxn_HSet-12    	  181048	      5992 ns/op	     765 B/op	      18 allocs/op
}

func BenchmarkRoseDB_SAdd(b *testing.B) {
	config := DefaultConfig()
	db := InitDB(config)
	defer DestroyDB(db)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.SAdd("my_set", GetValue())
		if err != nil {
			panic(err)
		}
	}
}
