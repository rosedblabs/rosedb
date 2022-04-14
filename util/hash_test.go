package util

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

func TestMemHash(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"1", args{buf: []byte("aa")},
		},
		{
			"2", args{buf: []byte("11")},
		},
		{
			"3", args{buf: []byte("0")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MemHash(tt.args.buf)
			assert.NotZero(t, got)
		})
	}
}

func TestMemHash2(t *testing.T) {
	m := make(map[uint64]struct{})
	for i := 0; i < 1000000; i++ {
		v := MemHash([]byte("lotusdb"))
		m[v] = struct{}{}
	}
	// all hash values should be the same in one process.
	assert.Equal(t, 1, len(m))
}

func TestMemHash3(t *testing.T) {
	// different groutines
	key := []byte("lotusdb")
	wg := sync.WaitGroup{}
	wg.Add(2)

	var (
		hash1 uint64
		hash2 uint64
	)
	go func() {
		hash1 = MemHash(key)
		wg.Done()
	}()

	go func() {
		hash2 = MemHash(key)
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, hash1, hash2)
}

func TestMemHash_SpecialCase(t *testing.T) {
	buf := make([]byte, 0)
	u := rthash(buf, 0)
	assert.Equal(t, u, uint64(0))
}

func BenchmarkMemHash(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		MemHash([]byte(strconv.Itoa(i * 1000)))
	}
}
