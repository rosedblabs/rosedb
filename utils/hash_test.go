package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemHash(t *testing.T) {
	// Test empty slice
	hash1 := MemHash([]byte{})
	assert.NotEqual(t, uint64(0), hash1)

	// Test non-empty slice
	hash2 := MemHash([]byte("hello"))
	assert.NotEqual(t, uint64(0), hash2)

	// Test same input produces same hash
	hash3 := MemHash([]byte("hello"))
	assert.Equal(t, hash2, hash3)

	// Test different input produces different hash
	hash4 := MemHash([]byte("world"))
	assert.NotEqual(t, hash2, hash4)

	// Test with various lengths
	for i := 1; i <= 100; i++ {
		data := make([]byte, i)
		for j := 0; j < i; j++ {
			data[j] = byte(j % 256)
		}
		hash := MemHash(data)
		assert.NotEqual(t, uint64(0), hash)
	}
}

func TestMemHashString(t *testing.T) {
	// Test empty string
	hash1 := MemHashString("")
	assert.NotEqual(t, uint64(0), hash1)

	// Test non-empty string
	hash2 := MemHashString("hello")
	assert.NotEqual(t, uint64(0), hash2)

	// Test same input produces same hash
	hash3 := MemHashString("hello")
	assert.Equal(t, hash2, hash3)

	// Test different input produces different hash
	hash4 := MemHashString("world")
	assert.NotEqual(t, hash2, hash4)

	// Test consistency between MemHash and MemHashString
	strHash := MemHashString("test")
	byteHash := MemHash([]byte("test"))
	assert.Equal(t, strHash, byteHash)
}

func TestMemHash_Collision(t *testing.T) {
	// Test that different keys produce different hashes (probabilistically)
	hashes := make(map[uint64]bool)
	collisions := 0

	for i := 0; i < 10000; i++ {
		key := GetTestKey(i)
		hash := MemHash(key)
		if hashes[hash] {
			collisions++
		}
		hashes[hash] = true
	}

	// Allow very few collisions (should be extremely rare with good hash function)
	assert.Less(t, collisions, 10, "Too many hash collisions")
}

func BenchmarkMemHash(b *testing.B) {
	data := []byte("benchmark-test-key-for-hash")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MemHash(data)
	}
}

func BenchmarkMemHashString(b *testing.B) {
	str := "benchmark-test-key-for-hash"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MemHashString(str)
	}
}
