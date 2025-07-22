package index

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rosedblabs/wal"
)

func TestMemoryBTree_Put_Get(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	key := []byte("testKey")
	chunkPosition, _ := w.Write([]byte("some data 1"))

	// Test Put
	oldPos := mt.Put(key, chunkPosition)
	if oldPos != nil {
		t.Fatalf("expected nil, got %+v", oldPos)
	}

	// Test Get
	gotPos := mt.Get(key)
	if chunkPosition.ChunkOffset != gotPos.ChunkOffset {
		t.Fatalf("expected %+v, got %+v", chunkPosition, gotPos)
	}
}

func TestMemoryBTree_Delete(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	key := []byte("testKey")
	chunkPosition, _ := w.Write([]byte("some data 2"))

	mt.Put(key, chunkPosition)

	// Test Delete
	delPos, ok := mt.Delete(key)
	if !ok {
		t.Fatal("expected item to be deleted")
	}
	if chunkPosition.ChunkOffset != delPos.ChunkOffset {
		t.Fatalf("expected %+v, got %+v", chunkPosition, delPos)
	}

	// Ensure the key is deleted
	if mt.Get(key) != nil {
		t.Fatal("expected nil, got value")
	}
}

func TestMemoryBTree_Size(t *testing.T) {
	mt := newBTree()

	if mt.Size() != 0 {
		t.Fatalf("expected size to be 0, got %d", mt.Size())
	}

	w, _ := wal.Open(wal.DefaultOptions)
	key := []byte("testKey")
	chunkPosition, _ := w.Write([]byte("some data 3"))

	mt.Put(key, chunkPosition)

	if mt.Size() != 1 {
		t.Fatalf("expected size to be 1, got %d", mt.Size())
	}
}

func TestMemoryBTree_Ascend_Descend(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	data := map[string][]byte{
		"apple":  []byte("some data 4"),
		"banana": []byte("some data 5"),
		"cherry": []byte("some data 6"),
	}

	positionMap := make(map[string]*wal.ChunkPosition)

	for k, v := range data {
		chunkPosition, _ := w.Write(v)
		positionMap[k] = chunkPosition
		mt.Put([]byte(k), chunkPosition)
	}

	// Test Ascend
	prevKey := ""

	// Define the Ascend handler function
	ascendHandler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if prevKey != "" && bytes.Compare([]byte(prevKey), key) >= 0 {
			return false, errors.New("items are not in ascending order")
		}
		expectedPos := positionMap[string(key)]
		if expectedPos.ChunkOffset != pos.ChunkOffset {
			return false, fmt.Errorf("expected position %+v, got %+v", expectedPos, pos)
		}
		prevKey = string(key)
		return true, nil
	}

	mt.Ascend(ascendHandler)

	// Define the Descend handler function
	descendHandler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if bytes.Compare([]byte(prevKey), key) <= 0 {
			return false, errors.New("items are not in descending order")
		}
		expectedPos := positionMap[string(key)]
		if expectedPos.ChunkOffset != pos.ChunkOffset {
			return false, fmt.Errorf("expected position %+v, got %+v", expectedPos, pos)
		}
		prevKey = string(key)
		return true, nil
	}

	// Test Descend
	prevKey = "zzzzzz"
	mt.Descend(descendHandler)
}

func TestMemoryBTree_AscendRange_DescendRange(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	data := map[string][]byte{
		"apple":  []byte("some data 1"),
		"banana": []byte("some data 2"),
		"cherry": []byte("some data 3"),
		"date":   []byte("some data 4"),
		"grape":  []byte("some data 5"),
	}

	positionMap := make(map[string]*wal.ChunkPosition)

	for k, v := range data {
		chunkPosition, _ := w.Write(v)
		positionMap[k] = chunkPosition
		mt.Put([]byte(k), chunkPosition)
	}

	// Test AscendRange
	fmt.Println("Testing AscendRange:")
	mt.AscendRange([]byte("banana"), []byte("grape"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		fmt.Printf("Key: %s, Position: %+v\n", key, pos)
		return true, nil
	})

	// Test DescendRange
	fmt.Println("Testing DescendRange:")
	mt.DescendRange([]byte("cherry"), []byte("date"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		fmt.Printf("Key: %s, Position: %+v\n", key, pos)
		return true, nil
	})
}

func TestMemoryBTree_AscendGreaterOrEqual_DescendLessOrEqual(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	data := map[string][]byte{
		"apple":  []byte("some data 1"),
		"banana": []byte("some data 2"),
		"cherry": []byte("some data 3"),
		"date":   []byte("some data 4"),
		"grape":  []byte("some data 5"),
	}

	positionMap := make(map[string]*wal.ChunkPosition)

	for k, v := range data {
		chunkPosition, _ := w.Write(v)
		positionMap[k] = chunkPosition
		mt.Put([]byte(k), chunkPosition)
	}

	// Test AscendGreaterOrEqual
	fmt.Println("Testing AscendGreaterOrEqual:")
	mt.AscendGreaterOrEqual([]byte("cherry"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		fmt.Printf("Key: %s, Position: %+v\n", key, pos)
		return true, nil
	})

	// Test DescendLessOrEqual
	fmt.Println("Testing DescendLessOrEqual:")
	mt.DescendLessOrEqual([]byte("date"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		fmt.Printf("Key: %s, Position: %+v\n", key, pos)
		return true, nil
	})
}

func TestMemoryBTree_Iterator(t *testing.T) {
	mt := newBTree()
	// Test iterator for empty tree
	it1 := mt.Iterator(false)
	assert.Equal(t, false, it1.Valid())

	// Build test data
	testData := map[string]*wal.ChunkPosition{
		"acee": {SegmentId: 1, BlockNumber: 2, ChunkOffset: 3, ChunkSize: 100},
		"bbcd": {SegmentId: 2, BlockNumber: 3, ChunkOffset: 4, ChunkSize: 200},
		"code": {SegmentId: 3, BlockNumber: 4, ChunkOffset: 5, ChunkSize: 300},
		"eede": {SegmentId: 4, BlockNumber: 5, ChunkOffset: 6, ChunkSize: 400},
	}

	// Insert test data
	for k, v := range testData {
		mt.Put([]byte(k), v)
	}

	// Test ascending iteration
	iter := mt.Iterator(false)
	var prevKey string
	count := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		currKey := string(iter.Key())
		pos := iter.Value()

		// Verify key order
		if prevKey != "" {
			assert.True(t, currKey > prevKey)
		}

		// Verify value correctness
		expectedPos := testData[currKey]
		assert.Equal(t, expectedPos, pos)

		prevKey = currKey
		count++
	}
	assert.Equal(t, len(testData), count)

	// Test descending iteration
	iter = mt.Iterator(true)
	prevKey = ""
	count = 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		currKey := string(iter.Key())
		pos := iter.Value()

		// Verify key order
		if prevKey != "" {
			assert.True(t, currKey < prevKey)
		}

		// Verify value correctness
		expectedPos := testData[currKey]
		assert.Equal(t, expectedPos, pos)

		prevKey = currKey
		count++
	}
	assert.Equal(t, len(testData), count)

	// Test Seek operation
	testCases := []struct {
		seekKey    string
		expectKey  string
		shouldFind bool
	}{
		{"b", "bbcd", true},   // Should find bbcd
		{"cc", "code", true},  // Should find code
		{"d", "eede", true},   // Should find eede
		{"f", "", false},      // Should not find any element
		{"aaa", "acee", true}, // Should find acee
	}

	for _, tc := range testCases {
		iter = mt.Iterator(false)
		iter.Seek([]byte(tc.seekKey))

		if tc.shouldFind {
			assert.True(t, iter.Valid())
			assert.Equal(t, tc.expectKey, string(iter.Key()))
			assert.Equal(t, testData[tc.expectKey], iter.Value())
		} else {
			assert.False(t, iter.Valid())
		}
	}
}
