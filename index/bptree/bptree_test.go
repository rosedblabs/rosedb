package bptree

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Basic Operations Tests
// ============================================================================

func TestBPlusTree_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Test Put and Get
	pos1 := &wal.ChunkPosition{SegmentId: 1, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 100}
	oldPos := tree.Put([]byte("key1"), pos1)
	assert.Nil(t, oldPos)

	got := tree.Get([]byte("key1"))
	assert.NotNil(t, got)
	assert.Equal(t, pos1.SegmentId, got.SegmentId)
	assert.Equal(t, pos1.BlockNumber, got.BlockNumber)
	assert.Equal(t, pos1.ChunkOffset, got.ChunkOffset)
	assert.Equal(t, pos1.ChunkSize, got.ChunkSize)

	// Test update
	pos2 := &wal.ChunkPosition{SegmentId: 2, BlockNumber: 1, ChunkOffset: 100, ChunkSize: 200}
	oldPos = tree.Put([]byte("key1"), pos2)
	assert.NotNil(t, oldPos)
	assert.Equal(t, pos1.SegmentId, oldPos.SegmentId)

	got = tree.Get([]byte("key1"))
	assert.Equal(t, pos2.SegmentId, got.SegmentId)

	// Test Get non-existent key
	got = tree.Get([]byte("nonexistent"))
	assert.Nil(t, got)

	// Test Size
	assert.Equal(t, 1, tree.Size())
}

func TestBPlusTree_Delete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	pos := &wal.ChunkPosition{SegmentId: 1, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 100}
	tree.Put([]byte("key1"), pos)

	// Delete existing key
	oldPos, deleted := tree.Delete([]byte("key1"))
	assert.True(t, deleted)
	assert.NotNil(t, oldPos)
	assert.Equal(t, pos.SegmentId, oldPos.SegmentId)

	// Verify deleted
	got := tree.Get([]byte("key1"))
	assert.Nil(t, got)
	assert.Equal(t, 0, tree.Size())

	// Delete non-existent key
	oldPos, deleted = tree.Delete([]byte("nonexistent"))
	assert.False(t, deleted)
	assert.Nil(t, oldPos)
}

func TestBPlusTree_MultipleKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Insert multiple keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	assert.Equal(t, len(keys), tree.Size())

	// Verify all keys
	for i, key := range keys {
		got := tree.Get([]byte(key))
		assert.NotNil(t, got, "key %s should exist", key)
		assert.Equal(t, uint32(i), got.SegmentId)
	}
}

func TestBPlusTree_UpdateMultiple(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Insert and update multiple times
	key := []byte("key")
	for i := 0; i < 100; i++ {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i), ChunkSize: 100}
		tree.Put(key, pos)
	}

	// Size should be 1 (same key updated)
	assert.Equal(t, 1, tree.Size())

	// Value should be the last one
	got := tree.Get(key)
	assert.Equal(t, uint32(99), got.SegmentId)
}

// ============================================================================
// Empty Tree Tests
// ============================================================================

func TestBPlusTree_EmptyTree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Test operations on empty tree
	assert.Equal(t, 0, tree.Size())
	assert.Nil(t, tree.Get([]byte("key")))

	// Delete on empty tree
	_, deleted := tree.Delete([]byte("key"))
	assert.False(t, deleted)

	// Iterator on empty tree
	it := tree.Iterator(false)
	it.Rewind()
	assert.False(t, it.Valid())
	assert.Nil(t, it.Key())
	assert.Nil(t, it.Value())
	it.Close()
}

// ============================================================================
// Iterator Tests
// ============================================================================

func TestBPlusTree_Iterator_Forward(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Insert keys in random order
	keys := []string{"cherry", "apple", "elderberry", "banana", "date"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	// Iterate in ascending order
	it := tree.Iterator(false)
	defer it.Close()

	expected := []string{"apple", "banana", "cherry", "date", "elderberry"}
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		assert.Equal(t, expected[i], string(it.Key()))
		assert.NotNil(t, it.Value())
		i++
	}
	assert.Equal(t, len(expected), i)
}

func TestBPlusTree_Iterator_Reverse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	// Iterate in descending order
	it := tree.Iterator(true)
	defer it.Close()

	expected := []string{"elderberry", "date", "cherry", "banana", "apple"}
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		assert.Equal(t, expected[i], string(it.Key()))
		i++
	}
	assert.Equal(t, len(expected), i)
}

func TestBPlusTree_Iterator_Seek(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	// Seek to existing key
	it := tree.Iterator(false)
	it.Seek([]byte("cherry"))
	assert.True(t, it.Valid())
	assert.Equal(t, "cherry", string(it.Key()))
	it.Close()

	// Seek to non-existent key (should find next)
	it = tree.Iterator(false)
	it.Seek([]byte("coconut"))
	assert.True(t, it.Valid())
	assert.Equal(t, "date", string(it.Key()))
	it.Close()

	// Seek beyond all keys
	it = tree.Iterator(false)
	it.Seek([]byte("zebra"))
	assert.False(t, it.Valid())
	it.Close()

	// Seek before all keys
	it = tree.Iterator(false)
	it.Seek([]byte("aaa"))
	assert.True(t, it.Valid())
	assert.Equal(t, "apple", string(it.Key()))
	it.Close()
}

func TestBPlusTree_Iterator_SeekReverse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	// Seek in reverse mode
	it := tree.Iterator(true)
	it.Seek([]byte("cherry"))
	assert.True(t, it.Valid())
	assert.Equal(t, "cherry", string(it.Key()))

	// Next should go to banana
	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, "banana", string(it.Key()))
	it.Close()
}

func TestBPlusTree_Iterator_Rewind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	keys := []string{"apple", "banana", "cherry"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	it := tree.Iterator(false)

	// Iterate partially
	it.Rewind()
	it.Next()
	assert.Equal(t, "banana", string(it.Key()))

	// Rewind and iterate again
	it.Rewind()
	assert.Equal(t, "apple", string(it.Key()))

	it.Close()
}

func TestBPlusTree_Iterator_Close(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	tree.Put([]byte("key1"), &wal.ChunkPosition{SegmentId: 1})

	it := tree.Iterator(false)
	it.Rewind()
	assert.True(t, it.Valid())

	it.Close()
	assert.False(t, it.Valid())
	assert.Nil(t, it.Key())
	assert.Nil(t, it.Value())
}

// ============================================================================
// Persistence Tests
// ============================================================================

func TestBPlusTree_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Create and populate tree
	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)

	keys := []string{"apple", "banana", "cherry"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: uint32(i), ChunkOffset: int64(i * 100), ChunkSize: uint32(100 + i)}
		tree.Put([]byte(key), pos)
	}

	err = tree.Close()
	require.NoError(t, err)

	// Reopen and verify
	tree2, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree2.Close()

	assert.Equal(t, len(keys), tree2.Size())

	for i, key := range keys {
		got := tree2.Get([]byte(key))
		require.NotNil(t, got, "key %s should exist", key)
		assert.Equal(t, uint32(i), got.SegmentId)
		assert.Equal(t, uint32(i), got.BlockNumber)
		assert.Equal(t, int64(i*100), got.ChunkOffset)
		assert.Equal(t, uint32(100+i), got.ChunkSize)
	}
}

func TestBPlusTree_PersistenceAfterDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Create tree and add keys
	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i)}
		tree.Put([]byte(key), pos)
	}

	// Delete some keys
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		tree.Delete([]byte(key))
	}

	tree.Close()

	// Reopen and verify
	tree2, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree2.Close()

	assert.Equal(t, 5, tree2.Size())

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		assert.Nil(t, tree2.Get([]byte(key)))
	}

	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		assert.NotNil(t, tree2.Get([]byte(key)))
	}
}

func TestBPlusTree_Sync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)

	tree.Put([]byte("key1"), &wal.ChunkPosition{SegmentId: 1})

	err = tree.Sync()
	assert.NoError(t, err)

	tree.Close()

	// Verify file exists and has content
	info, err := os.Stat(path)
	assert.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

// ============================================================================
// Split Tests
// ============================================================================

func TestBPlusTree_Split(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Use small order to trigger splits
	options := DefaultOptions
	options.Order = 4

	tree, err := Open(path, options)
	require.NoError(t, err)
	defer tree.Close()

	// Insert enough keys to trigger multiple splits
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	assert.Equal(t, 100, tree.Size())

	// Verify all keys are retrievable
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		got := tree.Get([]byte(key))
		assert.NotNil(t, got, "key %s not found", key)
		assert.Equal(t, uint32(i), got.SegmentId)
	}

	// Verify iteration order
	it := tree.Iterator(false)
	defer it.Close()

	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		expected := fmt.Sprintf("key%03d", i)
		assert.Equal(t, expected, string(it.Key()))
		i++
	}
	assert.Equal(t, 100, i)
}

func TestBPlusTree_SplitWithRandomKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	options := DefaultOptions
	options.Order = 4

	tree, err := Open(path, options)
	require.NoError(t, err)
	defer tree.Close()

	// Insert random keys
	rand.Seed(time.Now().UnixNano())
	keys := make(map[string]*wal.ChunkPosition)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%08d", rand.Int())
		pos := &wal.ChunkPosition{SegmentId: uint32(i), ChunkOffset: int64(i)}
		tree.Put([]byte(key), pos)
		keys[key] = pos
	}

	// Verify all keys
	for key, expectedPos := range keys {
		got := tree.Get([]byte(key))
		assert.NotNil(t, got, "key %s not found", key)
		assert.Equal(t, expectedPos.SegmentId, got.SegmentId)
	}
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

func TestBPlusTree_EmptyKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Empty key should work
	pos := &wal.ChunkPosition{SegmentId: 1}
	tree.Put([]byte(""), pos)

	got := tree.Get([]byte(""))
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), got.SegmentId)
}

func TestBPlusTree_LargeKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Large key (512 bytes)
	largeKey := make([]byte, 512)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	pos := &wal.ChunkPosition{SegmentId: 1}
	tree.Put(largeKey, pos)

	got := tree.Get(largeKey)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), got.SegmentId)
}

func TestBPlusTree_BinaryKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Binary key with null bytes
	key := []byte{0x00, 0x01, 0x02, 0x00, 0x03}
	pos := &wal.ChunkPosition{SegmentId: 1}
	tree.Put(key, pos)

	got := tree.Get(key)
	assert.NotNil(t, got)
}

func TestBPlusTree_NilValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Nil value should not crash
	tree.Put([]byte("key"), nil)

	got := tree.Get([]byte("key"))
	// Behavior depends on implementation
	_ = got
}

// ============================================================================
// Concurrent Tests
// ============================================================================

func TestBPlusTree_ConcurrentPut(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	keysPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-key%d", gid, i)
				pos := &wal.ChunkPosition{SegmentId: uint32(gid*1000 + i)}
				tree.Put([]byte(key), pos)
			}
		}(g)
	}

	wg.Wait()

	assert.Equal(t, numGoroutines*keysPerGoroutine, tree.Size())
}

func TestBPlusTree_ConcurrentGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	// Insert keys first
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i)}
		tree.Put([]byte(key), pos)
	}

	// Concurrent reads
	var wg sync.WaitGroup
	numGoroutines := 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key%d", i)
				got := tree.Get([]byte(key))
				assert.NotNil(t, got)
			}
		}()
	}

	wg.Wait()
}

func TestBPlusTree_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree.Close()

	var wg sync.WaitGroup

	// Writers
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("writer%d-key%d", gid, i)
				pos := &wal.ChunkPosition{SegmentId: uint32(gid*1000 + i)}
				tree.Put([]byte(key), pos)
			}
		}(g)
	}

	// Readers
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("writer%d-key%d", gid, rand.Intn(100))
				tree.Get([]byte(key))
			}
		}(g)
	}

	wg.Wait()
}

// ============================================================================
// File Operations Tests
// ============================================================================

func TestBPlusTree_FileCreation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Verify file doesn't exist
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err))

	tree, err := Open(path, DefaultOptions)
	require.NoError(t, err)

	// Verify file was created
	_, err = os.Stat(path)
	assert.NoError(t, err)

	tree.Close()
}

func TestBPlusTree_OpenExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Create and populate
	tree1, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	tree1.Put([]byte("key1"), &wal.ChunkPosition{SegmentId: 1})
	tree1.Close()

	// Open existing
	tree2, err := Open(path, DefaultOptions)
	require.NoError(t, err)
	defer tree2.Close()

	got := tree2.Get([]byte("key1"))
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), got.SegmentId)
}

func TestBPlusTree_CorruptedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	// Create a corrupted file
	err := os.WriteFile(path, []byte("corrupted data"), 0644)
	require.NoError(t, err)

	// Should fail to open
	_, err = Open(path, DefaultOptions)
	assert.Error(t, err)
}

// ============================================================================
// Indexer Interface Tests
// ============================================================================

func TestBPTreeIndexer(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	// Test Put and Get
	pos1 := &wal.ChunkPosition{SegmentId: 1, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 100}
	indexer.Put([]byte("key1"), pos1)

	got := indexer.Get([]byte("key1"))
	assert.NotNil(t, got)
	assert.Equal(t, pos1.SegmentId, got.SegmentId)

	// Test Size
	assert.Equal(t, 1, indexer.Size())

	// Test Delete
	oldPos, deleted := indexer.Delete([]byte("key1"))
	assert.True(t, deleted)
	assert.NotNil(t, oldPos)

	got = indexer.Get([]byte("key1"))
	assert.Nil(t, got)
}

func TestBPTreeIndexer_Ascend(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	keys := []string{"cherry", "apple", "banana", "date"}
	for i, key := range keys {
		pos := &wal.ChunkPosition{SegmentId: uint32(i)}
		indexer.Put([]byte(key), pos)
	}

	var result []string
	indexer.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return true, nil
	})

	assert.Equal(t, []string{"apple", "banana", "cherry", "date"}, result)
}

func TestBPTreeIndexer_AscendEarlyStop(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		indexer.Put([]byte(key), &wal.ChunkPosition{SegmentId: uint32(i)})
	}

	var result []string
	indexer.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return len(result) < 3, nil // stop after 3
	})

	assert.Equal(t, 3, len(result))
}

func TestBPTreeIndexer_Descend(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	keys := []string{"apple", "banana", "cherry"}
	for i, key := range keys {
		indexer.Put([]byte(key), &wal.ChunkPosition{SegmentId: uint32(i)})
	}

	var result []string
	indexer.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return true, nil
	})

	assert.Equal(t, []string{"cherry", "banana", "apple"}, result)
}

func TestBPTreeIndexer_AscendRange(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		indexer.Put([]byte(key), &wal.ChunkPosition{SegmentId: uint32(i)})
	}

	var result []string
	indexer.AscendRange([]byte("banana"), []byte("elderberry"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return true, nil
	})

	assert.Equal(t, []string{"banana", "cherry", "date"}, result)
}

func TestBPTreeIndexer_AscendGreaterOrEqual(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		indexer.Put([]byte(key), &wal.ChunkPosition{SegmentId: uint32(i)})
	}

	var result []string
	indexer.AscendGreaterOrEqual([]byte("cherry"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return true, nil
	})

	assert.Equal(t, []string{"cherry", "date", "elderberry"}, result)
}

func TestBPTreeIndexer_DescendRange(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		indexer.Put([]byte(key), &wal.ChunkPosition{SegmentId: uint32(i)})
	}

	var result []string
	indexer.DescendRange([]byte("elderberry"), []byte("banana"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return true, nil
	})

	// Should include elderberry, date, cherry (not banana as it's the end)
	assert.True(t, len(result) >= 2)
	assert.Equal(t, "elderberry", result[0])
}

func TestBPTreeIndexer_DescendLessOrEqual(t *testing.T) {
	dir := t.TempDir()

	indexer, err := NewBPTreeIndexer(dir, DefaultOptions)
	require.NoError(t, err)
	defer indexer.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		indexer.Put([]byte(key), &wal.ChunkPosition{SegmentId: uint32(i)})
	}

	var result []string
	indexer.DescendLessOrEqual([]byte("cherry"), func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		result = append(result, string(key))
		return true, nil
	})

	assert.Equal(t, []string{"cherry", "banana", "apple"}, result)
}

// ============================================================================
// Page Serialization Tests
// ============================================================================

func TestMetaPage_Serialization(t *testing.T) {
	meta := &MetaPage{
		Magic:        MagicNumber,
		Version:      Version,
		PageSize:     4096,
		RootPageID:   1,
		FreeListPage: 2,
		KeyCount:     100,
		PageCount:    50,
	}

	data := meta.Serialize()
	assert.Equal(t, metaPageSize, len(data))

	restored, err := DeserializeMetaPage(data)
	require.NoError(t, err)

	assert.Equal(t, meta.Magic, restored.Magic)
	assert.Equal(t, meta.Version, restored.Version)
	assert.Equal(t, meta.PageSize, restored.PageSize)
	assert.Equal(t, meta.RootPageID, restored.RootPageID)
	assert.Equal(t, meta.FreeListPage, restored.FreeListPage)
	assert.Equal(t, meta.KeyCount, restored.KeyCount)
	assert.Equal(t, meta.PageCount, restored.PageCount)
}

func TestNode_LeafSerialization(t *testing.T) {
	node := &Node{
		PageID:   1,
		PageType: PageTypeLeaf,
		KeyCount: 2,
		Parent:   0,
		Next:     2,
		Prev:     0,
		Keys:     [][]byte{[]byte("key1"), []byte("key2")},
		Values: []*wal.ChunkPosition{
			{SegmentId: 1, BlockNumber: 0, ChunkOffset: 0, ChunkSize: 100},
			{SegmentId: 2, BlockNumber: 1, ChunkOffset: 100, ChunkSize: 200},
		},
	}

	data := node.Serialize(4096)
	assert.Equal(t, 4096, len(data))

	restored, err := DeserializeNode(1, data)
	require.NoError(t, err)

	assert.Equal(t, node.PageID, restored.PageID)
	assert.Equal(t, node.PageType, restored.PageType)
	assert.Equal(t, node.KeyCount, restored.KeyCount)
	assert.Equal(t, node.Next, restored.Next)
	assert.True(t, bytes.Equal(node.Keys[0], restored.Keys[0]))
	assert.True(t, bytes.Equal(node.Keys[1], restored.Keys[1]))
	assert.Equal(t, node.Values[0].SegmentId, restored.Values[0].SegmentId)
	assert.Equal(t, node.Values[1].ChunkSize, restored.Values[1].ChunkSize)
}

func TestNode_InternalSerialization(t *testing.T) {
	node := &Node{
		PageID:   1,
		PageType: PageTypeInternal,
		KeyCount: 2,
		Parent:   0,
		Keys:     [][]byte{[]byte("key1"), []byte("key2")},
		Children: []uint32{2, 3, 4},
	}

	data := node.Serialize(4096)
	restored, err := DeserializeNode(1, data)
	require.NoError(t, err)

	assert.Equal(t, node.PageType, restored.PageType)
	assert.Equal(t, node.KeyCount, restored.KeyCount)
	assert.True(t, bytes.Equal(node.Keys[0], restored.Keys[0]))
	assert.Equal(t, node.Children[0], restored.Children[0])
	assert.Equal(t, node.Children[2], restored.Children[2])
}

// ============================================================================
// Cache Tests
// ============================================================================

func TestPageCache_Basic(t *testing.T) {
	cache := NewPageCache(10, 4096, nil)

	node := &Node{PageID: 1, PageType: PageTypeLeaf}
	cache.Put(node)

	got := cache.Get(1)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), got.PageID)

	// Non-existent page
	got = cache.Get(999)
	assert.Nil(t, got)
}

func TestPageCache_LRU(t *testing.T) {
	cache := NewPageCache(3, 4096, nil)

	// Add 3 pages
	for i := uint32(1); i <= 3; i++ {
		cache.Put(&Node{PageID: i})
	}

	assert.Equal(t, 3, cache.Len())

	// Add 4th page, should evict page 1
	cache.Put(&Node{PageID: 4})
	assert.Equal(t, 3, cache.Len())

	// Page 1 should be evicted
	assert.Nil(t, cache.Get(1))

	// Pages 2, 3, 4 should exist
	assert.NotNil(t, cache.Get(2))
	assert.NotNil(t, cache.Get(3))
	assert.NotNil(t, cache.Get(4))
}

func TestPageCache_Dirty(t *testing.T) {
	cache := NewPageCache(10, 4096, nil)

	node := &Node{PageID: 1}
	cache.Put(node)

	assert.False(t, cache.IsDirty(1))

	cache.MarkDirty(1)
	assert.True(t, cache.IsDirty(1))

	cache.ClearDirty(1)
	assert.False(t, cache.IsDirty(1))
}

func TestPageCache_GetDirtyPages(t *testing.T) {
	cache := NewPageCache(10, 4096, nil)

	for i := uint32(1); i <= 5; i++ {
		cache.Put(&Node{PageID: i})
	}

	cache.MarkDirty(1)
	cache.MarkDirty(3)
	cache.MarkDirty(5)

	dirtyPages := cache.GetDirtyPages()
	assert.Equal(t, 3, len(dirtyPages))
}

func TestPageCache_Remove(t *testing.T) {
	cache := NewPageCache(10, 4096, nil)

	cache.Put(&Node{PageID: 1})
	cache.MarkDirty(1)

	cache.Remove(1)

	assert.Nil(t, cache.Get(1))
	assert.False(t, cache.IsDirty(1))
}

// ============================================================================
// FreeList Tests
// ============================================================================

func TestFreeList_Basic(t *testing.T) {
	fl := NewFreeList()

	assert.Equal(t, 0, fl.Count())
	assert.Equal(t, uint32(0), fl.Allocate())

	fl.Free(1)
	fl.Free(2)
	fl.Free(3)

	assert.Equal(t, 3, fl.Count())

	// Should return in LIFO order
	assert.Equal(t, uint32(3), fl.Allocate())
	assert.Equal(t, uint32(2), fl.Allocate())
	assert.Equal(t, uint32(1), fl.Allocate())
	assert.Equal(t, uint32(0), fl.Allocate())
}

func TestFreeList_Serialization(t *testing.T) {
	fl := NewFreeList()
	fl.Free(10)
	fl.Free(20)
	fl.Free(30)

	data := fl.Serialize(4096)
	restored := DeserializeFreeList(data)

	assert.Equal(t, fl.Count(), restored.Count())
	assert.Equal(t, fl.Allocate(), restored.Allocate())
	assert.Equal(t, fl.Allocate(), restored.Allocate())
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkBPlusTree_Put(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.index")

	tree, err := Open(path, DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}
}

func BenchmarkBPlusTree_Get(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.index")

	tree, err := Open(path, DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	// Populate tree
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key%010d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i), BlockNumber: 0, ChunkOffset: int64(i * 100), ChunkSize: 100}
		tree.Put([]byte(key), pos)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i%100000)
		tree.Get([]byte(key))
	}
}

func BenchmarkBPlusTree_Delete(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.index")

	tree, err := Open(path, DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	// Populate tree
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i)}
		tree.Put([]byte(key), pos)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		tree.Delete([]byte(key))
	}
}

func BenchmarkBPlusTree_Iterator(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.index")

	tree, err := Open(path, DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	// Populate tree
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%010d", i)
		pos := &wal.ChunkPosition{SegmentId: uint32(i)}
		tree.Put([]byte(key), pos)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := tree.Iterator(false)
		for it.Rewind(); it.Valid(); it.Next() {
			_ = it.Key()
			_ = it.Value()
		}
		it.Close()
	}
}

func BenchmarkBPlusTree_ConcurrentPut(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.index")

	tree, err := Open(path, DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%010d", rand.Int())
			pos := &wal.ChunkPosition{SegmentId: uint32(i)}
			tree.Put([]byte(key), pos)
			i++
		}
	})
}
