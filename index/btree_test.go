package index

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rosedblabs/wal"
)

func TestMemoryBTree_Put_Get(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	key := []byte("testKey")
	position, _ := w.Write([]byte("some data 1"))

	// Test Put
	wrapper := &ChunkPositionWrapper{pos: position, deleted: false}
	oldPos := mt.Put(key, wrapper)
	if oldPos != nil {
		t.Fatalf("expected nil, got %+v", oldPos)
	}

	// Test Get
	gotPos := mt.Get(key)
	if position.ChunkOffset != gotPos.pos.ChunkOffset {
		t.Fatalf("expected %+v, got %+v", position, gotPos.pos.ChunkOffset)
	}
}

func TestMemoryBTree_Delete(t *testing.T) {
	mt := newBTree()
	w, _ := wal.Open(wal.DefaultOptions)

	key := []byte("testKey")
	position, _ := w.Write([]byte("some data 2"))

	wrapper := &ChunkPositionWrapper{pos: position, deleted: false}

	mt.Put(key, wrapper)

	// Test Delete
	delPos, ok := mt.Delete(key)
	if !ok {
		t.Fatal("expected item to be deleted")
	}
	if position.ChunkOffset != delPos.pos.ChunkOffset {
		t.Fatalf("expected %+v, got %+v", position, delPos.pos)
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
	position, _ := w.Write([]byte("some data 3"))

	wrapper := &ChunkPositionWrapper{pos: position, deleted: false}

	mt.Put(key, wrapper)

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
		position, _ := w.Write(v)
		positionMap[k] = position

		wrapper := &ChunkPositionWrapper{pos: position, deleted: false}
		mt.Put([]byte(k), wrapper)
	}

	// Test Ascend
	prevKey := ""

	// Define the Ascend handler function
	ascendHandler := func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if prevKey != "" && bytes.Compare([]byte(prevKey), key) >= 0 {
			return false, fmt.Errorf("items are not in ascending order")
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
			return false, fmt.Errorf("items are not in descending order")
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
		position, _ := w.Write(v)
		positionMap[k] = position

		wrapper := &ChunkPositionWrapper{pos: position, deleted: false}
		mt.Put([]byte(k), wrapper)
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
		position, _ := w.Write(v)
		positionMap[k] = position

		wrapper := &ChunkPositionWrapper{pos: position, deleted: false}
		mt.Put([]byte(k), wrapper)
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
