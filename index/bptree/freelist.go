package bptree

import (
	"encoding/binary"
	"sync"
)

// FreeList manages free pages in the B+Tree file.
// It uses a simple linked list of page IDs.
type FreeList struct {
	pageIDs []uint32
	mu      sync.Mutex
}

// NewFreeList creates a new free list.
func NewFreeList() *FreeList {
	return &FreeList{
		pageIDs: make([]uint32, 0),
	}
}

// Allocate returns a free page ID, or 0 if none available.
func (f *FreeList) Allocate() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.pageIDs) == 0 {
		return 0
	}

	pageID := f.pageIDs[len(f.pageIDs)-1]
	f.pageIDs = f.pageIDs[:len(f.pageIDs)-1]
	return pageID
}

// Free adds a page ID to the free list.
func (f *FreeList) Free(pageID uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pageIDs = append(f.pageIDs, pageID)
}

// Count returns the number of free pages.
func (f *FreeList) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pageIDs)
}

// Serialize serializes the free list to bytes.
// Format: [count(4)][pageID1(4)][pageID2(4)]...
func (f *FreeList) Serialize(pageSize uint32) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf := make([]byte, pageSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(f.pageIDs)))

	offset := 4
	for _, pageID := range f.pageIDs {
		if offset+4 > int(pageSize) {
			break // page full, would need overflow handling for large free lists
		}
		binary.LittleEndian.PutUint32(buf[offset:offset+4], pageID)
		offset += 4
	}

	return buf
}

// Deserialize deserializes bytes to a free list.
func DeserializeFreeList(buf []byte) *FreeList {
	count := binary.LittleEndian.Uint32(buf[0:4])
	pageIDs := make([]uint32, count)

	offset := 4
	for i := uint32(0); i < count && offset+4 <= len(buf); i++ {
		pageIDs[i] = binary.LittleEndian.Uint32(buf[offset : offset+4])
		offset += 4
	}

	return &FreeList{
		pageIDs: pageIDs,
	}
}
