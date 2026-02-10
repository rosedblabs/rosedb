package bptree

import (
	"bytes"
	"os"
	"sync"

	"github.com/rosedblabs/wal"
)

// BPlusTree represents a disk-based B+Tree index.
type BPlusTree struct {
	file     *os.File
	meta     *MetaPage
	cache    *PageCache
	freelist *FreeList
	order    int
	mu       sync.RWMutex
	fileMu   sync.Mutex // protects file read/write operations
	options  Options
}

// Open opens or creates a B+Tree index file.
func Open(path string, options Options) (*BPlusTree, error) {
	if options.PageSize == 0 {
		options.PageSize = DefaultOptions.PageSize
	}
	if options.CacheSize == 0 {
		options.CacheSize = DefaultOptions.CacheSize
	}
	if options.Order == 0 {
		options.Order = calculateOrder(options.PageSize)
	}

	// Open or create file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	tree := &BPlusTree{
		file:     file,
		freelist: NewFreeList(),
		order:    options.Order,
		options:  options,
	}

	// Create cache with flush callback
	tree.cache = NewPageCache(options.CacheSize, options.PageSize, func(pageID uint32, data []byte) error {
		return tree.writePage(pageID, data)
	})

	// Check if file is empty (new index)
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if stat.Size() == 0 {
		// Initialize new B+Tree
		if err := tree.initialize(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		// Load existing B+Tree
		if err := tree.load(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return tree, nil
}

// initialize creates a new B+Tree with empty root.
func (t *BPlusTree) initialize() error {
	// Create meta page
	t.meta = &MetaPage{
		Magic:        MagicNumber,
		Version:      Version,
		PageSize:     t.options.PageSize,
		RootPageID:   1, // Root will be page 1
		FreeListPage: 0, // No free list page initially
		KeyCount:     0,
		PageCount:    2, // Meta + Root
	}

	// Create empty root leaf node
	root := &Node{
		PageID:   1,
		PageType: PageTypeLeaf,
		KeyCount: 0,
		Parent:   InvalidPageID,
		Next:     InvalidPageID,
		Prev:     InvalidPageID,
		Keys:     make([][]byte, 0),
		Values:   make([]*wal.ChunkPosition, 0),
		dirty:    true,
	}

	// Write meta page
	if err := t.writePage(0, t.meta.Serialize()); err != nil {
		return err
	}

	// Write root page
	if err := t.writePage(1, root.Serialize(t.options.PageSize)); err != nil {
		return err
	}

	t.cache.Put(root)
	return nil
}

// load loads an existing B+Tree from disk.
func (t *BPlusTree) load() error {
	// Read meta page
	metaBuf := make([]byte, t.options.PageSize)
	if _, err := t.file.ReadAt(metaBuf, 0); err != nil {
		return err
	}

	meta, err := DeserializeMetaPage(metaBuf)
	if err != nil {
		return err
	}
	t.meta = meta
	t.options.PageSize = meta.PageSize

	// Load free list if exists
	if meta.FreeListPage != InvalidPageID {
		buf := make([]byte, t.options.PageSize)
		if _, err := t.file.ReadAt(buf, int64(meta.FreeListPage)*int64(t.options.PageSize)); err != nil {
			return err
		}
		t.freelist = DeserializeFreeList(buf)
	}

	return nil
}

// Close closes the B+Tree and flushes all dirty pages.
func (t *BPlusTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Flush all dirty pages
	if err := t.flush(); err != nil {
		return err
	}

	return t.file.Close()
}

// flush writes all dirty pages to disk.
func (t *BPlusTree) flush() error {
	// Flush dirty pages
	dirtyPages := t.cache.GetDirtyPages()
	for _, pageID := range dirtyPages {
		node := t.cache.Get(pageID)
		if node != nil {
			if err := t.writePage(pageID, node.Serialize(t.options.PageSize)); err != nil {
				return err
			}
			t.cache.ClearDirty(pageID)
		}
	}

	// Write meta page
	if err := t.writePage(0, t.meta.Serialize()); err != nil {
		return err
	}

	// Write free list if not empty
	if t.freelist.Count() > 0 {
		if t.meta.FreeListPage == InvalidPageID {
			t.meta.FreeListPage = t.allocatePage()
		}
		if err := t.writePage(t.meta.FreeListPage, t.freelist.Serialize(t.options.PageSize)); err != nil {
			return err
		}
	}

	if t.options.SyncWrites {
		return t.file.Sync()
	}
	return nil
}

// Sync flushes all dirty pages to disk.
func (t *BPlusTree) Sync() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flush()
}

// readPage reads a page from disk.
func (t *BPlusTree) readPage(pageID uint32) ([]byte, error) {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()

	buf := make([]byte, t.options.PageSize)
	_, err := t.file.ReadAt(buf, int64(pageID)*int64(t.options.PageSize))
	return buf, err
}

// writePage writes a page to disk.
func (t *BPlusTree) writePage(pageID uint32, data []byte) error {
	t.fileMu.Lock()
	defer t.fileMu.Unlock()

	// Ensure data is page-sized
	if len(data) < int(t.options.PageSize) {
		padded := make([]byte, t.options.PageSize)
		copy(padded, data)
		data = padded
	}
	_, err := t.file.WriteAt(data, int64(pageID)*int64(t.options.PageSize))
	return err
}

// getNode retrieves a node, first checking cache, then disk.
func (t *BPlusTree) getNode(pageID uint32) (*Node, error) {
	// Check cache first
	if node := t.cache.Get(pageID); node != nil {
		return node, nil
	}

	// Read from disk
	buf, err := t.readPage(pageID)
	if err != nil {
		return nil, err
	}

	node, err := DeserializeNode(pageID, buf)
	if err != nil {
		return nil, err
	}

	t.cache.Put(node)
	return node, nil
}

// allocatePage allocates a new page ID.
func (t *BPlusTree) allocatePage() uint32 {
	// Try free list first
	if pageID := t.freelist.Allocate(); pageID != InvalidPageID {
		return pageID
	}

	// Allocate new page
	pageID := t.meta.PageCount
	t.meta.PageCount++
	return pageID
}

// Put inserts or updates a key-value pair.
func (t *BPlusTree) Put(key []byte, pos *wal.ChunkPosition) *wal.ChunkPosition {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Find leaf node
	leaf, err := t.findLeaf(key)
	if err != nil {
		return nil
	}

	// Try to find existing key
	idx, found := t.searchInNode(leaf, key)

	var oldPos *wal.ChunkPosition
	if found {
		// Update existing key
		oldPos = leaf.Values[idx]
		leaf.Values[idx] = pos
	} else {
		// Insert new key
		t.insertIntoLeaf(leaf, idx, key, pos)
		t.meta.KeyCount++
	}

	t.cache.MarkDirty(leaf.PageID)

	// Check if node needs splitting
	if int(leaf.KeyCount) >= t.order {
		t.splitLeaf(leaf)
	}

	return oldPos
}

// Get retrieves the value for a key.
func (t *BPlusTree) Get(key []byte) *wal.ChunkPosition {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leaf, err := t.findLeaf(key)
	if err != nil {
		return nil
	}

	idx, found := t.searchInNode(leaf, key)
	if !found {
		return nil
	}

	return leaf.Values[idx]
}

// Delete removes a key from the tree.
func (t *BPlusTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	leaf, err := t.findLeaf(key)
	if err != nil {
		return nil, false
	}

	idx, found := t.searchInNode(leaf, key)
	if !found {
		return nil, false
	}

	oldPos := leaf.Values[idx]

	// Remove key-value from leaf
	leaf.Keys = append(leaf.Keys[:idx], leaf.Keys[idx+1:]...)
	leaf.Values = append(leaf.Values[:idx], leaf.Values[idx+1:]...)
	leaf.KeyCount--
	t.meta.KeyCount--

	t.cache.MarkDirty(leaf.PageID)

	// Note: For simplicity, we don't implement node merging here
	// A full implementation would handle underflow by merging or redistributing

	return oldPos, true
}

// Size returns the number of keys in the tree.
func (t *BPlusTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int(t.meta.KeyCount)
}

// findLeaf finds the leaf node that should contain the given key.
func (t *BPlusTree) findLeaf(key []byte) (*Node, error) {
	node, err := t.getNode(t.meta.RootPageID)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf() {
		idx := t.findChildIndex(node, key)
		node, err = t.getNode(node.Children[idx])
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// findChildIndex finds the index of the child that should contain the key.
func (t *BPlusTree) findChildIndex(node *Node, key []byte) int {
	// Binary search for the first key > key
	lo, hi := 0, int(node.KeyCount)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(node.Keys[mid], key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// searchInNode searches for a key in a node.
// Returns the index and whether the key was found.
func (t *BPlusTree) searchInNode(node *Node, key []byte) (int, bool) {
	lo, hi := 0, int(node.KeyCount)
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(node.Keys[mid], key)
		if cmp == 0 {
			return mid, true
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

// insertIntoLeaf inserts a key-value pair into a leaf node at the given index.
func (t *BPlusTree) insertIntoLeaf(leaf *Node, idx int, key []byte, pos *wal.ChunkPosition) {
	// Make room for new entry
	leaf.Keys = append(leaf.Keys, nil)
	copy(leaf.Keys[idx+1:], leaf.Keys[idx:])
	leaf.Keys[idx] = key

	leaf.Values = append(leaf.Values, nil)
	copy(leaf.Values[idx+1:], leaf.Values[idx:])
	leaf.Values[idx] = pos

	leaf.KeyCount++
}

// splitLeaf splits a full leaf node.
func (t *BPlusTree) splitLeaf(leaf *Node) {
	mid := int(leaf.KeyCount) / 2

	// Create new leaf
	newLeaf := &Node{
		PageID:   t.allocatePage(),
		PageType: PageTypeLeaf,
		KeyCount: uint16(int(leaf.KeyCount) - mid),
		Parent:   leaf.Parent,
		Next:     leaf.Next,
		Prev:     leaf.PageID,
		Keys:     make([][]byte, int(leaf.KeyCount)-mid),
		Values:   make([]*wal.ChunkPosition, int(leaf.KeyCount)-mid),
		dirty:    true,
	}

	// Copy second half to new leaf
	copy(newLeaf.Keys, leaf.Keys[mid:])
	copy(newLeaf.Values, leaf.Values[mid:])

	// Update old leaf
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.KeyCount = uint16(mid)
	leaf.Next = newLeaf.PageID

	// Update next leaf's prev pointer
	if newLeaf.Next != InvalidPageID {
		nextLeaf, err := t.getNode(newLeaf.Next)
		if err == nil {
			nextLeaf.Prev = newLeaf.PageID
			t.cache.MarkDirty(nextLeaf.PageID)
		}
	}

	t.cache.Put(newLeaf)
	t.cache.MarkDirty(leaf.PageID)
	t.cache.MarkDirty(newLeaf.PageID)

	// Insert into parent
	t.insertIntoParent(leaf, newLeaf.Keys[0], newLeaf)
}

// insertIntoParent inserts a key and new child into the parent node.
func (t *BPlusTree) insertIntoParent(left *Node, key []byte, right *Node) {
	if left.Parent == InvalidPageID {
		// Create new root
		newRoot := &Node{
			PageID:   t.allocatePage(),
			PageType: PageTypeInternal,
			KeyCount: 1,
			Parent:   InvalidPageID,
			Keys:     [][]byte{key},
			Children: []uint32{left.PageID, right.PageID},
			dirty:    true,
		}

		left.Parent = newRoot.PageID
		right.Parent = newRoot.PageID
		t.meta.RootPageID = newRoot.PageID

		t.cache.Put(newRoot)
		t.cache.MarkDirty(newRoot.PageID)
		t.cache.MarkDirty(left.PageID)
		t.cache.MarkDirty(right.PageID)
		return
	}

	parent, err := t.getNode(left.Parent)
	if err != nil {
		return
	}

	// Find insertion point
	idx := t.findChildIndex(parent, key)

	// Insert key and child
	parent.Keys = append(parent.Keys, nil)
	copy(parent.Keys[idx+1:], parent.Keys[idx:])
	parent.Keys[idx] = key

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[idx+2:], parent.Children[idx+1:])
	parent.Children[idx+1] = right.PageID

	parent.KeyCount++
	right.Parent = parent.PageID

	t.cache.MarkDirty(parent.PageID)
	t.cache.MarkDirty(right.PageID)

	// Check if parent needs splitting
	if int(parent.KeyCount) >= t.order {
		t.splitInternal(parent)
	}
}

// splitInternal splits a full internal node.
func (t *BPlusTree) splitInternal(node *Node) {
	mid := int(node.KeyCount) / 2

	// Create new internal node
	newNode := &Node{
		PageID:   t.allocatePage(),
		PageType: PageTypeInternal,
		KeyCount: uint16(int(node.KeyCount) - mid - 1),
		Parent:   node.Parent,
		Keys:     make([][]byte, int(node.KeyCount)-mid-1),
		Children: make([]uint32, int(node.KeyCount)-mid),
		dirty:    true,
	}

	// Key at mid will be promoted to parent
	promoteKey := node.Keys[mid]

	// Copy second half to new node (excluding the promoted key)
	copy(newNode.Keys, node.Keys[mid+1:])
	copy(newNode.Children, node.Children[mid+1:])

	// Update children's parent pointer
	for _, childID := range newNode.Children {
		if child, err := t.getNode(childID); err == nil {
			child.Parent = newNode.PageID
			t.cache.MarkDirty(child.PageID)
		}
	}

	// Update old node
	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]
	node.KeyCount = uint16(mid)

	t.cache.Put(newNode)
	t.cache.MarkDirty(node.PageID)
	t.cache.MarkDirty(newNode.PageID)

	// Insert into parent
	t.insertIntoParent(node, promoteKey, newNode)
}
