package bptree

import (
	"container/list"
	"sync"
)

// FlushFunc is a function that writes a page to disk.
type FlushFunc func(pageID uint32, data []byte) error

// PageCache is an LRU cache for B+Tree pages.
type PageCache struct {
	capacity int
	cache    map[uint32]*list.Element
	lru      *list.List
	dirty    map[uint32]bool // track dirty pages
	mu       sync.RWMutex
	flushFn  FlushFunc // callback to flush dirty pages
	pageSize uint32
}

type cacheEntry struct {
	pageID uint32
	node   *Node
}

// NewPageCache creates a new page cache with the given capacity.
func NewPageCache(capacity int, pageSize uint32, flushFn FlushFunc) *PageCache {
	return &PageCache{
		capacity: capacity,
		cache:    make(map[uint32]*list.Element),
		lru:      list.New(),
		dirty:    make(map[uint32]bool),
		flushFn:  flushFn,
		pageSize: pageSize,
	}
}

// Get retrieves a page from the cache.
func (c *PageCache) Get(pageID uint32) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[pageID]; ok {
		c.lru.MoveToFront(elem)
		return elem.Value.(*cacheEntry).node
	}
	return nil
}

// Put adds a page to the cache.
func (c *PageCache) Put(node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[node.PageID]; ok {
		c.lru.MoveToFront(elem)
		elem.Value.(*cacheEntry).node = node
		return
	}

	// Evict if at capacity
	if c.lru.Len() >= c.capacity {
		c.evictLocked()
	}

	entry := &cacheEntry{pageID: node.PageID, node: node}
	elem := c.lru.PushFront(entry)
	c.cache[node.PageID] = elem
}

// MarkDirty marks a page as dirty.
func (c *PageCache) MarkDirty(pageID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dirty[pageID] = true
}

// IsDirty checks if a page is dirty.
func (c *PageCache) IsDirty(pageID uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dirty[pageID]
}

// ClearDirty clears the dirty flag for a page.
func (c *PageCache) ClearDirty(pageID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.dirty, pageID)
}

// GetDirtyPages returns all dirty page IDs.
func (c *PageCache) GetDirtyPages() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pages := make([]uint32, 0, len(c.dirty))
	for pageID := range c.dirty {
		pages = append(pages, pageID)
	}
	return pages
}

// Remove removes a page from the cache.
func (c *PageCache) Remove(pageID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[pageID]; ok {
		c.lru.Remove(elem)
		delete(c.cache, pageID)
		delete(c.dirty, pageID)
	}
}

// evictLocked evicts the least recently used page.
// Must be called with lock held.
// If the page is dirty, it will be flushed to disk before eviction.
func (c *PageCache) evictLocked() *Node {
	elem := c.lru.Back()
	if elem == nil {
		return nil
	}

	entry := elem.Value.(*cacheEntry)

	// Flush dirty page before eviction
	if c.dirty[entry.pageID] && c.flushFn != nil {
		data := entry.node.Serialize(c.pageSize)
		_ = c.flushFn(entry.pageID, data)
		delete(c.dirty, entry.pageID)
	}

	c.lru.Remove(elem)
	delete(c.cache, entry.pageID)

	return entry.node
}

// EvictDirty returns a dirty page to be flushed if the cache is full.
// Returns nil if no eviction is needed.
func (c *PageCache) EvictDirty() *Node {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru.Len() < c.capacity {
		return nil
	}

	// Find the LRU dirty page
	for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*cacheEntry)
		if c.dirty[entry.pageID] {
			return entry.node
		}
	}

	return nil
}

// Clear clears the cache.
func (c *PageCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[uint32]*list.Element)
	c.lru = list.New()
	c.dirty = make(map[uint32]bool)
}

// Len returns the number of pages in the cache.
func (c *PageCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}
