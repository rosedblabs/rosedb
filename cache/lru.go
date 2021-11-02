package cache

import (
	"container/list"
	"sync"
)

// LruCache stands for a least recently used cache.
type LruCache struct {
	capacity  int
	cacheMap  map[string]*list.Element
	cacheList *list.List
	mu        sync.Mutex
}

type lruItem struct {
	key   string
	value []byte
}

// NewLruCache create a new LRU cache.
func NewLruCache(capacity int) *LruCache {
	lru := &LruCache{}
	if capacity > 0 {
		lru.capacity = capacity
		lru.cacheMap = make(map[string]*list.Element)
		lru.cacheList = list.New()
	}
	return lru
}

// Get ...
func (c *LruCache) Get(key []byte) ([]byte, bool) {
	if c.capacity <= 0 || len(c.cacheMap) <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(string(key))
}

// Set ...
func (c *LruCache) Set(key, value []byte) {
	if c.capacity <= 0 || key == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(string(key), value)
}

// Remove ...
func (c *LruCache) Remove(key []byte) {
	if c.cacheMap == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.cacheMap[string(key)]; ok {
		delete(c.cacheMap, string(key))
		c.cacheList.Remove(ele)
	}
}

func (c *LruCache) get(key string) ([]byte, bool) {
	ele, ok := c.cacheMap[key]
	if ok {
		c.cacheList.MoveToFront(ele)
		item := ele.Value.(*lruItem)
		return item.value, true
	}
	return nil, false
}

func (c *LruCache) set(key string, value []byte) {
	ele, ok := c.cacheMap[key]
	if ok {
		item := c.cacheMap[key].Value.(*lruItem)
		item.value = value
		c.cacheList.MoveToFront(ele)
	} else {
		ele = c.cacheList.PushFront(&lruItem{key: key, value: value})
		c.cacheMap[key] = ele

		if c.cacheList.Len() > c.capacity {
			c.removeOldest()
		}
	}
}

func (c *LruCache) removeOldest() {
	ele := c.cacheList.Back()
	c.cacheList.Remove(ele)
	item := ele.Value.(*lruItem)
	delete(c.cacheMap, item.key)
}
