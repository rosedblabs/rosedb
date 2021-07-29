package cache

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var KeyNotFoundError = errors.New("key not found")

type LRUCache struct {
	size      int
	mu        sync.RWMutex
	items     map[interface{}]*list.Element //store data
	evictList *list.List                    //store priority for LRU
}
type lruItem struct {
	key        interface{}
	value      interface{}
	expiration *time.Time
}

//NewLRUCache create a LRUCache with size
func NewLRUCache(size int) *LRUCache {
	if !(size > 0) {
		panic("Cache size should bigger than 0")
	}
	c := &LRUCache{}
	c.Setsize(size)
	c.initLRUCache()
	return c
}

//initLRUCache init LRUCache.evictList and LRUCache.items
func (c *LRUCache) initLRUCache() {
	c.evictList = list.New()
	c.items = make(map[interface{}]*list.Element, c.size+1)
}

//Setsize set size
func (c *LRUCache) Setsize(size int) {
	c.size = size
}

//set set key-value in cache
func (c *LRUCache) set(key, value interface{}) interface{} {
	var item *lruItem
	//if exists,update the value and bring it to front
	if it, ok := c.items[key]; ok {
		c.evictList.MoveToFront(it)
		item = it.Value.(*lruItem)
		item.value = value
	} else {
		// if exceeded size,remove the last element
		if c.evictList.Len() >= c.size {
			c.evict()
		}
		item = &lruItem{
			key:   key,
			value: value,
		}
		c.items[key] = c.evictList.PushFront(item)
	}
	return item
}

// Set use lock to set key-value
func (c *LRUCache) Set(key, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(key, value)
}

//SetWithExpire set key-value with expire time
func (c *LRUCache) SetWithExpire(key, value interface{}, expiration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	it := c.set(key, value)
	t := time.Now().Add(expiration)
	it.(*lruItem).expiration = &t
}

//get get value by key and update priority
//return KeyNotFoundError when fail to find
func (c *LRUCache) get(key interface{}) (interface{}, error) {
	item, ok := c.items[key]
	if ok {
		it := item.Value.(*lruItem)
		if !it.IsExpired() {
			c.evictList.MoveToFront(item)
			v := it.value
			return v, nil
		}
		c.removeElement(item)
	}
	return nil, KeyNotFoundError
}

//Get use lock to get value
func (c *LRUCache) Get(key interface{}) (interface{}, error) {
	// get value will change priority of element
	// so need to use Lock() instead of RLock()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(key)
}

//has find key if exists
func (c *LRUCache) has(key interface{}) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	return !item.Value.(*lruItem).IsExpired()
}

//Has find key if exists and quicker than get because of use RLock
func (c *LRUCache) Has(key interface{}) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.has(key)
}

//remove remove key-value in cache
//success will return true
func (c *LRUCache) remove(key interface{}) bool {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

//Remove use lock to remove key-value
func (c *LRUCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.remove(key)
}

//Clear remove all key-value in cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initLRUCache()
}

//evict remove the oldest key-value
func (c *LRUCache) evict() {
	ent := c.evictList.Back()
	c.removeElement(ent)
}

//removeElement remove some Element
func (c *LRUCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*lruItem)
	delete(c.items, entry.key)
}

// IsExpired returns boolean value whether this item is expired or not.
func (it *lruItem) IsExpired() bool {
	if it.expiration == nil {
		return false
	}
	return it.expiration.Before(time.Now())
}
