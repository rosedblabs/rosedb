package cache

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

type Cache struct {
	capacity  int
	cacheMap  map[interface{}]*list.Element
	cacheList *list.List
	mu        sync.Mutex
}

type lruItem struct {
	key   string
	value []byte
}

func NewCache(capacity int) *Cache {
	return &Cache{
		capacity:  capacity,
		cacheMap:  make(map[interface{}]*list.Element),
		cacheList: list.New(),
	}
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
	keyStr := convert(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.get(keyStr)
}

func (c *Cache) Set(key, value []byte) {
	keyStr := convert(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(keyStr, value)
}

func (c *Cache) get(key interface{}) ([]byte, bool) {
	ele, ok := c.cacheMap[key]
	if ok {
		c.cacheList.MoveToFront(ele)
		item := ele.Value.(*lruItem)
		return item.value, true
	}
	return nil, false
}

func (c *Cache) set(key string, value []byte) {
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

func (c *Cache) removeOldest() {
	ele := c.cacheList.Back()
	c.cacheList.Remove(ele)
	item := ele.Value.(*lruItem)
	delete(c.cacheMap, item.key)
}

func convert(bytes []byte) string {
	var str strings.Builder
	for _, b := range bytes {
		str.WriteString(fmt.Sprintf("%d,", int(b)))
	}
	return str.String()[:str.Len()-1]
}
