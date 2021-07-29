package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewLRUCache(t *testing.T) {
	lru := NewLRUCache(5)
	assert.NotNil(t, lru)
	assert.NotNil(t, lru.items)
	assert.NotNil(t, lru.evictList)

	go func() {
		defer func() {
			err := recover()
			assert.NotNil(t, err)
		}()
		NewLRUCache(0)
	}()
}

func TestLRUCache_Set(t *testing.T) {
	var err error
	lru := NewLRUCache(2)
	lru.Set("a", "")
	lru.Set("b", "")
	lru.Set("c", "")
	lru.Set("d", "")
	assert.Equal(t, 2, len(lru.items))

	//LRU will remove the oldest element
	_, err = lru.Get("a")
	assert.Equal(t, KeyNotFoundError, err)
	_, err = lru.Get("b")
	assert.Equal(t, KeyNotFoundError, err)

	//set will update value and priority
	lru.Set("c", "c")
	value, err := lru.Get("c")
	assert.NoError(t, err)
	assert.Equal(t, "c", value.(string))

	lru.Set("e", "")
	_, err = lru.get("d")
	assert.Equal(t, KeyNotFoundError, err)

}

func TestLRUCache_Get(t *testing.T) {
	var err error
	lru := NewLRUCache(2)
	lru.Set("a", "a")
	lru.Set("b", "b")

	value, err := lru.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "a", value.(string))

}

func TestLRUCache_SetWithExpire(t *testing.T) {
	var err error
	lru := NewLRUCache(2)
	lru.SetWithExpire("a", "a", time.Nanosecond)
	time.Sleep(time.Nanosecond)

	//get will remove expired element
	value, err := lru.Get("a")
	assert.Equal(t, KeyNotFoundError, err)
	assert.Len(t, lru.items, 0)

	lru.SetWithExpire("a", "a", 1000*time.Second)
	value, err = lru.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "a", value.(string))
}

func TestLRUCache_Remove(t *testing.T) {
	lru := NewLRUCache(2)
	lru.Set("a", "a")
	lru.Set("b", "b")

	assert.False(t, lru.Remove("c"))
	assert.True(t, lru.Remove("a"))
	assert.Len(t, lru.items, 1)
}

func TestLRUCache_Clear(t *testing.T) {
	lru := NewLRUCache(2)
	lru.Set("a", "a")
	lru.Set("b", "b")
	lru.Clear()
	assert.Len(t, lru.items, 0)
}

func TestLRUCache_Has(t *testing.T) {
	lru := NewLRUCache(2)
	lru.Set("a", "a")
	assert.True(t, lru.Has("a"))
	assert.False(t, lru.Has("b"))
}
