package cache

import "fmt"

func Example() {
	lru := NewLRUCache(5)

	lru.Set("a", "a")
	lru.Set("b", "b")
	lru.Set("c", "c")
	lru.Set("d", "d")
	lru.Set("e", "e")

	exists := lru.Has("a")
	fmt.Println(exists)

	value, _ := lru.Get("a")
	fmt.Println(value.(string))

	ok := lru.Remove("a")
	fmt.Println(ok)
}
