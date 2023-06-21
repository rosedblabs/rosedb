package utils

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	lock    = sync.Mutex{}
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 获取测试使用的 key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("rosedb-test-key-%09d", i))
}

// RandomValue 生成随机 value，用于测试
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		lock.Lock()
		b[i] = letters[randStr.Intn(len(letters))]
		lock.Unlock()
	}
	return []byte("rosedb-test-value-" + string(b))
}
