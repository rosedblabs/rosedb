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

// GetTestKey get formatted key, for test only
func GetTestKey(i int) []byte {
	return fmt.Appendf(nil, "rosedb-test-key-%09d", i)
}

// RandomValue generate random value, for test only
func RandomValue(n int) []byte {
	b := make([]byte, n)
	lock.Lock()
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	lock.Unlock()
	return []byte("rosedb-test-value-" + string(b))
}
