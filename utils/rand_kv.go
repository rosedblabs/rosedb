package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey get formated key, for test only
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("rosedb-test-key-%09d", i))
}

// RandomValue generate random value, for test only
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("rosedb-test-value-" + string(b))
}
