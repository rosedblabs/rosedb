package hash

import "testing"

func TestHash_HSet(t *testing.T) {
	hash := New()

	n := hash.HSet("my_hash", "a", []byte("123"))
	t.Log(n)
}

func TestHash_HGet(t *testing.T) {
	a := "acd"
	b := "ace"

	t.Log(a < b)
}
