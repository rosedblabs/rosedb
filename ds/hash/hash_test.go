package hash

import "testing"

var key = "my_hash"

func InitHash() *Hash {
	hash := New()

	hash.HSet(key, "a", []byte("hash_data_001"))
	hash.HSet(key, "b", []byte("hash_data_002"))
	hash.HSet(key, "c", []byte("hash_data_003"))

	return hash
}

func TestHash_HSet(t *testing.T) {
	hash := InitHash()
	_ = hash.HSet("my_hash", "d", []byte("123"))
	_ = hash.HSet("my_hash", "e", []byte("234"))
}

func TestHash_HSetNx(t *testing.T) {
	hash := InitHash()
	hash.HSetNx(key, "a", []byte("new one"))
	hash.HSetNx(key, "d", []byte("d-new one"))
}

func TestHash_HGet(t *testing.T) {
	hash := InitHash()

	val := hash.HGet(key, "a")
	t.Log(string(val))
	t.Log(string(hash.HGet(key, "c")))
	t.Log(string(hash.HGet(key, "m")))
}

func TestHash_HGetAll(t *testing.T) {
	hash := InitHash()

	vals := hash.HGetAll(key)
	for _, v := range vals {
		t.Log(string(v))
	}
}

func TestHash_HDel(t *testing.T) {
	hash := InitHash()

	_ = hash.HDel(key, "a")
	_ = hash.HDel(key, "c")
}

func TestHash_HExists(t *testing.T) {
	hash := InitHash()

	t.Log(hash.HExists(key, "a"))
	t.Log(hash.HExists(key, "c"))
	t.Log(hash.HExists(key, "s"))
}

func TestHash_HKeys(t *testing.T) {
	hash := InitHash()

	keys := hash.HKeys(key)
	for _, k := range keys {
		t.Log(k)
	}

	res := hash.HKeys("no")
	t.Log(len(res))
}

func TestHash_HValues(t *testing.T) {
	hash := InitHash()

	values := hash.HValues(key)
	for _, v := range values {
		t.Log(string(v))
	}
}

func TestHash_HLen(t *testing.T) {
	hash := InitHash()
	t.Log(hash.HLen(key))
}
