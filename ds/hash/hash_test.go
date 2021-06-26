package hash

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

var key = "my_hash"

func InitHash() *Hash {
	hash := New()

	hash.HSet(key, "a", []byte("hash_data_001"))
	hash.HSet(key, "b", []byte("hash_data_002"))
	hash.HSet(key, "c", []byte("hash_data_003"))

	return hash
}

func TestNew(t *testing.T) {
	hash := New()
	assert.NotEqual(t, hash, nil)
}

func TestHash_HSet(t *testing.T) {
	hash := InitHash()
	r1 := hash.HSet("my_hash", "d", []byte("123"))
	assert.Equal(t, r1, 1)
	r2 := hash.HSet("my_hash", "d", []byte("123"))
	assert.Equal(t, r2, 0)
	r3 := hash.HSet("my_hash", "e", []byte("234"))
	assert.Equal(t, r3, 1)
}

func TestHash_HSetNx(t *testing.T) {
	hash := InitHash()
	r1 := hash.HSetNx(key, "a", []byte("new one"))
	assert.Equal(t, r1, 0)
	r2 := hash.HSetNx(key, "f", []byte("d-new one"))
	assert.Equal(t, r2, 1)
	r3 := hash.HSetNx(key, "f", []byte("d-new one"))
	assert.Equal(t, r3, 0)
}

func TestHash_HGet(t *testing.T) {
	hash := InitHash()

	val := hash.HGet(key, "a")
	assert.Equal(t, []byte("hash_data_001"), val)
	valNotExist := hash.HGet(key, "m")
	assert.Equal(t, []byte(nil), valNotExist)
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
	//delete existed filed,return 1
	num1 := hash.HDel(key, "a")
	assert.Equal(t, 1, num1)
	//delete same field twice,return 0
	num0 := hash.HDel(key, "a")
	assert.Equal(t, 0, num0)
	//delete non existing field,expect 0
	numNotExist0 := hash.HDel(key, "m")
	assert.Equal(t, 0, numNotExist0)
}

func TestHash_HExists(t *testing.T) {
	hash := InitHash()
	// key and field both exist
	exist := hash.HExists(key, "a")
	assert.Equal(t, 1, exist)
	// key is non existing
	keyNot := hash.HExists("non exiting key", "a")
	assert.Equal(t, 0, keyNot)
	not := hash.HExists(key, "m")
	assert.Equal(t, 0, not)

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

func TestHash_HVals(t *testing.T) {
	hash := InitHash()
	values := hash.HVals(key)
	for i, v := range values {
		assert.Equal(t, []byte("hash_data_00"+strconv.Itoa(i+1)), v)
		t.Log(string(v))
	}
}

func TestHash_HLen(t *testing.T) {
	hash := InitHash()
	assert.Equal(t, 3, hash.HLen(key))
}

func TestHash_HClear(t *testing.T) {
	hash := InitHash()
	hash.HClear(key)

	v := hash.HGet(key, "a")
	assert.Equal(t, len(v), 0)
}

func TestHash_HKeyExists(t *testing.T) {
	hash := InitHash()
	exists := hash.HKeyExists(key)
	assert.Equal(t, exists, true)

	hash.HClear(key)

	exists1 := hash.HKeyExists(key)
	assert.Equal(t, exists1, false)
}
