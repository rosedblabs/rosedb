package utils

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func BenchmarkEncodeKey(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tt := &TestKey{
			Id:   i,
			Name: "roseduan",
		}
		_, err := EncodeKey(tt)
		if err != nil {
			panic(err)
		}
	}
}

func TestEncodeKey(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		a := 100
		v, err := EncodeKey(a)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, len(v), 0)
	})
}

func TestEncodeValue(t *testing.T) {
	t.Run("byte", func(t *testing.T) {
		b := []byte("roseduan")
		res, err := EncodeValue(b)
		assert.Equal(t, err, nil)
		t.Log(res)

		var r []byte
		err = DecodeValue(res, &r)
		assert.Equal(t, err, nil)
		t.Log("val = ", string(r))
	})

	t.Run("struct", func(t *testing.T) {
		v := &TestKey{
			Id:   9943,
			Name: "roseduan",
		}
		res, err := EncodeValue(v)
		assert.Equal(t, err, nil)

		t.Log(res)

		r := &TestKey{}
		err = DecodeValue(res, r)
		assert.Equal(t, err, nil)
		t.Log(r)
	})
}

type TestKey struct {
	Id   int
	Name string
}

func TestKeyWithSeq(t *testing.T) {
	v1, err := KeyWithSeq(0, 1)
	assert.Nil(t, err)
	t.Log(string(v1))

	v2, err := KeyWithSeq(0, 1)
	t.Log(string(v2))

	com := bytes.Compare(v1, v2)
	t.Log(com)
}
