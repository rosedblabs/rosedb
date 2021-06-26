package storage

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"log"
	"os"
	"testing"
	"time"
)

func TestNewEntry(t *testing.T) {
	key, val := []byte("test_key"), []byte("test_val")
	extra := []byte("extra val")
	_ = NewEntry(key, val, extra, String, 0)
}

func TestNewEntryWithExpire(t *testing.T) {

	t.Run("normal", func(t *testing.T) {
		key, val := []byte("test_key_001"), []byte("test_val_001")
		deadline := time.Now().Add(time.Second * 21).Unix()
		e := NewEntryWithExpire(key, val, deadline, String, 2)
		assert.NotEqual(t, e, nil)
	})

	t.Run("val nil", func(t *testing.T) {
		deadline := time.Now().Add(time.Second * 21).Unix()
		e := NewEntryWithExpire([]byte("aa"), nil, deadline, String, 2)
		assert.NotEqual(t, e, nil)
	})
}

func TestEntry_GetType(t *testing.T) {
	key, val := []byte("test_key_001"), []byte("test_val_001")
	deadline := time.Now().Add(time.Second * 21).Unix()
	e := NewEntryWithExpire(key, val, deadline, ZSet, 2)

	assert.Equal(t, e.GetType(), uint16(4))
}

func TestEntry_GetMark(t *testing.T) {
	key, val := []byte("test_key_001"), []byte("test_val_001")
	deadline := time.Now().Add(time.Second * 21).Unix()
	e := NewEntryWithExpire(key, val, deadline, ZSet, 15)

	assert.Equal(t, e.GetMark(), uint16(15))
}

func TestNewEntryNoExtra(t *testing.T) {
	_ = NewEntryNoExtra([]byte("key001"), []byte("val001"), 1, 2)
}

func TestEntry_Size(t *testing.T) {
	e := NewEntryNoExtra([]byte("key001"), []byte("val001"), 1, 2)
	e.Size()
}

func TestEntry_Encode(t *testing.T) {
	//正常key和value的情况
	t.Run("test1", func(t *testing.T) {
		e := &Entry{
			Meta: &Meta{
				Key:   []byte("test_key_0001"),
				Value: []byte("test_value_0001"),
			},
		}

		e.Meta.KeySize = uint32(len(e.Meta.Key))
		e.Meta.ValueSize = uint32(len(e.Meta.Value))

		encVal, err := e.Encode()
		if err != nil {
			log.Fatal(err)
		}
		t.Log(e.Size())
		t.Log(encVal)

		//写入文件为了测试下面的Decode方法
		if encVal != nil {
			file, _ := os.OpenFile("/tmp/rosedb/test.dat", os.O_CREATE|os.O_WRONLY, 0644)
			file.Write(encVal)
		}
	})

	//value为空的情况
	t.Run("test2", func(t *testing.T) {
		e := &Entry{
			Meta: &Meta{
				Key: []byte("test_key_0001"),
			},
		}

		e.Meta.KeySize = uint32(len(e.Meta.Key))
		e.Meta.ValueSize = uint32(len(e.Meta.Value))

		encVal, err := e.Encode()
		if err != nil {
			log.Fatal(err)
		}
		t.Log(e.Size())
		t.Log(encVal)
	})
}

func TestDecode(t *testing.T) {
	//expected val : [169 64 25 4 0 0 0 13 0 0 0 15 116 101 115 116 95 107 101 121 95 48 48 48 49 116 101 115 116 95 118 97 108 117 101 95 48 48 48 49]
	if file, err := os.OpenFile("/tmp/rosedb/test.dat", os.O_RDONLY, os.ModePerm); err != nil {
		t.Error("open File err ", err)
	} else {
		buf := make([]byte, entryHeaderSize)
		var offset int64 = 0
		if n, err := file.ReadAt(buf, offset); err != nil {
			t.Error("read data err ", err)
		} else {
			t.Log("success read ", n)

			t.Log(buf)
			e, _ := Decode(buf)

			//read key
			offset += entryHeaderSize
			if e.Meta.KeySize > 0 {
				key := make([]byte, e.Meta.KeySize)
				file.ReadAt(key, offset)
				e.Meta.Key = key
			}

			//read value
			offset += int64(e.Meta.KeySize)
			if e.Meta.ValueSize > 0 {
				val := make([]byte, e.Meta.ValueSize)
				file.ReadAt(val, offset)
				e.Meta.Value = val
			}

			t.Logf("Key = %s, Value = %s, KeySize = %d, ValueSize = %d\n",
				string(e.Meta.Key), string(e.Meta.Value), e.Meta.KeySize, e.Meta.ValueSize)

			checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
			t.Log(checkCrc, e.crc32)
		}
	}
}
