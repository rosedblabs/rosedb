package storage

import (
	"hash/crc32"
	"log"
	"os"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	//正常key和value的情况
	t.Run("test1", func(t *testing.T) {
		e := &Entry{
			Key:   []byte("test_key_0001"),
			Value: []byte("test_value_0001"),
		}

		e.keySize = uint32(len(e.Key))
		e.valueSize = uint32(len(e.Value))

		encVal, err := e.Encode()
		if err != nil {
			log.Fatal(err)
		}
		t.Log(e.Size())
		t.Log(encVal)

		//写入文件为了测试下面的Decode方法
		if encVal != nil {
			file, _ := os.OpenFile("/Users/roseduan/resources/rosedb/test.dat", os.O_CREATE|os.O_WRONLY, 0644)
			file.Write(encVal)
		}
	})

	//value为空的情况
	t.Run("test2", func(t *testing.T) {
		e := &Entry{
			Key: []byte("test_key_0001"),
		}

		e.keySize = uint32(len(e.Key))
		e.valueSize = uint32(len(e.Value))

		encVal, err := e.Encode()
		if err != nil {
			log.Fatal(err)
		}
		t.Log(e.Size())
		t.Log(encVal)
	})

	//key为空的情况
	t.Run("test3", func(t *testing.T) {
		e := &Entry{
			Key:   []byte(""),
			Value: []byte("val_001"),
		}

		e.keySize = uint32(len(e.Key))
		e.valueSize = uint32(len(e.Value))

		if encode, err := e.Encode(); err != nil {
			t.Error(err)
		} else {
			t.Log(encode)
		}
	})
}

func TestDecode(t *testing.T) {
	//expected val : [169 64 25 4 0 0 0 13 0 0 0 15 116 101 115 116 95 107 101 121 95 48 48 48 49 116 101 115 116 95 118 97 108 117 101 95 48 48 48 49]
	if file, err := os.OpenFile("/Users/roseduan/resources/rosedb/test.dat", os.O_RDONLY, os.ModePerm); err != nil {
		t.Error("open File err ", err)
	} else {
		buf := make([]byte, 40)
		if n, err := file.ReadAt(buf, 0); err != nil {
			t.Error("read data err ", err)
		} else {
			t.Log("success read ", n)

			t.Log(buf)
			e, _ := Decode(buf)
			t.Logf("Key = %s, Value = %s, keySize = %d, valueSize = %d\n",
				string(e.Key), string(e.Value), e.keySize, e.valueSize)

			checkCrc := crc32.ChecksumIEEE(e.Value)
			t.Log(checkCrc, e.crc32)
		}
	}
}
