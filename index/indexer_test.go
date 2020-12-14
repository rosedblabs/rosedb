package index

import (
	"log"
	"rosedb/ds/skiplist"
	"testing"
)

func TestIndexer(t *testing.T) {

	path := "/Users/roseduan/resources/rosedb/temp.idx"

	key := []byte("test_key")
	val := []byte("test_val")
	i1 := &Indexer{
		Key:       key,
		Value:     val,
		EntrySize: 132,
		FileId:    23,
		Offset:    39983,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(val)),
	}

	key2 := []byte("test_key2")
	i2 := &Indexer{
		Key:       key2,
		EntrySize: 1322,
		FileId:    3,
		Offset:    9383,
		KeySize:   uint32(len(key2)),
	}

	t.Run("encode1", func(t *testing.T) {
		b := i1.encode()
		t.Logf("%v", b)
	})

	t.Run("encode2", func(t *testing.T) {
		b := i2.encode()
		t.Logf("%v", b)
	})

	t.Run("store", func(t *testing.T) {
		list := skiplist.New()
		list.Put(i1.Key, i1)
		list.Put(i2.Key, i2)

		err := Store(list, path)
		if err != nil {
			log.Fatal(err)
		}
	})

	t.Run("load", func(t *testing.T) {
		list := skiplist.New()
		err := Build(list, path)
		if err != nil {
			log.Printf("加载索引失败 %v", err)
		}

		t.Log(list.Size)
		t.Logf("%+v", list.Get(key).Value().(*Indexer))
		t.Logf("%+v", list.Get(key2).Value().(*Indexer))
	})
}
