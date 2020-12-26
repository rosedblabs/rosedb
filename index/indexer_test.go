package index

import (
	"log"
	"math/rand"
	"rosedb/ds/skiplist"
	"rosedb/storage"
	"strconv"
	"testing"
	"time"
)

func TestIndexer(t *testing.T) {

	path := "/Users/roseduan/resources/rosedb/temp.idx"

	key := []byte("test_key")
	val := []byte("test_val")
	i1 := &Indexer{
		Meta: &storage.Meta{
			Key:       key,
			Value:     val,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(val)),
		},
		EntrySize: 132,
		FileId:    23,
		Offset:    39983,
	}

	key2 := []byte("test_key2")
	i2 := &Indexer{
		Meta: &storage.Meta{
			Key:     key2,
			KeySize: uint32(len(key2)),
		},
		EntrySize: 1322,
		FileId:    3,
		Offset:    9383,
	}

	t.Run("encode1", func(t *testing.T) {
		b := i1.encode()
		t.Logf("%v", b)
	})

	t.Run("encode2", func(t *testing.T) {
		b := i2.encode()
		t.Logf("%v", b)
	})

	t.Run("store index info", func(t *testing.T) {
		list := skiplist.New()
		list.Put(i1.Meta.Key, i1)
		list.Put(i2.Meta.Key, i2)

		err := Store(list, path)
		if err != nil {
			log.Fatal(err)
		}
	})

	t.Run("build index", func(t *testing.T) {
		list := skiplist.New()
		err := Build(list, path)
		if err != nil {
			log.Printf("build index error %v", err)
		}

		t.Log(list.Len)
		t.Logf("%+v", list.Get(key).Value().(*Indexer))
		t.Logf("%+v", list.Get(key2).Value().(*Indexer))
	})

	t.Run("test store large data", func(t *testing.T) {
		rand.Seed(time.Now().Unix())
		keyPrefix := "test_key_"
		valPrefix := "test_value_"

		list := skiplist.New()
		for i := 0; i < 100000; i++ {
			key := []byte(keyPrefix + strconv.Itoa(rand.Intn(1000000)))
			val := []byte(valPrefix + strconv.Itoa(rand.Intn(1000000)))

			i := &Indexer{
				Meta: &storage.Meta{
					Key:       key,
					Value:     val,
					KeySize:   uint32(len(key)),
					ValueSize: uint32(len(val)),
				},
				EntrySize: 132,
				FileId:    23,
				Offset:    39983,
			}

			list.Put(key, i)
		}

		err := Store(list, path)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("test build large data", func(t *testing.T) {
		list := skiplist.New()

		err := Build(list, path)
		if err != nil {
			t.Error(err)
		}

		t.Log(list.Len)

		printInfo := func(i *Indexer) {
			t.Logf("%+v", i)
			t.Logf("meta = %+v", i.Meta)
			t.Logf("key = %+v, val = %+v", string(i.Meta.Key), string(i.Meta.Value))

			t.Log("---------")
		}

		p := list.Front()
		for i := 0; i < 10; i++ {
			i := p.Value().(*Indexer)
			printInfo(i)

			p = p.Next()
		}
	})
}
