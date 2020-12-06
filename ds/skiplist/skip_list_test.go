package skiplist

import (
	"rosedb/index"
	"testing"
)

func TestSkipList_Add(t *testing.T) {
	list := New()

	e1 := &index.Indexer{Key: []byte("accedf")}
	e2 := &index.Indexer{Key: []byte("abcedf")}
	e3 := &index.Indexer{Key: []byte("acdedf")}
	e4 := &index.Indexer{Key: []byte("accegk")}
	e5 := &index.Indexer{Key: []byte("bccedf")}
	e6 := &index.Indexer{Key: []byte("bacedf")}

	list.Add(e1)
	list.Add(e2)
	list.Add(e3)
	list.Add(e4)
	list.Add(e5)
	list.Add(e6)

	t.Log(list.elemSize)
}

func TestSkipList_Find(t *testing.T) {
	list := New()

	e1 := &index.Indexer{Key: []byte("a")}
	e2 := &index.Indexer{Key: []byte("c")}
	e3 := &index.Indexer{Key: []byte("g")}
	e4 := &index.Indexer{Key: []byte("e")}
	e5 := &index.Indexer{Key: []byte("b")}
	e6 := &index.Indexer{Key: []byte("f")}

	list.Add(e1)
	list.Add(e2)
	list.Add(e3)
	list.Add(e4)
	list.Add(e5)
	list.Add(e6)

	node := list.Find([]byte("e"))
	//向后的元素
	t.Log("向后的元素")
	for p := node; p != nil; p = p.next[0] {
		t.Log(string(p.key))
	}

	t.Log("向前的元素")
	for p := node; p != nil; p = p.prev {
		t.Log(string(p.key))
	}
}

func TestSkipList_Remove(t *testing.T) {
	list := New()

	e1 := &index.Indexer{Key: []byte("a")}
	e2 := &index.Indexer{Key: []byte("c")}
	e3 := &index.Indexer{Key: []byte("g")}
	e4 := &index.Indexer{Key: []byte("e")}
	e5 := &index.Indexer{Key: []byte("b")}
	e6 := &index.Indexer{Key: []byte("f")}
	e7 := &index.Indexer{Key: []byte("e")}
	e8 := &index.Indexer{Key: []byte("e")}

	list.Add(e1)
	list.Add(e2)
	list.Add(e3)
	list.Add(e4)
	list.Add(e5)
	list.Add(e6)
	list.Add(e7)
	list.Add(e8)

	t.Log(list.Size())
	list.Remove(e4.Key)
	t.Log(list.Size())
}
