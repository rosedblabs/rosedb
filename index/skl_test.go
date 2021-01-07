package index

import (
	"fmt"
	"testing"
)

type Employee struct {
	id   uint32
	name string
	age  uint8
}

func TestSkipList_Put(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)
	list.Put([]byte("ac"), val)
	list.Put([]byte("ae"), val)
	list.Put([]byte("bc"), val)
	list.Put([]byte("22"), val)
	list.Put([]byte("2"), val)
	list.Put([]byte("bc"), val)
	list.Put([]byte("xc"), val)
	list.Put([]byte("34"), val)
	list.Put([]byte("13"), val)

	e := list.Front()
	for p := e; p != nil; p = p.Next() {
		t.Logf("key = %+v, val = %+v", p.Key(), p.Value())
	}
}

func TestSkipList_Get(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), 123)
	list.Put([]byte("ac"), val)

	list.Put([]byte("111"), Employee{3330912, "mary", 24})

	t.Logf("%v \n", list.Get([]byte("ec")))
	t.Logf("%v \n", list.Get([]byte("ac")))
	t.Logf("%v \n", list.Get([]byte("111")))
}

func TestSkipList_Remove(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), 123)
	list.Put([]byte("ac"), val)

	t.Log(list.Len)
	list.Remove([]byte("dc"))
	list.Remove([]byte("ec"))
	list.Remove([]byte("ac"))
	t.Log(list.Len)
}

func TestSkipList_Foreach(t *testing.T) {
	list := NewSkipList()
	val1 := []byte("test_val1")
	val2 := []byte("test_val2")
	val3 := []byte("test_val3")
	val4 := []byte("test_val4")

	list.Put([]byte("ec"), val1)
	list.Put([]byte("dc"), val2)
	list.Put([]byte("ac"), val3)
	list.Put([]byte("ae"), val4)

	keys := func(e *Element) bool {
		t.Logf("%s ", e.key)
		return false
	}

	list.Foreach(keys)

	vals := func(e *Element) bool {
		t.Logf("%s ", e.value)
		return true
	}

	list.Foreach(vals)
}

func TestSkipList_Foreach2(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)
	list.Put([]byte("ac"), val)
	list.Put([]byte("ae"), val)

	list.Foreach(func(e *Element) bool {
		e.value = []byte("test_val_002")
		return true
	})

	for p := list.Front(); p != nil; p = p.Next() {
		fmt.Printf("%s %s \n", string(p.Key()), string(p.Value().([]byte)))
	}
}

func TestElement_SetValue(t *testing.T) {
	list := NewSkipList()
	list.Put([]byte("a"), []byte("13"))
	list.Put([]byte("a"), []byte("19"))

	t.Log(list.Len)
	val := list.Get([]byte("a")).Value().([]byte)
	t.Log(string(val))
}

func TestSkipList_PrefixScan(t *testing.T) {
	list := NewSkipList()
	list.Put([]byte("acccbf"), 132)
	list.Put([]byte("acceew"), 44)
	list.Put([]byte("acadef"), 124)
	list.Put([]byte("accdef"), 232)

	e1 := list.FindPrefix([]byte("eee"))
	t.Logf("%+v", e1)

	e2 := list.FindPrefix([]byte("acc"))
	t.Logf("%+v", e2)

	e3 := list.FindPrefix([]byte("accc"))
	t.Logf("%+v", e3)
}
