package list

import (
	"fmt"
	"testing"
)

var key = "my_list"

func InitList() *List {
	list := New()

	list.LPush(key, []byte("a"), []byte("b"), []byte("c"))
	list.LPush(key, []byte("d"), []byte("e"), []byte("f"))

	return list
}

func PrintListData(lis *List) {
	if lis.record[key] == nil || lis.record[key].Len() <= 0 {
		fmt.Println("list is empty")
		return
	}

	for p := lis.record[key].Front(); p != nil; p = p.Next() {
		fmt.Print(string(p.Value.([]byte)), " ")
	}

	fmt.Println()
}

func TestList_LPush(t *testing.T) {
	list := InitList()

	size := list.LPush(key, []byte("rosedb"))
	PrintListData(list)
	t.Log("size = ", size)
}

func TestList_RPush(t *testing.T) {
	list := InitList()
	size := list.RPush(key, []byte("rose"), []byte("database"))

	PrintListData(list)
	t.Log("size = ", size)
}

func TestList_LPop(t *testing.T) {
	list := InitList()

	t.Log(string(list.LPop(key)))
	t.Log(string(list.LPop(key)))
	t.Log(string(list.LPop(key)))
	t.Log(string(list.LPop(key)))
	t.Log(string(list.LPop(key)))
	t.Log(string(list.LPop(key)))

	t.Log(list.record[key].Len())

	t.Log(string(list.LPop(key)))

	t.Log(list.record[key].Len())
}

func TestList_RPop(t *testing.T) {
	list := InitList()

	t.Log(list.record[key].Len())

	t.Log(string(list.RPop(key)))
	t.Log(string(list.RPop(key)))
	t.Log(string(list.RPop(key)))
	t.Log(string(list.RPop(key)))
	t.Log(string(list.RPop(key)))
	t.Log(string(list.RPop(key)))

	t.Log(list.record[key].Len())
}

func TestList_LIndex(t *testing.T) {
	list := InitList()

	//f e d c b a
	t.Log(string(list.LIndex(key, 1)))
	t.Log(string(list.LIndex(key, 5)))
	t.Log(string(list.LIndex(key, 0)))

	t.Log(string(list.LIndex(key, -1)))
	t.Log(string(list.LIndex(key, -3)))
	t.Log(string(list.LIndex(key, -6)))

	t.Log(string(list.LIndex(key, -7)))
	t.Log(string(list.LIndex(key, 100)))
	t.Log(string(list.LIndex(key, -100)))
}

func TestList_LRem(t *testing.T) {
	list := InitList()

	PrintListData(list)
	rem := list.LRem(key, []byte("a"), 0)
	t.Log(rem)

	rem = list.LRem(key, []byte("f"), 0)
	t.Log(rem)

	rem = list.LRem(key, []byte("e"), 0)
	t.Log(rem)

	rem = list.LRem(key, []byte("d"), -12)
	t.Log(rem)

	rem = list.LRem(key, []byte("c"), 23)
	t.Log(rem)

	rem = list.LRem(key, []byte("b"), 0)
	t.Log(rem)

	PrintListData(list)

	t.Log("--------another test for duplicate data---------")
	lis := New()
	lis.RPush(key, []byte("a"), []byte("c"), []byte("a"), []byte("a"))

	rem = lis.LRem(key, []byte("a"), 0)
	t.Log(rem)
	PrintListData(lis)

	rem = lis.LRem(key, []byte("a"), 1)
	t.Log(rem)
	PrintListData(lis)

	rem = lis.LRem(key, []byte("c"), -1)
	t.Log(rem)
	PrintListData(lis)
}

func TestList_LInsert(t *testing.T) {

	list := InitList()

	t.Run("before", func(t *testing.T) {
		n := list.LInsert(key, Before, []byte("a"), []byte("AA"))
		t.Log(n)

		n = list.LInsert(key, Before, []byte("f"), []byte("FF"))
		t.Log(n)

		n = list.LInsert(key, Before, []byte("e"), []byte("EE"))
		t.Log(n)

		PrintListData(list)
	})

	t.Run("after", func(t *testing.T) {
		n := list.LInsert(key, After, []byte("a"), []byte("AA"))
		t.Log(n)

		n = list.LInsert(key, After, []byte("f"), []byte("FF"))
		t.Log(n)

		n = list.LInsert(key, After, []byte("e"), []byte("EE"))
		t.Log(n)

		PrintListData(list)
	})
}

func TestList_LSet(t *testing.T) {
	list := InitList()
	ok := list.LSet(key, 0, []byte("FF"))
	t.Log(ok)

	ok = list.LSet(key, 5, []byte("AA"))
	t.Log(ok)

	PrintListData(list)

	ok = list.LSet(key, -1, []byte("AAAA"))
	t.Log(ok)

	ok = list.LSet(key, -21, []byte("AA"))
	t.Log(ok)
	PrintListData(list)
}

func TestList_LRange(t *testing.T) {
	list := InitList()

	printRes := func(res [][]byte) {
		for _, r := range res {
			fmt.Print(string(r), " ")
		}
		fmt.Println()
	}

	res := list.LRange(key, 5, 3)
	printRes(res)

	res = list.LRange(key, 0, -1)
	printRes(res)
}

func TestList_LLen(t *testing.T) {
	list := InitList()

	t.Log(list.LLen(key))

	l := New()
	t.Log(l.LLen(key))
	l.RPush(key, []byte("a"))
	t.Log(l.LLen(key))
}

func TestList_LTrim(t *testing.T) {

	t.Run("test1", func(t *testing.T) {
		list := InitList()
		//f e d c b a

		trim := list.LTrim(key, 3, -5)
		t.Log(trim)

		PrintListData(list)
	})
	//
	//t.Run("large data test", func(t *testing.T) {
	//	newLIst := New()
	//	for i := 0; i < 100000; i++ {
	//		newLIst.RPush(key, []byte(strconv.Itoa(i)))
	//	}
	//
	//	newLIst.LTrim(key, 0, -1)
	//	//newLIst.LTrim(key, 75000, 200000)
	//	//newLIst.LTrim(key, 30000, 35000)
	//
	//	t.Log(newLIst.LLen(key))
	//	PrintListData(newLIst)
	//})
}

func TestList_LKeyExists(t *testing.T) {
	lis := InitList()
	ok1 := lis.LKeyExists(key)
	t.Log(ok1)

	ok2 := lis.LKeyExists("not")
	t.Log(ok2)
}

func TestList_LValExists(t *testing.T) {
	lis := InitList()

	ok1 := lis.LValExists(key, []byte("a"))
	t.Log(ok1)

	ok2 := lis.LValExists(key, []byte("f"))
	t.Log(ok2)

	ok3 := lis.LValExists(key, []byte("aaa"))
	t.Log(ok3)

	lis.RPop(key)
	lis.RPop(key)
	lis.RPop(key)

	lis.RPush(key, []byte("a"))

	ok4 := lis.LValExists(key, []byte("a"))
	t.Log(ok4)

	PrintListData(lis)
}
