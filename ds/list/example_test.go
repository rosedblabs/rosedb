package list

import "fmt"

var listKey = "my_list"

func Example() {
	list := New()
	list.LPush(listKey, [][]byte{[]byte("a"), []byte("b"), []byte("c")}...)
	list.RPush(listKey, [][]byte{[]byte("a"), []byte("b"), []byte("c")}...)

	list.LPop(listKey)
	list.RPop(listKey)

	length := list.LLen(listKey)
	fmt.Println(length)

	list.LSet(listKey, 10, []byte("d"))
}
