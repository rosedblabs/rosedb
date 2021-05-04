package index

import "fmt"

func Example() {
	skl := NewSkipList()
	val := []byte("test_val")

	skl.Put([]byte("ec"), val)
	skl.Put([]byte("dc"), val)
	skl.Put([]byte("ac"), val)
	skl.Put([]byte("ae"), val)
	skl.Put([]byte("fe"), val)

	ok := skl.Exist([]byte("ac"))
	fmt.Println(ok)

	skl.Remove([]byte("ec"))

	ele := skl.Get([]byte("dc"))
	fmt.Printf("%+v", ele)

	pre := skl.FindPrefix([]byte("a"))
	fmt.Printf("%+v", pre)
}
