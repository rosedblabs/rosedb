package list

import "testing"

func TestNewList(t *testing.T) {
	lis := New()

	key := []byte("my_list")
	//p1 := lis.LPush(key, []byte("11"))
	//t.Log(p1)

	lis.RPush(key, []byte("a"))
	//lis.LPush(key, []byte("b"))
	//lis.LPush(key, []byte("c"))
	//lis.LPush(key, []byte("d"))
	//
	v1 := lis.LIndex(key, -1)
	t.Log(string(v1))
}
