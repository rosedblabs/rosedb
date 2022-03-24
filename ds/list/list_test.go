package list

import "testing"

func TestNewList(t *testing.T) {
	lis := New()

	key := []byte("my_list")
	//p1 := lis.LPush(key, []byte("11"))
	//t.Log(p1)

	lis.LPush(key, []byte("2311"))
	lis.LPush(key, []byte("asd"))
	lis.LPush(key, []byte("9903"))
	lis.RPush(key, []byte("1133"))
}
