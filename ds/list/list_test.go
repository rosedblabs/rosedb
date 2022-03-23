package list

import "testing"

func TestNewList(t *testing.T) {
	lis := New()

	key := []byte("my_list")
	p1 := lis.LPush(key, []byte("11"))
	t.Log(p1)

	v0, p2 := lis.LPop(key)
	t.Log("pop val = ", string(v0))
	t.Log(p2)

	//v := lis.RPop(key)
	//t.Log("pop val = ", string(v))
	//
	//v2 := lis.RPop(key)
	//t.Log("pop val = ", string(v2))
	//lis.LPush(key, []byte("231"))
	//lis.LPush(key, []byte("asd"))
	//lis.LPush(key, []byte("9903"))
	//
	//v1 := lis.LPop(key)
	//t.Log("pop val = ", string(v1))
	//
	//v2 := lis.LPop(key)
	//t.Log("pop val = ", string(v2))
}
