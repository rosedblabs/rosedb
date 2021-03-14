package storage

import (
	"fmt"
	"math"
	"testing"
)

func TestExpires_SaveExpires(t *testing.T) {
	//expires := make(Expires)
	//expires["key_001"] = 43312223
	//expires["key_002"] = 18334312
	//expires["key_003"] = 2312223
	//expires["key_005"] = 7312223
	//
	//err := expires.SaveExpires("/Users/roseduan/resources/rosedb/db.expires")
	//if err != nil {
	//	log.Println(err)
	//} else {
	//	t.Log("操作完成")
	//}

	fmt.Println(math.MaxUint32)
	fmt.Println(math.MaxUint32 / 60 / 60 / 24 / 365)

	newExpires := LoadExpires("/Users/roseduan/resources/rosedb/db.expires")
	t.Logf("%+v\n", newExpires)
	for k, v := range newExpires {
		fmt.Println(k, ":", v)
	}

	res := test()
	fmt.Println(res)
}

func test() uint32 {
	return 0
}
