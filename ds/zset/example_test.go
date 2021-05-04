package zset

import "fmt"

var zsetKey = "my_zset"

func Example() {
	zset := New()

	zset.ZAdd(zsetKey, 12.1, "PHP")
	zset.ZAdd(zsetKey, 34.23, "Java")
	zset.ZAdd(zsetKey, 23.5, "Python")

	val := zset.ZGetByRank(zsetKey, 2)
	for _, v := range val {
		fmt.Printf("%+v", v)
	}

	zset.ZRange(zsetKey, 1, 10)
}
