package hash

import "fmt"

var (
	hashKey = "my_hash"
)

func Example() {
	// create a new Hash and run some functions
	hash := New()
	hash.HSet(getTestIndexer(hashKey, "field1", "Coding"))
	hash.HSet(getTestIndexer(hashKey, "field2", "Writing"))

	ok := hash.HExists(hashKey, "field2")
	fmt.Println(ok)

	val := hash.HGet(hashKey, "field1")
	fmt.Println(val.Meta.Value)

	hash.HDel(hashKey, "field1")

	keys := hash.HKeys(hashKey)
	for _, v := range keys {
		fmt.Println(v)
	}
}
