package set

import "fmt"

var setKey = "my_set"

func Example() {
	// create a new set and run some functions
	set := New()
	set.SAdd(setKey, []byte("Java"))
	set.SAdd(setKey, []byte("Golang"))

	card := set.SCard(setKey)
	fmt.Println(card)

	val := set.SPop(setKey, 1)
	for _, v := range val {
		fmt.Println(string(v))
	}

	ok := set.SRem(setKey, []byte("Golang"))
	fmt.Println(ok)

	isMember := set.SIsMember(setKey, []byte("Python"))
	fmt.Println(isMember)
}
