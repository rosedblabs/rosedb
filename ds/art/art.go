package art

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldVal interface{}, updated bool) {
	return art.tree.Insert(key, value)
}

func (art *AdaptiveRadixTree) Get(key []byte) interface{} {
	value, _ := art.tree.Search(key)
	return value
}

func (art *AdaptiveRadixTree) Delete(key []byte) (val interface{}, updated bool) {
	return art.tree.Delete(key)
}

func (art *AdaptiveRadixTree) Iterator() goart.Iterator {
	return art.tree.Iterator()
}

func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}
