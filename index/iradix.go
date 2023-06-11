package index

import (
	"sync"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"github.com/rosedblabs/wal"
)

// IRadixTree is a wrapper of immutable radix tree of hashicorp.
// See more details: github.com/hashicorp/go-immutable-radix.
type IRadixTree struct {
	tree *iradix.Tree[*wal.ChunkPosition]
	lock sync.Mutex
}

func newRadixTree() *IRadixTree {
	return &IRadixTree{
		tree: iradix.New[*wal.ChunkPosition](),
	}
}

func (irx *IRadixTree) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	irx.lock.Lock()
	defer irx.lock.Unlock()
	var oldPos *wal.ChunkPosition
	irx.tree, oldPos, _ = irx.tree.Insert(key, position)
	return oldPos
}

func (irx *IRadixTree) Get(key []byte) *wal.ChunkPosition {
	pos, _ := irx.tree.Get(key)
	return pos
}

func (irx *IRadixTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	irx.lock.Lock()
	defer irx.lock.Unlock()
	var oldPos *wal.ChunkPosition
	var ok bool
	irx.tree, oldPos, ok = irx.tree.Delete(key)
	return oldPos, ok
}

func (irx *IRadixTree) Size() int {
	return irx.tree.Len()
}
