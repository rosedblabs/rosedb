package bptree

import (
	"github.com/rosedblabs/wal"
)

// Iterator represents a B+Tree iterator.
type Iterator struct {
	tree    *BPlusTree
	current *Node
	index   int
	reverse bool
	valid   bool
}

// NewIterator creates a new iterator.
func (t *BPlusTree) Iterator(reverse bool) *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()

	it := &Iterator{
		tree:    t,
		reverse: reverse,
	}

	it.rewindLocked()
	return it
}

// Rewind resets the iterator to the beginning.
func (it *Iterator) Rewind() {
	it.tree.mu.RLock()
	defer it.tree.mu.RUnlock()
	it.rewindLocked()
}

func (it *Iterator) rewindLocked() {
	if it.tree.meta.KeyCount == 0 {
		it.valid = false
		return
	}

	if it.reverse {
		// Find rightmost leaf
		node, err := it.tree.getNode(it.tree.meta.RootPageID)
		if err != nil {
			it.valid = false
			return
		}

		for !node.IsLeaf() {
			node, err = it.tree.getNode(node.Children[node.KeyCount])
			if err != nil {
				it.valid = false
				return
			}
		}

		it.current = node
		it.index = int(node.KeyCount) - 1
		it.valid = it.index >= 0
	} else {
		// Find leftmost leaf
		node, err := it.tree.getNode(it.tree.meta.RootPageID)
		if err != nil {
			it.valid = false
			return
		}

		for !node.IsLeaf() {
			node, err = it.tree.getNode(node.Children[0])
			if err != nil {
				it.valid = false
				return
			}
		}

		it.current = node
		it.index = 0
		it.valid = node.KeyCount > 0
	}
}

// Seek positions the iterator at the first key >= given key.
func (it *Iterator) Seek(key []byte) {
	it.tree.mu.RLock()
	defer it.tree.mu.RUnlock()

	leaf, err := it.tree.findLeaf(key)
	if err != nil {
		it.valid = false
		return
	}

	idx, found := it.tree.searchInNode(leaf, key)
	it.current = leaf

	if it.reverse {
		if found {
			it.index = idx
		} else if idx > 0 {
			it.index = idx - 1
		} else {
			// Need to go to previous leaf
			if leaf.Prev != InvalidPageID {
				prevLeaf, err := it.tree.getNode(leaf.Prev)
				if err != nil {
					it.valid = false
					return
				}
				it.current = prevLeaf
				it.index = int(prevLeaf.KeyCount) - 1
			} else {
				it.valid = false
				return
			}
		}
	} else {
		it.index = idx
		if idx >= int(leaf.KeyCount) {
			// Need to go to next leaf
			if leaf.Next != InvalidPageID {
				nextLeaf, err := it.tree.getNode(leaf.Next)
				if err != nil {
					it.valid = false
					return
				}
				it.current = nextLeaf
				it.index = 0
			} else {
				it.valid = false
				return
			}
		}
	}

	it.valid = it.current != nil && it.index >= 0 && it.index < int(it.current.KeyCount)
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}

	it.tree.mu.RLock()
	defer it.tree.mu.RUnlock()

	if it.reverse {
		it.index--
		if it.index < 0 {
			// Move to previous leaf
			if it.current.Prev != InvalidPageID {
				prevLeaf, err := it.tree.getNode(it.current.Prev)
				if err != nil {
					it.valid = false
					return
				}
				it.current = prevLeaf
				it.index = int(prevLeaf.KeyCount) - 1
			} else {
				it.valid = false
			}
		}
	} else {
		it.index++
		if it.index >= int(it.current.KeyCount) {
			// Move to next leaf
			if it.current.Next != InvalidPageID {
				nextLeaf, err := it.tree.getNode(it.current.Next)
				if err != nil {
					it.valid = false
					return
				}
				it.current = nextLeaf
				it.index = 0
				if it.current.KeyCount == 0 {
					it.valid = false
				}
			} else {
				it.valid = false
			}
		}
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	it.tree.mu.RLock()
	defer it.tree.mu.RUnlock()
	return it.current.Keys[it.index]
}

// Value returns the current value.
func (it *Iterator) Value() *wal.ChunkPosition {
	if !it.valid {
		return nil
	}
	it.tree.mu.RLock()
	defer it.tree.mu.RUnlock()
	return it.current.Values[it.index]
}

// Close releases the iterator resources.
func (it *Iterator) Close() {
	it.current = nil
	it.valid = false
}
