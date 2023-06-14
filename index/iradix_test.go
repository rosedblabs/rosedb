package index

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

func TestIRadixTree_Put(t *testing.T) {
	tree := newRadixTree()
	type args struct {
		key      []byte
		position *wal.ChunkPosition
	}
	tests := []struct {
		name string
		tree *IRadixTree
		args args
		want *wal.ChunkPosition
	}{
		{
			"empty-key", tree, args{key: nil, position: nil}, nil,
		},
		{
			"empty-value", tree, args{key: utils.GetTestKey(1), position: nil}, nil,
		},
		{
			"valid-key-value", tree, args{key: utils.GetTestKey(1), position: &wal.ChunkPosition{SegmentId: 1, BlockNumber: 1, ChunkOffset: 100}}, nil,
		},
		{
			// do not run this test individually, because it will fail.
			"duplicated-key", tree, args{key: utils.GetTestKey(1), position: &wal.ChunkPosition{SegmentId: 2, BlockNumber: 2, ChunkOffset: 200}},
			&wal.ChunkPosition{SegmentId: 1, BlockNumber: 1, ChunkOffset: 100},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tree.Put(tt.args.key, tt.args.position); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Put() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIRadixTree_Get(t *testing.T) {
	tree := newRadixTree()
	tree.Put(utils.GetTestKey(1), &wal.ChunkPosition{BlockNumber: 1, ChunkOffset: 100})
	tree.Put(utils.GetTestKey(1), &wal.ChunkPosition{BlockNumber: 3, ChunkOffset: 300})
	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		tree *IRadixTree
		args args
		want *wal.ChunkPosition
	}{
		{
			"not-exist", tree, args{key: utils.GetTestKey(10000)}, nil,
		},
		{
			"exist-val", tree, args{key: utils.GetTestKey(1)}, &wal.ChunkPosition{BlockNumber: 3, ChunkOffset: 300},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tree.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIRadixTree_Delete(t *testing.T) {
	tree := newRadixTree()
	tree.Put(utils.GetTestKey(1), &wal.ChunkPosition{BlockNumber: 1, ChunkOffset: 100})
	tree.Put(utils.GetTestKey(1), &wal.ChunkPosition{BlockNumber: 3, ChunkOffset: 300})
	type args struct {
		key []byte
	}
	tests := []struct {
		name  string
		tree  *IRadixTree
		args  args
		want  *wal.ChunkPosition
		want1 bool
	}{
		{
			"not-exist", tree, args{key: utils.GetTestKey(6000)}, nil, false,
		},
		{
			"exist", tree, args{key: utils.GetTestKey(1)}, &wal.ChunkPosition{BlockNumber: 3, ChunkOffset: 300}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.tree.Delete(tt.args.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Delete() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestIRadixTree_Iterator_Normal(t *testing.T) {
	tree := newRadixTree()
	options := IteratorOptions{Prefix: nil, Reverse: false}

	// empty tree
	iter1 := tree.Iterator(options)
	defer iter1.Close()
	assert.False(t, iter1.Valid())

	// tree with one node
	tree.Put(utils.GetTestKey(1), &wal.ChunkPosition{BlockNumber: 1, ChunkOffset: 100})
	iter2 := tree.Iterator(options)
	defer iter2.Close()
	assert.True(t, iter2.Valid())
	iter2.Next()
	assert.False(t, iter2.Valid())

	testIRadixTreeIterator(t, options, 1000)

	// reverse
	options.Reverse = true
	testIRadixTreeIterator(t, options, 1000)
}

func TestIRadixTreeIterator_Prefix(t *testing.T) {
	tree := newRadixTree()

	keys := [][]byte{
		[]byte("ccade"),
		[]byte("aaame"),
		[]byte("aujea"),
		[]byte("ccnea"),
		[]byte("bbeda"),
		[]byte("kkimq"),
		[]byte("neusa"),
		[]byte("mjiue"),
		[]byte("kjuea"),
		[]byte("rnhse"),
		[]byte("mjiqe"),
	}
	for _, key := range keys {
		tree.Put(key, &wal.ChunkPosition{BlockNumber: 1, ChunkOffset: 100})
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	iter := tree.tree.Root().Iterator()
	iter.SeekPrefix([]byte("au"))
	iter.SeekLowerBound([]byte("auj"))
	key, _, ok := iter.Next()
	t.Log(key, ok)

	//optiosn := IteratorOptions{Reverse: false, Prefix: []byte("mm")}
	//tree.Iterator(optiosn)
}

func testIRadixTreeIterator(t *testing.T, options IteratorOptions, size int) {
	tree := newRadixTree()
	var keys [][]byte
	for i := 0; i < size; i++ {
		key := utils.RandomValue(10)
		keys = append(keys, key)
		tree.Put(key, &wal.ChunkPosition{BlockNumber: 1, ChunkOffset: 100})
	}

	sort.Slice(keys, func(i, j int) bool {
		if options.Reverse {
			return bytes.Compare(keys[i], keys[j]) > 0
		} else {
			return bytes.Compare(keys[i], keys[j]) < 0
		}
	})

	var i = 0
	iter3 := tree.Iterator(options)
	defer iter3.Close()
	for ; iter3.Valid(); iter3.Next() {
		assert.Equal(t, keys[i], iter3.Key())
		i++
	}
	assert.Equal(t, i, size)
}
