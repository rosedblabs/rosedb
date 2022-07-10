package art

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"testing"
)

func TestAdaptiveRadixTree_PrefixScan(t *testing.T) {
	art := NewART()
	art.Put([]byte("acse"), 123)
	art.Put([]byte("cced"), 123)
	art.Put([]byte("acde"), 123)
	art.Put([]byte("bbfe"), 123)
	art.Put([]byte("bbfc"), 123)
	art.Put([]byte("eefs"), 123)

	keys1 := art.PrefixScan([]byte("bbf"), -1)
	assert.Equal(t, 0, len(keys1))

	keys2 := art.PrefixScan(nil, 0)
	assert.Equal(t, 0, len(keys2))

	keys3 := art.PrefixScan([]byte("b"), 1)
	assert.Equal(t, 1, len(keys3))

	keys4 := art.PrefixScan(nil, 6)
	assert.Equal(t, 6, len(keys4))
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
	tree := NewART()
	type args struct {
		key   []byte
		value interface{}
	}
	tests := []struct {
		name        string
		art         *AdaptiveRadixTree
		args        args
		wantOldVal  interface{}
		wantUpdated bool
	}{
		{
			"nil", tree, args{key: nil, value: nil}, nil, false,
		},
		{
			"normal-1", tree, args{key: []byte("1"), value: 11}, nil, false,
		},
		{
			// must run after previous one.
			"normal-2", tree, args{key: []byte("1"), value: 22}, 11, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOldVal, gotUpdated := tt.art.Put(tt.args.key, tt.args.value)
			if !reflect.DeepEqual(gotOldVal, tt.wantOldVal) {
				t.Errorf("Put() gotOldVal = %v, want %v", gotOldVal, tt.wantOldVal)
			}
			if gotUpdated != tt.wantUpdated {
				t.Errorf("Put() gotUpdated = %v, want %v", gotUpdated, tt.wantUpdated)
			}
		})
	}
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	tree := NewART()
	tree.Put(nil, nil)
	tree.Put([]byte("0"), 0)
	tree.Put([]byte("11"), 11)
	tree.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		tree *AdaptiveRadixTree
		args args
		want interface{}
	}{
		{

			"nil", tree, args{key: nil}, nil,
		},
		{
			"zero", tree, args{key: []byte("0")}, 0,
		},
		{
			"rewrite-data", tree, args{key: []byte("11")}, "rewrite-data",
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

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	tree := NewART()
	tree.Put(nil, nil)
	tree.Put([]byte("0"), 0)
	tree.Put([]byte("11"), 11)
	tree.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}
	tests := []struct {
		name        string
		tree        *AdaptiveRadixTree
		args        args
		wantVal     interface{}
		wantUpdated bool
	}{
		{
			"nil", tree, args{key: nil}, nil, false,
		},
		{
			"zero", tree, args{key: []byte("0")}, 0, true,
		},
		{
			"rewrite-data", tree, args{key: []byte("11")}, "rewrite-data", true,
		},
		{
			"not-exist", tree, args{key: []byte("not-exist")}, nil, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotUpdated := tt.tree.Delete(tt.args.key)
			if !reflect.DeepEqual(gotVal, tt.wantVal) {
				t.Errorf("Delete() gotVal = %v, want %v", gotVal, tt.wantVal)
			}
			if gotUpdated != tt.wantUpdated {
				t.Errorf("Delete() gotUpdated = %v, want %v", gotUpdated, tt.wantUpdated)
			}
		})
	}
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	iter1 := art.Iterator()
	assert.False(t, iter1.HasNext())

	var keys = [][]byte{[]byte("acse"), []byte("cced"), []byte("acde"), []byte("bbfe")}
	for i, key := range keys {
		art.Put(key, i)
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	var targes [][]byte
	iter2 := art.Iterator()
	for iter2.HasNext() {
		node, err := iter2.Next()
		assert.Nil(t, err)
		targes = append(targes, node.Key())
	}
	assert.Equal(t, keys, targes)
}
