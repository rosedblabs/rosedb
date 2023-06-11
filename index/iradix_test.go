package index

import (
	"reflect"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
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
