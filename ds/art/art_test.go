package art

import (
	"reflect"
	"testing"
)

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
