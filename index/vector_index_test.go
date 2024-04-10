package index

import (
	"fmt"
	"testing"

	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
)

func TestVectorIndex_Put_Get(t *testing.T) {
	vi := newVectorIndex(3, 5, 5)
	w, _ := wal.Open(wal.DefaultOptions)

	var vectorArr = []govector.Vector{{8, -7, -10, -8, 3, -6, 6, -2, 5, 1},
		{-2, -2, -6, -10, 10, -3, 1, 3, -9, -10},
		{-4, 7, -6, -1, 3, -5, 5, -2, -10, -3},
		{1, 0, -7, 1, 3, -3, 1, 0, -2, 7},
		{-3, -7, -6, -3, 5, 3, 1, 1, -6, 6},
		{9, 0, 8, -3, -4, 1, -3, -9, -10, 4},
		{8, -5, -7, 4, -10, 0, -7, 4, 10, 0},
		{-2, -10, -7, -1, -10, -4, 1, 2, -3, 3},
		{-1, -7, 6, 2, -2, -2, -2, -1, -2, -10},
		{9, -2, -1, -1, -6, 9, 2, 3, -7, 5},
	}

	for _, vector := range vectorArr {
		key := encodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.Put(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet, err := vi.Get(vectorArr[3], 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}

func TestVectorIndex_Simple_Put_Get(t *testing.T) {
	vi := newVectorIndex(3, 5, 5)
	w, _ := wal.Open(wal.DefaultOptions)

	var vectorArr = []govector.Vector{{1,2},
	{4,8},
	{4,9},
	{8,10},
	{10,12},
	{10,6},
	{15,3},
	{5,4},
	{6,7},
	{8,3},
	{2,9},
	{12,5},
	{14,2},
	}

	for _, vector := range vectorArr {
		key := encodeVector(vector)
		chunkPosition, _ := w.Write(key)
		_, err := vi.Put(vector, chunkPosition)
		if err != nil {
			t.Fatalf("put failed: %v", err.Error())
		}
	}

	resSet, err := vi.Get(govector.Vector{8, 7}, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, resVec := range resSet {
		fmt.Println(resVec)
	}
}
