package index

import (
	"fmt"
	"sync"

	queue "github.com/Jcowwell/go-algorithm-club/PriorityQueue"
	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
)

type NaiveVectorIndex struct {
	btreeIndex *MemoryBTree
	mu         sync.RWMutex
}

func newNaiveVectorIndex() *NaiveVectorIndex {
	return &NaiveVectorIndex{
		btreeIndex: newBTree(),
	}
}

type naivePQItem struct {
	distance float64
	idx      uint32
}

func naiveMinPQ(left naivePQItem, right naivePQItem) bool {
	return left.distance < right.distance
}

func (nvi *NaiveVectorIndex) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	// call btreeIndex's Put method
	return nvi.btreeIndex.Put(key, position)
}

func (nvi *NaiveVectorIndex) PutVector(key govector.Vector, position *wal.ChunkPosition) (bool, error) {
	position = nvi.Put(EncodeVector(key), position)
	if position == nil {
		return false, fmt.Errorf("put failed")
	}
	return true, nil
}

func (nvi *NaiveVectorIndex) Get(key []byte) *wal.ChunkPosition {
	return nvi.btreeIndex.Get(key)
}

func (nvi *NaiveVectorIndex) GetVector(key govector.Vector, num uint32) ([]govector.Vector, error) {
	nvi.mu.RLock()
	defer nvi.mu.RUnlock()

	// iterate over btree to get all the keys (vectors) in the database
	vectors := make([]govector.Vector, 0)
	handleFn := func(key []byte, position *wal.ChunkPosition) (bool, error) {
		vec := decodeVector(key)
		vectors = append(vectors, vec)
		return true, nil
	}
	nvi.Ascend(handleFn)

	// print each element of the vector
	for i := 0; i < len(vectors); i++ {
		fmt.Println(vectors[i])
	}

	// calculate distances between the given vector and other vectors in the databse
	distances := make([]float64, 0)
	for _, vector := range vectors {
		dis, err := distance(key, vector)
		if err != nil {
			return nil, err
		}
		distances = append(distances, dis)
	}

	// get the nearest n vectors
	pq := queue.PriorityQueueInit(naiveMinPQ)
	for i, dis := range distances {
		pq.Enqueue(naivePQItem{distance: dis, idx: uint32(i)})
	}
	res := make([]govector.Vector, 0)
	for i := 0; i < int(num); i++ {
		item, success := pq.Dequeue()
		if success {
			res = append(res, vectors[item.idx])
			fmt.Println(vectors[item.idx])
		}
	}
	return res, nil
}

func (nvi *NaiveVectorIndex) Delete(key []byte) (*wal.ChunkPosition, bool) {
	fmt.Println("vector index's Delete method is being called")
	return nvi.btreeIndex.Delete(key)
}

func (nvi *NaiveVectorIndex) Size() int {
	fmt.Println("vector index's Size method is being called")
	return nvi.btreeIndex.Size()
}

func (nvi *NaiveVectorIndex) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	nvi.btreeIndex.Ascend(handleFn)
}

func (nvi *NaiveVectorIndex) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	nvi.btreeIndex.Descend(handleFn)
}

func (nvi *NaiveVectorIndex) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	nvi.btreeIndex.AscendRange(startKey, endKey, handleFn)
}

func (nvi *NaiveVectorIndex) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	nvi.btreeIndex.DescendRange(startKey, endKey, handleFn)
}

func (nvi *NaiveVectorIndex) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	nvi.btreeIndex.AscendGreaterOrEqual(key, handleFn)
}

func (nvi *NaiveVectorIndex) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	nvi.btreeIndex.DescendLessOrEqual(key, handleFn)
}
