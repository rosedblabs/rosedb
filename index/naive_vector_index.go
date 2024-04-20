package index

import (
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

type PQItem struct {
	distance float64
	idx      uint32
}

func minPQ(left PQItem, right PQItem) bool {
	return left.distance < right.distance
}

func (nvi *NaiveVectorIndex) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	return nvi.btreeIndex.Put(key, position)
}

func (nvi *NaiveVectorIndex) PutVector(key govector.Vector, position *wal.ChunkPosition) (bool, error) {
	nvi.btreeIndex.Put(EncodeVector(key), position)
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

	// calculate distances between the given vector and other vectors in the databse
	distances := make([]float64, 0)
	for _, vector := range vectors {
		dis, err := distance(key, vector)
		if err != nil {
			return nil, err
		}
		distances = append(distances, dis)
	}

	// get the nearest num vectors
	pq := queue.PriorityQueueInit(minPQ)
	for i, dis := range distances {
		pq.Enqueue(PQItem{distance: dis, idx: uint32(i)})
	}
	res := make([]govector.Vector, 0)
	for i := 0; i < int(num); i++ {
		item, success := pq.Dequeue()
		if success {
			res = append(res, vectors[item.idx])
			// fmt.Println(vectors[item.idx])
		}
	}
	return res, nil
}

func (nvi *NaiveVectorIndex) Delete(key []byte) (*wal.ChunkPosition, bool) {
	return nvi.btreeIndex.Delete(key)
}

func (nvi *NaiveVectorIndex) Size() int {
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
