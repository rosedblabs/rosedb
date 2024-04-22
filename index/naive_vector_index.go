package index

import (
	"sync"

	queue "github.com/Jcowwell/go-algorithm-club/PriorityQueue"
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
	distance float32
	idx      uint32
}

type vectorPair struct {
	vector RoseVector
	chunk *wal.ChunkPosition
}

func minPQ(left PQItem, right PQItem) bool {
	return left.distance < right.distance
}

func (nvi *NaiveVectorIndex) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	return nvi.btreeIndex.Put(key, &ChunkPositionWrapper{pos: position, deleted: false}).pos
}

func (nvi *NaiveVectorIndex) PutVector(key RoseVector, position *wal.ChunkPosition) (bool, error) {
	nvi.btreeIndex.Put(EncodeVector(key), &ChunkPositionWrapper{pos: position, deleted: false})
	return true, nil
}

func (nvi *NaiveVectorIndex) Get(key []byte) *wal.ChunkPosition {
	return nvi.btreeIndex.Get(key).pos
}

func (nvi *NaiveVectorIndex) getVectorInternal(key RoseVector, num uint32) ([]vectorPair, error) {
	nvi.mu.RLock()
	defer nvi.mu.RUnlock()

	// iterate over btree to get all the keys (vectors) in the database
	vectors := make([]vectorPair, 0)
	handleFn := func(key []byte, position *wal.ChunkPosition) (bool, error) {
		vec := DecodeVector(key)
		vectors = append(vectors, vectorPair{vector: vec, chunk: position})
		return true, nil
	}
	nvi.Ascend(handleFn)

	// calculate distances between the given vector and other vectors in the databse
	distances := make([]float32, 0)
	for _, vector := range vectors {
		dis := distance(key, vector.vector)
		distances = append(distances, dis)
	}

	// get the nearest num vectors
	pq := queue.PriorityQueueInit(minPQ)
	for i, dis := range distances {
		pq.Enqueue(PQItem{distance: dis, idx: uint32(i)})
	}
	res := make([]vectorPair, 0)
	for i := 0; i < int(num); i++ {
		item, success := pq.Dequeue()
		if success {
			res = append(res, vectors[item.idx])
			// fmt.Println(vectors[item.idx])
		}
	}
	return res, nil
}

func (nvi *NaiveVectorIndex) GetVectorTest(key RoseVector, num uint32) ([]RoseVector, error) {
	res, err := nvi.getVectorInternal(key, num)

	if err != nil {
		return nil, err
	}

	vectorList := make([]RoseVector, 0)

	for _, vpair := range res {
		vectorList = append(vectorList, vpair.vector)
	}
	return vectorList, nil
}

func (nvi *NaiveVectorIndex) GetVector(key RoseVector, num uint32) ([]*wal.ChunkPosition, error) {
	res, err := nvi.getVectorInternal(key, num)

	if err != nil {
		return nil, err
	}

	chunkList := make([]*wal.ChunkPosition, 0)

	for _, vpair := range res {
		chunkList = append(chunkList, vpair.chunk)
	}
	return chunkList, nil
}

func (nvi *NaiveVectorIndex) Delete(key []byte) (*wal.ChunkPosition, bool) {
	wrapper, deleted := nvi.btreeIndex.Delete(key)
	return wrapper.pos, deleted
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
