package index

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	queue "github.com/Jcowwell/go-algorithm-club/PriorityQueue"
	"github.com/drewlanenga/govector"
	"github.com/google/btree"
	"github.com/rosedblabs/wal"
)

type vectorItem struct {
	key govector.Vector
	pos *wal.ChunkPosition
}

type graphNode struct {
	item vectorItem
}

type VectorIndex struct {
	graph           map[uint32]map[uint32]struct{}
	graphNodeMap    map[uint32]graphNode
	currGraphNodeId uint32
	m               uint32
	maxM            uint32
	interval        uint32 // decide which node will be entry point

	btreeIndex *MemoryBTree
	mu         sync.RWMutex

	entryNode []uint32

	// from the original index.go
	tree *btree.BTree
	lock *sync.RWMutex
}

type priorityQueueItem struct {
	distance float64
	nodeId   uint32
}

func newVectorIndex(m uint32, maxM uint32, interval uint32) *VectorIndex {
	return &VectorIndex{
		currGraphNodeId: 0,
		m:               m,
		maxM:            maxM,
		interval:        interval,
		btreeIndex:      newBTree(),
		graphNodeMap:    make(map[uint32]graphNode),
		graph:           make(map[uint32]map[uint32]struct{}),
		entryNode:       make([]uint32, 0),
	}
}

func minPriorityQueue(left priorityQueueItem, right priorityQueueItem) bool {
	return left.distance < right.distance
}

func maxPriorityQueue(left priorityQueueItem, right priorityQueueItem) bool {
	return left.distance > right.distance
}

func distance(v1 govector.Vector, v2 govector.Vector) (float64, error) {
	diff, err := v1.Subtract(v2)

	if err != nil {
		return 0, err
	}

	return govector.Norm(diff, 2), nil
}

func encodeVector(v govector.Vector) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(v)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return buffer.Bytes()
}

func decodeVector(data []byte) govector.Vector {
	var vector govector.Vector
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&vector)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return vector
}

func (vi *VectorIndex) getNodeIdsByKey(key govector.Vector, num uint32) ([]uint32, error) {
	if len(vi.entryNode) == 0 {
		return []uint32{}, nil
	}
	candidateQueue := queue.PriorityQueueInit(minPriorityQueue)
	resultQueue := queue.PriorityQueueInit(maxPriorityQueue)
	visited := make(map[uint32]struct{})

	// initialize all with entry points
	for _, e := range vi.entryNode {
		d, err := distance(key, vi.graphNodeMap[e].item.key)
		if err != nil {
			return nil, err
		}
		candidateQueue.Enqueue(priorityQueueItem{distance: d, nodeId: e})

		resultQueue.Enqueue(priorityQueueItem{distance: d, nodeId: e})
		// keep result queue the same number as parameter num
		if uint32(resultQueue.Count()) > num {
			resultQueue.Dequeue()
		}

		visited[e] = struct{}{}
	}
	for candidateQueue.Count() != 0 {
		currNode, _ := candidateQueue.Dequeue()
		furthestNode, _ := resultQueue.Peek()

		// we assume there will be no node close to the target
		if currNode.distance > furthestNode.distance && uint32(resultQueue.Count()) == num {
			break
		}

		// exploring the neighbor
		for neighbor_id := range vi.graph[currNode.nodeId] {
			d, err := distance(key, vi.graphNodeMap[neighbor_id].item.key)
			if err != nil {
				return nil, err
			}

			// if we didnt visit this neighbor before, put into candidate queue.
			if _, exists := visited[neighbor_id]; !exists {
				candidateQueue.Enqueue(priorityQueueItem{distance: d, nodeId: neighbor_id})

				resultQueue.Enqueue(priorityQueueItem{distance: d, nodeId: neighbor_id})
				// keep result queue the same number as parameter num
				if uint32(resultQueue.Count()) > num {
					resultQueue.Dequeue()
				}

				visited[neighbor_id] = struct{}{}
			}
		}
	}
	res := make([]uint32, 0)
	for resultQueue.Count() > 0 {
		n, _ := resultQueue.Dequeue()
		res = append(res, n.nodeId)
	}
	return res, nil
}

func (vi *VectorIndex) GetVector(key govector.Vector, num uint32) ([]govector.Vector, error) {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	nodeIdList, err := vi.getNodeIdsByKey(key, num)
	if err != nil {
		// TODO: update position if needed
		return nil, err, position
	}

	if len(vi.entryNode) == 0 {
		// TODO: update position if needed
		return []govector.Vector{}, nil, position
	}
	res := make([]govector.Vector, 0)

	for _, nodeId := range nodeIdList {
		res = append(res, vi.graphNodeMap[nodeId].item.key)
	}
	// TODO: update position if needed
	return res, nil, position
}

func (vi *VectorIndex) AddEdge(inNode uint32, outNode uint32) {
	if _, ok := vi.graph[inNode]; !ok {
		vi.graph[inNode] = make(map[uint32]struct{})
	}
	vi.graph[inNode][outNode] = struct{}{}
	if _, ok := vi.graph[outNode]; !ok {
		vi.graph[outNode] = make(map[uint32]struct{})
	}
	vi.graph[outNode][inNode] = struct{}{}
}

func (vi *VectorIndex) DeleteEdge(inNode uint32, outNode uint32) {
	if inner, ok := vi.graph[inNode]; ok {
		if _, ok := inner[outNode]; ok {
			delete(inner, outNode)
		}
	}
	if inner, ok := vi.graph[outNode]; ok {
		if _, ok := inner[inNode]; ok {
			delete(inner, inNode)
		}
	}
}

func (vi *VectorIndex) PutVector(key govector.Vector, position *wal.ChunkPosition) (bool, error, *wal.ChunkPosition) {
	// TODO: check uniqueness in B-tree Index
	bTreeKey := encodeVector(key)
	existKey := vi.btreeIndex.Get(bTreeKey)
	if existKey != nil {
		// TODO: update position if needed
		return true, nil, position
	}
	// insert key into b-tree
	vi.btreeIndex.Put(bTreeKey, position)

	vi.mu.Lock()
	defer vi.mu.Unlock()

	newNodeId := vi.currGraphNodeId
	vi.currGraphNodeId++

	// find m closest nodes
	nodeIdList, e := vi.getNodeIdsByKey(key, vi.m)
	if e != nil {
		// TODO: update position if needed
		return false, e, position
	}
	if newNodeId%vi.interval == 0 {
		vi.entryNode = append(vi.entryNode, newNodeId)
	}

	// add node to entry node every #interval times of put

	graphNode := graphNode{item: vectorItem{
		key: key,
		pos: position,
	}}
	vi.graphNodeMap[newNodeId] = graphNode
	for _, nodeId := range nodeIdList {
		vi.AddEdge(newNodeId, nodeId)
		if uint32(len(vi.graph[nodeId])) > vi.maxM {
			// delete edges if nodeId has more than max_m edges, just find the node with farest distance
			maxDistance := float64(0)
			var deleteNode uint32
			nodeVector := vi.graphNodeMap[nodeId].item.key
			for dNode := range vi.graph[nodeId] {
				dis, err := distance(nodeVector, vi.graphNodeMap[dNode].item.key)
				if err != nil {
					// TODO: update position if needed
					return false, err, position
				}
				if dis > maxDistance {
					maxDistance = dis
					deleteNode = dNode
				}
			}
			vi.DeleteEdge(nodeId, deleteNode)
		}
	}
	// TODO: update position if needed
	return true, nil, position
}

func (vi *VectorIndex) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	govec, err := vi.BytesToVector(key)
	if err != nil {
		return position
	}
	_, err, put_position := vi.PutVector(govec, position)
	if err != nil {
		return put_position // TODO: check
	}
	return put_position
}

func (vi *VectorIndex) Get(key []byte) *wal.ChunkPosition {
	govec, err := vi.BytesToVector(key)
	if err != nil {
		return nil
	}
	_, err, position := vi.GetVector(govec, 1)
	if err != nil {
		return nil
	}
	return position
}

// from index/index.go

func (vi *VectorIndex) Delete(key []byte) (*wal.ChunkPosition, bool) {
	vi.lock.Lock()
	defer vi.lock.Unlock()

	value := vi.tree.Delete(&item{key: key})
	if value != nil {
		return value.(*item).pos, true
	}
	return nil, false
}

func (vi *VectorIndex) Size() int {
	return vi.tree.Len()
}

func (vi *VectorIndex) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.lock.RLock()
	defer vi.lock.RUnlock()

	vi.tree.Ascend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (vi *VectorIndex) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.lock.RLock()
	defer vi.lock.RUnlock()

	vi.tree.Descend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (vi *VectorIndex) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.lock.RLock()
	defer vi.lock.RUnlock()

	vi.tree.AscendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (vi *VectorIndex) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.lock.RLock()
	defer vi.lock.RUnlock()

	vi.tree.DescendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (vi *VectorIndex) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.lock.RLock()
	defer vi.lock.RUnlock()

	vi.tree.AscendGreaterOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (vi *VectorIndex) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.lock.RLock()
	defer vi.lock.RUnlock()

	vi.tree.DescendLessOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}
