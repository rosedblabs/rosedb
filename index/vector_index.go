package index

import (
	"errors"
	"sync"
	"sync/atomic"

	queue "github.com/Jcowwell/go-algorithm-club/PriorityQueue"
	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
)

var (
	ErrVec = errors.New("the input is not a vector")
)

var (
	kReconstructPercent = 0.1
)

type vectorItem struct {
	key govector.Vector
	pos *ChunkPositionWrapper
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

	toDelete atomic.Uint32
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

		if !vi.graphNodeMap[e].item.pos.deleted {
			resultQueue.Enqueue(priorityQueueItem{distance: d, nodeId: e})
			// keep result queue the same number as parameter num
			if uint32(resultQueue.Count()) > num {
				resultQueue.Dequeue()
			}
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

				if !vi.graphNodeMap[neighbor_id].item.pos.deleted {
					resultQueue.Enqueue(priorityQueueItem{distance: d, nodeId: neighbor_id})
					// keep result queue the same number as parameter num
					if uint32(resultQueue.Count()) > num {
						resultQueue.Dequeue()
					}
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

func (vi *VectorIndex) reconstructGraph() {
	vi.mu.Lock()

	defer vi.mu.Unlock()

	newEntryNode := []uint32{}

	for _, node := range vi.entryNode {
		if !vi.graphNodeMap[node].item.pos.deleted {
			newEntryNode = append(newEntryNode, node)
		}
	}

	vi.entryNode = newEntryNode

	toBeDeleted := []uint32{}
	for node, neighbor := range vi.graph {
		if !vi.graphNodeMap[node].item.pos.deleted {
			continue
		}

		for n := range neighbor {
			vi.deleteEdge(node, n)
		}

		toBeDeleted = append(toBeDeleted, node)
	}

	for _, node := range toBeDeleted {
		delete(vi.graph, node)
		delete(vi.graphNodeMap, node)
	}

	vi.toDelete.Store(0)

}

func (vi *VectorIndex) addEdge(inNode uint32, outNode uint32) {
	if _, ok := vi.graph[inNode]; !ok {
		vi.graph[inNode] = make(map[uint32]struct{})
	}
	vi.graph[inNode][outNode] = struct{}{}
	if _, ok := vi.graph[outNode]; !ok {
		vi.graph[outNode] = make(map[uint32]struct{})
	}
	vi.graph[outNode][inNode] = struct{}{}
}

func (vi *VectorIndex) deleteEdge(inNode uint32, outNode uint32) {
	if inner, ok := vi.graph[inNode]; ok {
		delete(inner, outNode)
	}
	if inner, ok := vi.graph[outNode]; ok {
		delete(inner, inNode)
	}
}

func (vi *VectorIndex) putVector(key govector.Vector, chunkWrapper *ChunkPositionWrapper) (bool, error) {
	vi.mu.Lock()
	defer vi.mu.Unlock()

	newNodeId := vi.currGraphNodeId
	vi.currGraphNodeId++

	// find m closest nodes
	nodeIdList, e := vi.getNodeIdsByKey(key, vi.m)
	if e != nil {
		return false, e
	}
	if newNodeId%vi.interval == 0 {
		vi.entryNode = append(vi.entryNode, newNodeId)
	}

	// add node to entry node every #interval times of put
	graphNode := graphNode{item: vectorItem{
		key: key,
		pos: chunkWrapper,
	}}
	vi.graphNodeMap[newNodeId] = graphNode
	for _, nodeId := range nodeIdList {
		vi.addEdge(newNodeId, nodeId)
		if uint32(len(vi.graph[nodeId])) > vi.maxM {
			// delete edges if nodeId has more than max_m edges, just find the node with farest distance
			maxDistance := float64(0)
			var deleteNode uint32
			nodeVector := vi.graphNodeMap[nodeId].item.key
			for dNode := range vi.graph[nodeId] {
				dis, err := distance(nodeVector, vi.graphNodeMap[dNode].item.key)
				if err != nil {
					return false, err
				}
				if dis > maxDistance {
					maxDistance = dis
					deleteNode = dNode
				}
			}
			vi.deleteEdge(nodeId, deleteNode)
		}
	}
	return true, nil
}

func (vi *VectorIndex) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {

	// insert key into b-tree
	var wrapper *ChunkPositionWrapper
	var exists bool

	wrapper = vi.btreeIndex.Get(key)

	if wrapper == nil {
		wrapper = &ChunkPositionWrapper{pos: position, deleted: false}
		exists = false
	} else {
		wrapper.pos = position
		exists = true
	}

	resWrapper := vi.btreeIndex.Put(key, wrapper)

	if exists {
		// vector already exists in the graph
		return resWrapper.pos
	}
	//convert byte array to govector
	govec := DecodeVector(key)
	if govec == nil {
		return nil
	}

	// store vector and get position by calling btree's Put method
	_, put_err := vi.putVector(govec, wrapper)

	if put_err != nil {
		return nil
	}

	if resWrapper == nil {
		return nil
	}
	return resWrapper.pos
}

// Testing purpose only
func (vi *VectorIndex) GetVectorTest(keyVec govector.Vector, num uint32) ([]govector.Vector, error) {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	nodeIdList, err := vi.getNodeIdsByKey(keyVec, num)
	if err != nil {
		return nil, err
	}

	res := make([]govector.Vector, 0)

	for _, nodeId := range nodeIdList {
		res = append(res, vi.graphNodeMap[nodeId].item.key)
	}

	return res, nil
}

func (vi *VectorIndex) GetVector(key govector.Vector, num uint32) ([]*wal.ChunkPosition, error) {
	if key == nil {
		return nil, ErrVec
	}

	vi.mu.RLock()
	defer vi.mu.RUnlock()
	nodeIdList, err := vi.getNodeIdsByKey(key, num)
	if err != nil {
		return nil, err
	}

	res := make([]*wal.ChunkPosition, 0)

	for _, nodeId := range nodeIdList {
		res = append(res, vi.graphNodeMap[nodeId].item.pos.pos)
	}

	return res, nil
}

func (vi *VectorIndex) Get(key []byte) *wal.ChunkPosition {
	chunkWrapper := vi.btreeIndex.Get(key)

	if chunkWrapper == nil {
		return nil
	}
	return chunkWrapper.pos
}

func (vi *VectorIndex) Delete(key []byte) (*wal.ChunkPosition, bool) {
	wrapper, res := vi.btreeIndex.Delete(key)
	if wrapper == nil {
		return nil, res
	}

	wrapper.deleted = true

	vi.toDelete.Add(1)

	if float64(vi.toDelete.Load()) > float64(len(vi.graphNodeMap)) * kReconstructPercent {
		vi.reconstructGraph()
	}
	return wrapper.pos, res
}

func (vi *VectorIndex) Size() int {
	return vi.btreeIndex.Size()
}

func (vi *VectorIndex) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.btreeIndex.Ascend(handleFn)
}

func (vi *VectorIndex) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.btreeIndex.Descend(handleFn)
}

func (vi *VectorIndex) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.btreeIndex.AscendRange(startKey, endKey, handleFn)
}

func (vi *VectorIndex) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.btreeIndex.DescendRange(startKey, endKey, handleFn)
}

func (vi *VectorIndex) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.btreeIndex.AscendGreaterOrEqual(key, handleFn)
}

func (vi *VectorIndex) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	vi.btreeIndex.DescendLessOrEqual(key, handleFn)
}
