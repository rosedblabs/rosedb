package index

import (
	"sync"

	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"

	queue "github.com/Jcowwell/go-algorithm-club/PriorityQueue"
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
	mu              sync.RWMutex

	entryNode []uint32
}

type priorityQueueItem struct {
	distance float64
	nodeId   uint32
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
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	// initialize all with entry points
	for _, e := range vi.entryNode {
		d, err := distance(key, vi.graphNodeMap[e].item.key)
		if err != nil {
			return nil, err
		}
		candidateQueue.Enqueue(priorityQueueItem{distance: d, nodeId: e})

		resultQueue.Enqueue(priorityQueueItem{distance: d, nodeId: e})
		// keep result queue the same number as parameter num
		if uint32(resultQueue.Count()) < num {
			resultQueue.Dequeue()
		}

		visited[e] = struct{}{}
	}
	for candidateQueue.Count() != 0 {
		currNode, _ := candidateQueue.Dequeue()
		furthestNode, _ := resultQueue.Peek()

		// we assume there will be no node close to the target
		if currNode.distance > furthestNode.distance {
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
				if uint32(resultQueue.Count()) < num {
					resultQueue.Dequeue()
				}
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

func (vi *VectorIndex) Get(key govector.Vector, num uint32) ([]govector.Vector, error) {
	nodeIdList, err := vi.getNodeIdsByKey(key, num)
	if err != nil {
		return nil, err
	}

	if len(vi.entryNode) == 0 {
		return []govector.Vector{}, nil
	}
	res := make([]govector.Vector, 0)

	for _, nodeId := range nodeIdList {
		res = append(res, vi.graphNodeMap[nodeId].item.key)
	}
	return res, nil
}

func (vi *VectorIndex) Put(key govector.Vector, position *wal.ChunkPosition) (bool, error) {

	// TODO: check uniqueness in B-tree Index

	// find m closest nodes
	nodeIdList, e := vi.getNodeIdsByKey(key, vi.m)
	if e != nil {
		return false, e
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()
	// build graph
	// if one node has more than max_m edges, we need to delete edges

}
