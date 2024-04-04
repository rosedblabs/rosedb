package index

import (
	"bytes"
	"encoding/gob"
	queue "github.com/Jcowwell/go-algorithm-club/PriorityQueue"
	"github.com/drewlanenga/govector"
	"github.com/rosedblabs/wal"
	"sync"
)

type VectorItem struct {
	key govector.Vector
	pos *wal.ChunkPosition
}

type GraphNode struct {
	item VectorItem
}

type VectorIndex struct {
	graph           map[uint32]map[uint32]struct{}
	graphNodeMap    map[uint32]GraphNode
	currGraphNodeId uint32
	m               uint32
	maxM            uint32
	interval        uint32 // decide which node will be entry point

	btreeIndex *MemoryBTree
	mu         sync.RWMutex

	entryNode []uint32
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

func (vi *VectorIndex) Put(key govector.Vector, position *wal.ChunkPosition) (bool, error) {
	// TODO: check uniqueness in B-tree Index
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(key)
	if err != nil {
		return false, err
	}
	bTreeKey := buffer.Bytes()
	existKey := vi.btreeIndex.Get(bTreeKey)
	if existKey != nil {
		return true, nil
	}
	// insert key into b-tree
	vi.btreeIndex.Put(bTreeKey, position)

	newNodeId := vi.currGraphNodeId
	vi.currGraphNodeId++

	// find m closest nodes
	nodeIdList, e := vi.getNodeIdsByKey(key, vi.m)
	if e != nil {
		return false, e
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()

	// add node to entry node every #interval times of put
	if newNodeId%vi.interval == 0 {
		vi.entryNode = append(vi.entryNode, newNodeId)
	}
	graphNode := GraphNode{item: VectorItem{
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
			for dNode, _ := range vi.graph[nodeId] {
				dis, err := distance(nodeVector, vi.graphNodeMap[dNode].item.key)
				if err != nil {
					return false, err
				}
				if dis > maxDistance {
					maxDistance = dis
					deleteNode = dNode
				}
			}
			vi.DeleteEdge(nodeId, deleteNode)
		}
	}
	return true, nil
}
