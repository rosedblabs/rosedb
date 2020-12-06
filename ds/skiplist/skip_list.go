//SkipList是跳表的实现，跳表是一个高效的可替代平衡二叉搜索树的数据结构
//它能够在O(log(n))的时间复杂度下进行插入、删除、查找操作
//跳表的具体解释可参考Wikipedia上的描述：https://zh.wikipedia.org/wiki/%E8%B7%B3%E8%B7%83%E5%88%97%E8%A1%A8

package skiplist

import (
	"bytes"
	"math/bits"
	"math/rand"
	"rosedb/index"
	"time"
)

//maxLevel表示跳表的最大层数，可根据实际情况进行调整
const maxLevel = 25

type (

	//跳表存储节点定义
	Node struct {
		next  [maxLevel]*Node
		prev  *Node
		level int
		key   []byte
		Value interface{}
	}

	//跳表定义
	SkipList struct {
		startLevels [maxLevel]*Node
		endLevels   [maxLevel]*Node
		maxNewLevel int
		maxLevel    int
		elemSize    int
	}
)

func New() *SkipList {
	rand.Seed(time.Now().UTC().UnixNano()) //设置随机数种子，防止伪随机

	return &SkipList{
		startLevels: [maxLevel]*Node{},
		endLevels:   [maxLevel]*Node{},
		maxNewLevel: maxLevel,
		maxLevel:    0,
		elemSize:    0,
	}
}

func (t *SkipList) Find(key []byte) (node *Node) {
	if t == nil || key == nil {
		return
	}

	return t.findNear(key, false)
}

func (t *SkipList) FindGreaterOrEqual(key []byte) (elem *Node) {
	if t == nil || key == nil {
		return
	}

	return t.findNear(key, true)
}

func (t *SkipList) Remove(key []byte) {
	if t == nil || t.Empty() || key == nil {
		return
	}

	idx := t.findIndex(key, 0)

	var currentNode *Node
	nextNode := currentNode

	for {
		if currentNode == nil {
			nextNode = t.startLevels[idx]
		} else {
			nextNode = currentNode.next[idx]
		}

		//找到元素并删除
		//if nextNode != nil && math.Abs(nextNode.key-key) <= t.eps {
		if nextNode != nil && compare(nextNode.key, key) == 0 {

			if currentNode != nil {
				currentNode.next[idx] = nextNode.next[idx]
			}

			if idx == 0 {
				if nextNode.next[idx] != nil {
					nextNode.next[idx].prev = currentNode
				}
				t.elemSize--
			}

			if t.startLevels[idx] == nextNode {
				t.startLevels[idx] = nextNode.next[idx]
				if t.startLevels[idx] == nil {
					t.maxLevel = idx - 1
				}
			}

			if nextNode.next[idx] == nil {
				t.endLevels[idx] = currentNode
			}
			nextNode.next[idx] = nil
		}

		//if nextNode != nil && nextNode.key < key {
		if nextNode != nil && compare(nextNode.key, key) < 0 {
			//在当前层向后遍历
			currentNode = nextNode
		} else {
			//到下一层索引
			idx--
			if idx < 0 {
				break
			}
		}
	}
}

func (t *SkipList) Add(e *index.Indexer) {
	if t == nil || e == nil {
		return
	}

	level := t.randomLevel(t.maxNewLevel)

	if level > t.maxLevel {
		level = t.maxLevel + 1
		t.maxLevel = level
	}

	elem := &Node{
		next:  [maxLevel]*Node{},
		level: level,
		key:   e.Key,
		Value: e,
	}

	t.elemSize++

	newFirst, newLast := true, true
	if !t.Empty() {
		//newFirst = elem.key < t.startLevels[0].key
		newFirst = compare(elem.key, t.startLevels[0].key) < 0

		//newLast = elem.key > t.endLevels[0].key
		newLast = compare(elem.key, t.endLevels[0].key) > 0
	}

	normallyInserted := false
	if !newFirst && !newLast {

		normallyInserted = true

		idx := t.findIndex(elem.key, level)

		var curNode *Node
		nextNode := t.startLevels[idx]

		for {
			if curNode == nil {
				nextNode = t.startLevels[idx]
			} else {
				nextNode = curNode.next[idx]
			}

			//if idx <= level && (nextNode == nil || nextNode.key > elem.key) {
			if idx <= level && (nextNode == nil || compare(nextNode.key, elem.key) > 0) {
				elem.next[idx] = nextNode
				if curNode != nil {
					curNode.next[idx] = elem
				}
				if idx == 0 {
					elem.prev = curNode
					if nextNode != nil {
						nextNode.prev = elem
					}
				}
			}

			//if nextNode != nil && nextNode.key <= elem.key {
			if nextNode != nil && compare(nextNode.key, elem.key) <= 0 {
				curNode = nextNode
			} else {
				idx--
				if idx < 0 {
					break
				}
			}
		}
	}

	for i := level; i >= 0; i-- {
		flag := false

		if newFirst || normallyInserted {
			//if t.startLevels[i] == nil || t.startLevels[i].key > elem.key {
			if t.startLevels[i] == nil || compare(t.startLevels[i].key, elem.key) > 0 {
				if i == 0 && t.startLevels[i] != nil {
					t.startLevels[i].prev = elem
				}
				elem.next[i] = t.startLevels[i]
				t.startLevels[i] = elem
			}

			if elem.next[i] == nil {
				t.endLevels[i] = elem
			}

			flag = true
		}

		if newLast {
			if !newFirst {
				if t.endLevels[i] != nil {
					t.endLevels[i].next[i] = elem
				}
				if i == 0 {
					elem.prev = t.endLevels[i]
				}
				t.endLevels[i] = elem
			}

			//if t.startLevels[i] == nil || t.startLevels[i].key > elem.key {
			if t.startLevels[i] == nil || compare(t.startLevels[i].key, elem.key) > 0 {
				t.startLevels[i] = elem
			}

			flag = true
		}

		if !flag {
			break
		}
	}
}

//校验跳表是否为空
func (t *SkipList) Empty() bool {
	return t.startLevels[0] == nil
}

//节点的后一个节点
func (t *SkipList) Next(e *Node) *Node {
	if e.next[0] == nil {
		return t.startLevels[0]
	}

	return e.next[0]
}

//节点的前一个节点
func (t *SkipList) Prev(e *Node) *Node {
	if e.prev == nil {
		return t.endLevels[0]
	}

	return e.prev
}

//Size 方法返回跳表中元素的个数
func (t *SkipList) Size() int {
	return t.elemSize
}

func (t *SkipList) SetVal(e *Node, key []byte, newValue interface{}) (ok bool) {
	//if (newValue.ExtractKey() - e.key) <= t.eps {
	if compare(key, e.key) == 0 {
		e.Value = newValue
		ok = true
	} else {
		ok = false
	}
	return
}

//生成随机层数
func (t *SkipList) randomLevel(maxLevel int) int {
	level := maxLevel - 1
	var x = rand.Uint64() & ((1 << uint(maxLevel-1)) - 1)
	zeroes := bits.TrailingZeros64(x)
	if zeroes <= maxLevel {
		level = zeroes
	}

	return level
}

//if t.startLevels[i] != nil && t.startLevels[i].key <= key || i <= level {
func (t *SkipList) findIndex(key []byte, level int) int {
	for i := t.maxLevel; i >= 0; i-- {
		if t.startLevels[i] != nil && compare(t.startLevels[i].key, key) <= 0 || i <= level {
			return i
		}
	}
	return 0
}

func (t *SkipList) findNear(key []byte, greaterOrEqual bool) (foundElem *Node) {
	foundElem = nil

	if t == nil || t.Empty() {
		return
	}

	idx := t.findIndex(key, 0)
	var curNode *Node

	curNode = t.startLevels[idx]
	nextNode := curNode

	//if greaterOrEqual && curNode.key > key {
	if greaterOrEqual && compare(curNode.key, key) > 0 {
		foundElem = curNode
		return
	}

	for {
		//if math.Abs(curNode.key-key) <= t.eps {
		if compare(curNode.key, key) == 0 {
			foundElem = curNode
			return
		}

		nextNode = curNode.next[idx]

		//if nextNode != nil && nextNode.key <= key {
		if nextNode != nil && compare(nextNode.key, key) <= 0 {
			//在当前层索引向右遍历
			curNode = nextNode
		} else {
			if idx > 0 {
				//if curNode.next[0] != nil && math.Abs(curNode.next[0].key-key) <= t.eps {
				if curNode.next[0] != nil && compare(curNode.next[0].key, key) == 0 {
					foundElem = curNode.next[0]
					return
				}
				//进入到下一层索引
				idx--
			} else {
				if greaterOrEqual {
					foundElem = nextNode
				}
				return
			}
		}
	}
}

//比较字节数组
//如果a == b 返回 0，如果 a > b 返回 1，如果 a < b 返回 -1
func compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
