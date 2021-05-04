package index

//SkipList是跳表的实现，跳表是一个高效的可替代平衡二叉搜索树的数据结构
//它能够在O(log(n))的时间复杂度下进行插入、删除、查找操作
//跳表的具体解释可参考Wikipedia上的描述：https://zh.wikipedia.org/wiki/%E8%B7%B3%E8%B7%83%E5%88%97%E8%A1%A8

// SkipList is the implementation of skip list, skip list is an efficient data structure that can replace the balanced binary search tree
// It can insert, delete, and query in O(logN) time complexity
// For a specific explanation of the skip list,  you can refer to Wikipedia: https://en.wikipedia.org/wiki/Skip_list
import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	// 跳表索引最大层数，可根据实际情况进行调整
	// the max level of the skl indexes, can be adjusted according to the actual situation
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

// 遍历节点的函数，bool返回值为false时遍历结束
// iterate the skl node, ends when the return value is false
type handleEle func(e *Element) bool

type (
	// Node the skl node
	Node struct {
		next []*Element
	}

	// Element element is the data stored
	Element struct {
		Node
		key   []byte
		value interface{}
	}

	// SkipList define the skip list
	SkipList struct {
		Node
		maxLevel       int
		Len            int
		randSource     rand.Source
		probability    float64
		probTable      []float64
		prevNodesCache []*Node
	}
)

// NewSkipList create a new skip list
func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}

// Key the key of the Element
func (e *Element) Key() []byte {
	return e.key
}

// Value the value of the Element
func (e *Element) Value() interface{} {
	return e.value
}

// SetValue set the elem val
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

// Next 跳表的第一层索引是原始数据，有序排列，可根据Next方法获取一个串联所有数据的链表
// The first-level index of the skip list is the original data, which is arranged in an orderly manner.
// A linked list of all data in series can be obtained according to the Next method.
func (e *Element) Next() *Element {
	return e.next[0]
}

// Front 获取跳表头元素，获取到之后，可向后遍历得到所有的数据
// Get the head element of skl, and get all data by traversing backward
//	e := list.Front()
//	for p := e; p != nil; p = p.Next() {
//		//do something with Element p
//	}
func (t *SkipList) Front() *Element {
	return t.next[0]
}

// Put 方法存储一个元素至跳表中，如果key已经存在，则会更新其对应的value
// 因此此跳表的实现暂不支持相同的key
// put an element into skl, replace the value if key already exists.
func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := t.backNodes(key)

	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, t.randomLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}

	t.Len++
	return element
}

// Get 方法根据 key 查找对应的 Element 元素
// 未找到则返回nil
// find value by the key, returns nil if not found.
func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

// Exist 判断跳表是否存在对应的key
// check if exists the key in skl.
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

// Remove Remove方法根据key删除跳表中的元素，返回删除后的元素指针
// remove element by the key.
func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)

	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}

		t.Len--
		return element
	}

	return nil
}

// Foreach 遍历跳表中的每个元素
// iterate all elements in the skip list.
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// 找到key对应的前一个节点索引的信息
// find the previous node at the key
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node
	var next *Element

	prevs := t.prevNodesCache

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}

		prevs[i] = prev
	}

	return prevs
}

// FindPrefix 找到第一个和前缀匹配的Element
// find the first element that matches the prefix
func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil {
		next = t.Front()
	}

	return next
}

// 生成索引随机层数
// generate random index level
func (t *SkipList) randomLevel() (level int) {
	r := float64(t.randSource.Int63()) / (1 << 63)

	level = 1
	for level < t.maxLevel && r < t.probTable[level] {
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
