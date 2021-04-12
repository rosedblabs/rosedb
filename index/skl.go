package index

//SkipList是跳表的实现，跳表是一个高效的可替代平衡二叉搜索树的数据结构
//它能够在O(log(n))的时间复杂度下进行插入、删除、查找操作
//跳表的具体解释可参考Wikipedia上的描述：https://zh.wikipedia.org/wiki/%E8%B7%B3%E8%B7%83%E5%88%97%E8%A1%A8

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	//跳表索引最大层数，可根据实际情况进行调整
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

//遍历节点的函数，bool返回值为false时遍历结束
type handleEle func(e *Element) bool

type (
	Node struct {
		next []*Element
	}

	// Element 跳表存储元素定义
	Element struct {
		Node
		key   []byte
		value interface{}
	}

	// SkipList 跳表定义
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

// NewSkipList 初始化一个空的跳表
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

func (e *Element) Key() []byte {
	return e.key
}

func (e *Element) Value() interface{} {
	return e.value
}

func (e *Element) SetValue(val interface{}) {
	e.value = val
}

// Next 跳表的第一层索引是原始数据，有序排列，可根据Next方法获取一个串联所有数据的链表
func (e *Element) Next() *Element {
	return e.next[0]
}

// Front 获取跳表头元素，获取到之后，可向后遍历得到所有的数据
//	e := list.Front()
//	for p := e; p != nil; p = p.Next() {
//		//do something with Element p
//	}
func (t *SkipList) Front() *Element {
	return t.next[0]
}

// Put 方法存储一个元素至跳表中，如果key已经存在，则会更新其对应的value
//因此此跳表的实现暂不支持相同的key
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
//未找到则返回nil
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
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

// Remove Remove方法根据key删除跳表中的元素，返回删除后的元素指针
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
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

//找到key对应的前一个节点索引的信息
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

//生成索引随机层数
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
