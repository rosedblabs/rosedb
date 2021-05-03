package list

import (
	"container/list"
	"reflect"
)

// List is the implementation of doubly linked list

// InsertOption insert option for LInsert
type InsertOption uint8

const (
	// Before insert before
	Before InsertOption = iota
	// After insert after
	After
)

type (
	// List list idx
	List struct {
		record Record
	}

	// Record list record to save
	Record map[string]*list.List
)

// New create a new list idx
func New() *List {
	return &List{
		make(Record),
	}
}

// LPush 在列表的头部添加元素，返回添加后的列表长度
// Insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (lis *List) LPush(key string, val ...[]byte) int {
	return lis.push(true, key, val...)
}

// LPop 取出列表头部的元素
// Removes and returns the first elements of the list stored at key.
func (lis *List) LPop(key string) []byte {
	return lis.pop(true, key)
}

// RPush 在列表的尾部添加元素，返回添加后的列表长度
// Insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (lis *List) RPush(key string, val ...[]byte) int {
	return lis.push(false, key, val...)
}

// RPop 取出列表尾部的元素
// Removes and returns the last elements of the list stored at key.
func (lis *List) RPop(key string) []byte {
	return lis.pop(false, key)
}

// LIndex 返回列表在index处的值，如果不存在则返回nil
// Returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
func (lis *List) LIndex(key string, index int) []byte {
	ok, newIndex := lis.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex
	var val []byte
	e := lis.index(key, index)
	if e != nil {
		val = e.Value.([]byte)
	}

	return val
}

// LRem 根据参数 count 的值，移除列表中与参数 value 相等的元素
//count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
//count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
//count = 0 : 移除列表中所有与 value 相等的值
//返回成功删除的元素个数
//Removes the first count occurrences of elements equal to element from the list stored at key.
//
//The count argument influences the operation in the following ways:
//count > 0: Remove elements equal to element moving from head to tail.
//count < 0: Remove elements equal to element moving from tail to head.
//count = 0: Remove all elements equal to element.
func (lis *List) LRem(key string, val []byte, count int) int {
	item := lis.record[key]
	if item == nil {
		return 0
	}

	var ele []*list.Element
	if count == 0 {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	if count > 0 {
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	if count < 0 {
		for p := item.Back(); p != nil && len(ele) < -count; p = p.Prev() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	for _, e := range ele {
		item.Remove(e)
	}

	length := len(ele)
	ele = nil
	return length
}

// LInsert 将值 val 插入到列表 key 当中，位于值 pivot 之前或之后
// 如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到 pivot ，返回 -1
// Inserts element in the list stored at key either before or after the reference value pivot.
func (lis *List) LInsert(key string, option InsertOption, pivot, val []byte) int {
	e := lis.find(key, pivot)
	if e == nil {
		return -1
	}

	item := lis.record[key]
	if option == Before {
		item.InsertBefore(val, e)
	}
	if option == After {
		item.InsertAfter(val, e)
	}

	return item.Len()
}

// LSet 将列表 key 下标为 index 的元素的值设置为 val
// bool返回值表示操作是否成功
// Sets the list element at index to element
func (lis List) LSet(key string, index int, val []byte) bool {
	e := lis.index(key, index)
	if e == nil {
		return false
	}

	e.Value = val
	return true
}

// LRange 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定
//如果 start 下标比列表的最大下标(len-1)还要大，那么 LRange 返回一个空列表
//如果 end 下标比 len 还要大，则将 end 的值设置为 len - 1
//Returns the specified elements of the list stored at key.
//The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
//These offsets can also be negative numbers indicating offsets starting at the end of the list.
//For example, -1 is the last element of the list, -2 the penultimate, and so on.
func (lis *List) LRange(key string, start, end int) [][]byte {
	var val [][]byte
	item := lis.record[key]

	if item == nil || item.Len() <= 0 {
		return val
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)

	if start > end || start >= length {
		return val
	}

	//从列表中取出val
	mid := length >> 1

	//从左往右遍历
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				val = append(val, p.Value.([]byte))
			}
		}
	} else { //否则从右往左遍历
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}

		if len(val) > 0 {
			for i, j := 0, len(val)-1; i < j; i, j = i+1, j-1 {
				val[i], val[j] = val[j], val[i]
			}
		}
	}

	return val
}

// LTrim 对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
// Trim an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
func (lis *List) LTrim(key string, start, end int) bool {
	item := lis.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)

	//start小于等于左边界，end大于等于右边界，不处理
	if start <= 0 && end >= length-1 {
		return false
	}

	//start大于end，或者start超出右边界，则直接将列表置空
	if start > end || start >= length {
		lis.record[key] = nil
		return true
	}

	startEle, endEle := lis.index(key, start), lis.index(key, end)
	if end-start+1 < (length >> 1) {
		newList := list.New()
		for p := startEle; p != endEle.Next(); p = p.Next() {
			newList.PushBack(p.Value)
		}

		item = nil
		lis.record[key] = newList
	} else {
		var ele []*list.Element
		for p := item.Front(); p != startEle; p = p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p = p.Prev() {
			ele = append(ele, p)
		}

		for _, e := range ele {
			item.Remove(e)
		}

		ele = nil
	}

	return true
}

// LLen 返回指定key的列表中的元素个数
// Returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (lis *List) LLen(key string) int {
	length := 0
	if lis.record[key] != nil {
		length = lis.record[key].Len()
	}

	return length
}

func (lis *List) find(key string, val []byte) *list.Element {
	item := lis.record[key]
	var e *list.Element

	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				e = p
				break
			}
		}
	}

	return e
}

func (lis *List) index(key string, index int) *list.Element {
	ok, newIndex := lis.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex
	item := lis.record[key]
	var e *list.Element

	if item != nil && item.Len() > 0 {
		if index <= (item.Len() >> 1) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i-- {
				val = val.Prev()
			}
			e = val
		}
	}

	return e
}

func (lis *List) push(front bool, key string, val ...[]byte) int {

	if lis.record[key] == nil {
		lis.record[key] = list.New()
	}

	for _, v := range val {
		if front {
			lis.record[key].PushFront(v)
		} else {
			lis.record[key].PushBack(v)
		}
	}

	return lis.record[key].Len()
}

func (lis *List) pop(front bool, key string) []byte {
	item := lis.record[key]
	var val []byte

	if item != nil && item.Len() > 0 {
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}

		val = e.Value.([]byte)
		item.Remove(e)
	}

	return val
}

// 校验index是否有效，并返回新的index
// check if the index is valid and returns the new index
func (lis *List) validIndex(key string, index int) (bool, int) {
	item := lis.record[key]
	if item == nil || item.Len() <= 0 {
		return false, index
	}

	length := item.Len()
	if index < 0 {
		index += length
	}

	return index >= 0 && index < length, index
}

// 处理start和end的值(负数和边界情况)
// handle the value of start and end (negative and corner case)
func (lis *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}

	if end < 0 {
		end += length
	}

	if start < 0 {
		start = 0
	}

	if end >= length {
		end = length - 1
	}

	return start, end
}
