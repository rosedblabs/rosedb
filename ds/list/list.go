package list

import (
	"container/list"
	"reflect"
)

// List is the implementation of doubly linked list.

// InsertOption insert option for LInsert.
type InsertOption uint8

const (
	// Before insert before pivot.
	Before InsertOption = iota
	// After insert after pivot.
	After
)

type (
	// List list idx.
	List struct {
		// record saves the List of a specified key.
		record Record

		// values saves the values of a List, help checking if a value exists in List.
		values map[string]map[string]int
	}

	// Record list record to save.
	Record map[string]*list.List
)

// New create a new list idx.
func New() *List {
	return &List{
		make(Record),
		make(map[string]map[string]int),
	}
}

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (lis *List) LPush(key string, val ...[]byte) int {
	return lis.push(true, key, val...)
}

// LPop removes and returns the first elements of the list stored at key.
func (lis *List) LPop(key string) []byte {
	return lis.pop(true, key)
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (lis *List) RPush(key string, val ...[]byte) int {
	return lis.push(false, key, val...)
}

// RPop removes and returns the last elements of the list stored at key.
func (lis *List) RPop(key string) []byte {
	return lis.pop(false, key)
}

// LIndex returns the element at index index in the list stored at key.
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

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
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

	if lis.values[key] != nil {
		cnt := lis.values[key][string(val)] - 1
		if cnt <= 0 {
			delete(lis.values[key], string(val))
		} else {
			lis.values[key][string(val)] = cnt
		}
	}
	return length
}

// LInsert inserts element in the list stored at key either before or after the reference value pivot.
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

	if lis.values[key] == nil {
		lis.values[key] = make(map[string]int)
	}
	lis.values[key][string(val)] += 1

	return item.Len()
}

// LSet sets the list element at index to element.
func (lis *List) LSet(key string, index int, val []byte) bool {
	e := lis.index(key, index)
	if e == nil {
		return false
	}

	if lis.values[key] == nil {
		lis.values[key] = make(map[string]int)
	}

	// update value count.
	if e.Value != nil {
		v := string(e.Value.([]byte))
		cnt := lis.values[key][v] - 1
		if cnt <= 0 {
			delete(lis.values[key], v)
		} else {
			lis.values[key][v] = cnt
		}
	}

	e.Value = val
	lis.values[key][string(val)] += 1
	return true
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
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

	mid := length >> 1

	// Traverse from left to right.
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				val = append(val, p.Value.([]byte))
			}
		}
	} else { // Traverse from right to left.
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

// LTrim trim an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
func (lis *List) LTrim(key string, start, end int) bool {
	item := lis.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)

	if start <= 0 && end >= length-1 {
		return false
	}

	if start > end || start >= length {
		lis.record[key] = nil
		lis.values[key] = nil
		return true
	}

	startEle, endEle := lis.index(key, start), lis.index(key, end)
	if end-start+1 < (length >> 1) {
		newList := list.New()
		newValuesMap := make(map[string]int)
		for p := startEle; p != endEle.Next(); p = p.Next() {
			newList.PushBack(p.Value)
			if p.Value != nil {
				newValuesMap[string(p.Value.([]byte))] += 1
			}
		}

		item = nil
		lis.record[key] = newList
		lis.values[key] = newValuesMap
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
			if lis.values[key] != nil && e.Value != nil {
				v := string(e.Value.([]byte))
				cnt := lis.values[key][v] - 1
				if cnt <= 0 {
					delete(lis.values[key], v)
				} else {
					lis.values[key][v] = cnt
				}
			}
		}
		ele = nil
	}
	return true
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (lis *List) LLen(key string) int {
	length := 0
	if lis.record[key] != nil {
		length = lis.record[key].Len()
	}

	return length
}

// LClear clear a specified key for List.
func (lis *List) LClear(key string) {
	delete(lis.record, key)
	delete(lis.values, key)
}

// LKeyExists check if the key of a List exists.
func (lis *List) LKeyExists(key string) (ok bool) {
	_, ok = lis.record[key]
	return
}

// LValExists check if the val exists in a specified List stored at key.
func (lis *List) LValExists(key string, val []byte) (ok bool) {
	if lis.values[key] != nil {
		cnt := lis.values[key][string(val)]
		ok = cnt > 0
	}
	return
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
	if lis.values[key] == nil {
		lis.values[key] = make(map[string]int)
	}

	for _, v := range val {
		if front {
			lis.record[key].PushFront(v)
		} else {
			lis.record[key].PushBack(v)
		}
		lis.values[key][string(v)] += 1
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
		// update value count.
		if lis.values[key] != nil {
			cnt := lis.values[key][string(val)] - 1
			if cnt <= 0 {
				delete(lis.values[key], string(val))
			} else {
				lis.values[key][string(val)] = cnt
			}
		}
	}
	return val
}

// check if the index is valid and returns the new index.
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

// handle the value of start and end (negative and corner case).
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
