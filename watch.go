package rosedb

import (
	"sync"
	"time"
)

const (
	ActionPut    = "Put"
	ActionDelete = "Delete"
)

type Event struct {
	Action  string
	Key     []byte
	Value   []byte
	BatchId uint64
}

func NewEvent(action string, key, value []byte, batchId uint64) *Event {
	return &Event{
		Action:  action,
		Key:     key,
		Value:   value,
		BatchId: batchId,
	}
}

type Watcher struct {
	queue eventQueue
	mu    sync.RWMutex
}

func NewWatcher(capacity int) *Watcher {
	// Leave two spaces to determine whether the queue is full and overwrite the write.
	size := capacity + 2
	return &Watcher{
		queue: eventQueue{
			Events:   make([]*Event, size),
			Capacity: size,
		},
	}
}

func (w *Watcher) Insert(e *Event) {
	w.mu.Lock()
	w.queue.push(e)
	if w.queue.isFull() {
		w.queue.frontTakeAStep()
	}
	w.mu.Unlock()
}

func (w *Watcher) Scan() (e *Event, isEmpty bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if isEmpty = w.queue.isEmpty(); isEmpty {
		return
	}
	e = w.queue.pop()
	return
}

func (w *Watcher) Sync(c chan *Event) {
	for {
		e, isEmpty := w.Scan()
		if isEmpty {
			time.Sleep(100 * time.Millisecond)
		}
		c <- e
	}
}

type eventQueue struct {
	Events   []*Event
	Capacity int
	Front    int // read point
	Back     int // write point
}

func (eq *eventQueue) push(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity
	return
}

func (eq *eventQueue) pop() *Event {
	e := eq.Events[eq.Front]
	eq.frontTakeAStep()
	return e
}

func (eq *eventQueue) isFull() bool {
	return (eq.Back+1)%eq.Capacity == eq.Front
}

func (eq *eventQueue) isEmpty() bool {
	return eq.Back == eq.Front
}

func (eq *eventQueue) frontTakeAStep() {
	eq.Front = (eq.Front + 1) % eq.Capacity
}
