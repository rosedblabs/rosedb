package rosedb

import (
	"fmt"
	"sync"
	"time"
)

type WatchActionType = byte

const (
	WatchActionPut WatchActionType = iota
	WatchActionDelete
)

// Event
type Event struct {
	Action  WatchActionType
	Key     []byte
	Value   []byte
	BatchId uint64
}

func (e *Event) String() string {
	return fmt.Sprintf(`
	Event{
		Action: %d,
		Key: %s,
		Value: %s,
		BatchId: %d
	}`,
		e.Action,
		e.Key,
		e.Value,
		e.BatchId)
}

// Watcher temporarily stores event information,
// as it is generated until it is synchronized to DB's watch.
//
// If the event is overflow, It will remove the oldest data,
// even if event hasn't been read yet.
type Watcher struct {
	queue eventQueue
	mu    sync.RWMutex
}

func NewWatcher(capacity uint64) *Watcher {
	return &Watcher{
		queue: eventQueue{
			Events:   make([]*Event, capacity),
			Capacity: capacity,
		},
	}
}

func (w *Watcher) insertEvent(e *Event) {
	w.mu.Lock()
	w.queue.push(e)
	if w.queue.isFull() {
		w.queue.frontTakeAStep()
	}
	w.mu.Unlock()
}

// getEvent if queue is empty, it will return nil.
func (w *Watcher) getEvent() *Event {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if isEmpty := w.queue.isEmpty(); isEmpty {
		return nil
	}
	e := w.queue.pop()
	return e
}

// sendEvent send events to DB's watch
func (w *Watcher) sendEvent(c chan *Event) {
	for {
		e := w.getEvent()
		if e == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c <- e
	}
}

type eventQueue struct {
	Events   []*Event
	Capacity uint64
	Front    uint64 // read point
	Back     uint64 // write point
}

func (eq *eventQueue) push(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity
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
