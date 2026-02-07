package rosedb

import (
	"sync"
	"time"
)

type WatchActionType = byte

const (
	WatchActionPut WatchActionType = iota
	WatchActionDelete
)

// Event is the event that occurs when the database is modified.
// It is used to synchronize the watch of the database.
type Event struct {
	Action  WatchActionType
	Key     []byte
	Value   []byte
	BatchId uint64
}

// Watcher temporarily stores event information,
// as it is generated until it is synchronized to DB's watch.
//
// If the event is overflow, It will remove the oldest data,
// even if event hasn't been read yet.
type Watcher struct {
	queue eventQueue
	mu    sync.Mutex
	done  chan struct{}
}

func NewWatcher(capacity uint64) *Watcher {
	return &Watcher{
		queue: eventQueue{
			Events:   make([]*Event, capacity),
			Capacity: capacity,
		},
		done: make(chan struct{}),
	}
}

func (w *Watcher) putEvent(e *Event) {
	w.mu.Lock()
	w.queue.push(e)
	if w.queue.isFull() {
		w.queue.frontTakeAStep()
	}
	w.mu.Unlock()
}

// getEvent if queue is empty, it will return nil.
func (w *Watcher) getEvent() *Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.queue.isEmpty() {
		return nil
	}
	return w.queue.pop()
}

// sendEvent send events to DB's watch.
// It will return when the watcher is closed.
func (w *Watcher) sendEvent(c chan *Event) {
	for {
		select {
		case <-w.done:
			return
		default:
			event := w.getEvent()
			if event == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			select {
			case c <- event:
			case <-w.done:
				return
			}
		}
	}
}

// Close stops the watcher goroutine.
func (w *Watcher) Close() {
	close(w.done)
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
