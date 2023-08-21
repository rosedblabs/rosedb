package rosedb

import (
	"sync"
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

type WatchChan <-chan *Event

type Watcher interface {
	Watch() WatchChan
	Close() error
}

// watcher temporarily stores event information,
// as it is generated until it is synchronized to DB's watch.
//
// If the event is overflow, It will remove the oldest data,
// even if event hasn't been read yet.
type watcher struct {
	queue   eventQueue
	mu      sync.RWMutex
	watchCh chan *Event

	cond     *sync.Cond
	capacity uint64

	closeMu    sync.Mutex
	notifyCh   chan struct{}
	cancelFunc func()
}

func newWatcher(capacity uint64) *watcher {
	w := &watcher{
		queue: eventQueue{
			Events:   make([]*Event, capacity),
			Capacity: capacity,
		},
		watchCh:  make(chan *Event, 100),
		cond:     sync.NewCond(&sync.Mutex{}),
		notifyCh: make(chan struct{}),
	}
	w.cancelFunc = func() {
		close(w.notifyCh)
		close(w.watchCh)
	}
	return w
}

func (w *watcher) putEvent(e *Event) {
	w.mu.Lock()
	w.queue.push(e)
	if w.queue.isFull() {
		w.queue.frontTakeAStep()
	}
	if w.capacity < w.queue.Capacity {
		w.capacity++
	}

	w.cond.Signal()
	w.mu.Unlock()
}

// getEvent if queue is empty, it will return nil.
func (w *watcher) getEvent() *Event {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.queue.isEmpty() {
		return nil
	}
	if w.capacity > 0 {
		w.capacity--
	}
	return w.queue.pop()
}

func (w *watcher) Watch() WatchChan {
	return w.watchCh
}

func (w *watcher) Close() error {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()

	w.cancelFunc()
	return nil
}

// sendEvent send events to DB's watch
func (w *watcher) sendEvent() {
	for {
		select {
		case <-w.notifyCh:
			return
		default:
			// Note that there may be three locks held here,
			// so you need to pay attention to the order of
			// the locks to avoid deadlocks.
			w.cond.L.Lock()
			for w.capacity <= 0 {
				w.cond.Wait()
			}
			event := w.getEvent()
			if event == nil {
				w.cond.L.Unlock()
				break
			}
			// check again
			w.closeMu.Lock()
			select {
			case <-w.notifyCh:
			default:
				w.watchCh <- event
			}
			w.closeMu.Unlock()
			w.cond.L.Unlock()
		}
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
