package rosedb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
)

// Batch is a batch operations of the database.
// If readonly is true, you can only get data from the batch by Get method.
// An error will be returned if you try to use Put or Delete method.
//
// If readonly is false, you can use Put and Delete method to write data to the batch.
// The data will be written to the database when you call Commit method.
//
// Batch is not a transaction, it does not guarantee isolation.
// But it can guarantee atomicity, consistency and durability(if the Sync options is true).
//
// You must call Commit method to commit the batch, otherwise the DB will be locked.
type Batch struct {
	db            *DB
	pendingWrites []*LogRecord // save the data to be written
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool // whether the batch has been committed
	rollbacked    bool // whether the batch has been rollbacked
	batchId       *snowflake.Node
	buffers       []*bytebufferpool.ByteBuffer
}

// NewBatch creates a new Batch instance.
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		options:    options,
		committed:  false,
		rollbacked: false,
	}
	if !options.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func newBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func newRecord() interface{} {
	return &LogRecord{}
}

func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.committed = false
	b.rollbacked = false
	// put all buffers back to the pool
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put adds a key-value pair to the batch for writing.
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	var record *LogRecord
	// if the key exists in pendingWrites, update the value directly
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	if record == nil {
		// if the key does not exist in pendingWrites, write a new record
		// the record will be put back to the pool when the batch is committed or rollbacked
		record = b.db.recordPool.Get().(*LogRecord)
		b.pendingWrites = append(b.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	b.mu.Unlock()

	return nil
}

// PutWithTTL adds a key-value pair with ttl to the batch for writing.
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	var record *LogRecord
	// if the key exists in pendingWrites, update the value directly
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	if record == nil {
		// if the key does not exist in pendingWrites, write a new record
		// the record will be put back to the pool when the batch is committed or rollbacked
		record = b.db.recordPool.Get().(*LogRecord)
		b.pendingWrites = append(b.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mu.Unlock()

	return nil
}

// Get retrieves the value associated with a given key from the batch.
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// get from pendingWrites
	b.mu.RLock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	b.mu.RUnlock()

	// if the record is in pendingWrites, return the value directly
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Value, nil
	}

	// get key/value from data file
	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	// check if the record is deleted or expired
	record = b.db.recordPool.Get().(*LogRecord)
	decodeLogRecord(chunk, record)
	defer b.db.recordPool.Put(record)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// Delete marks a key for deletion in the batch.
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// only need key and type when deleting a value.
	var exist bool
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			b.pendingWrites[i].Type = LogRecordDeleted
			b.pendingWrites[i].Value = nil
			b.pendingWrites[i].Expire = 0
			exist = true
			break
		}
	}
	if !exist {
		b.pendingWrites = append(b.pendingWrites, &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		})
	}
	b.mu.Unlock()

	return nil
}

// Exist checks if the key exists in the database.
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// check if the key exists in pendingWrites
	b.mu.RLock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	b.mu.RUnlock()

	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}

	// check if the key exists in index
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}

	// check if the record is deleted or expired
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record = b.db.recordPool.Get().(*LogRecord)
	decodeLogRecord(chunk, record)
	defer b.db.recordPool.Put(record)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// Expire sets the ttl of the key.
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}

	// if the key exists in pendingWrites, update the expiry time directly
	if record != nil {
		// return key not found if the record is deleted or expired
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
	} else {
		// if the key does not exist in pendingWrites, get the value from wal
		position := b.db.index.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		now := time.Now()
		record = b.db.recordPool.Get().(*LogRecord)
		decodeLogRecord(chunk, record)
		// if the record is deleted or expired, we can assume that the key does not exist,
		// and delete the key from the index
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			b.db.index.Delete(key)
			return ErrKeyNotFound
		}
		// now we get the value from wal, update the expiry time
		// and rewrite the record to pendingWrites
		record.Expire = now.Add(ttl).UnixNano()
		b.pendingWrites = append(b.pendingWrites, record)
	}

	return nil
}

// TTL returns the ttl of the key.
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	// check if the key exists in pendingWrites
	if len(b.pendingWrites) > 0 {
		var record *LogRecord
		for i := len(b.pendingWrites) - 1; i >= 0; i-- {
			if bytes.Equal(key, b.pendingWrites[i].Key) {
				record = b.pendingWrites[i]
				break
			}
		}
		// if the key exists in pendingWrites, return the ttl directly
		if record != nil {
			if record.Expire == 0 {
				return -1, nil
			}
			// return key not found if the record is deleted or expired
			if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
				return -1, ErrKeyNotFound
			}
			// now we get the valid expiry time, we can calculate the ttl
			return time.Duration(record.Expire - now.UnixNano()), nil
		}
	}

	// if the key does not exist in pendingWrites, get the value from wal
	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	// return key not found if the record is deleted or expired
	record := b.db.recordPool.Get().(*LogRecord)
	decodeLogRecord(chunk, record)
	defer b.db.recordPool.Put(record)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}

	// now we get the valid expiry time, we can calculate the ttl
	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	return -1, nil
}

// Persist removes the ttl of the key.
func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// if the key exists in pendingWrites, update the expiry time directly
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted && record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = 0
	} else {
		// check if the key exists in index
		position := b.db.index.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		record := b.db.recordPool.Get().(*LogRecord)
		decodeLogRecord(chunk, record)
		now := time.Now().UnixNano()
		// check if the record is deleted or expired
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
			return ErrKeyNotFound
		}
		// if the expiration time is 0, it means that the key has no expiration time,
		// so we can return directly
		if record.Expire == 0 {
			return nil
		}

		// set the expiration time to 0, and rewrite the record to wal
		record.Expire = 0
		b.pendingWrites = append(b.pendingWrites, record)
	}

	return nil
}

// Commit commits the batch, if the batch is readonly or empty, it will return directly.
//
// It will iterate the pendingWrites and write the data to the database,
// then write a record to indicate the end of the batch to guarantee atomicity.
// Finally, it will write the index.
func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// check if committed or rollbacked
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Generate()
	now := time.Now().UnixNano()
	// write to wal buffer
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}

	// write a record to indicate the end of the batch
	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(endRecord)

	// write to wal file
	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}

	// flush wal if necessary
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// write to index
	for i, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, chunkPositions[i])
		}

		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			b.db.watcher.putEvent(e)
		}
		// put the record back to the pool
		b.db.recordPool.Put(record)
	}

	b.committed = true
	return nil
}

// Rollback discards an uncommitted batch instance.
// the discard operation will clear the buffered data and release the lock.
func (b *Batch) Rollback() error {
	defer b.unlock()

	if b.db.closed {
		return ErrDBClosed
	}

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}

	if !b.options.ReadOnly {
		// clear pendingWrites
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
	}

	b.rollbacked = true
	return nil
}
