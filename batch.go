package rosedb

import (
	"fmt"
	"sync"

	"github.com/bwmarrin/snowflake"

	"github.com/rosedblabs/wal"
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
	pendingWrites map[string]*LogRecord // save the data to be written
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool // whether the batch has been committed
	rollbacked    bool // whether the batch has been rollbacked
	batchId       *snowflake.Node
}

// NewBatch creates a new Batch instance.
func NewBatch() interface{} {
	batch := &Batch{
		committed: false,
	}
	return batch
}

func (b *Batch) WithDB(db *DB) *Batch {
	b.db = db
	b.lock()
	return b
}

func (b *Batch) Init(ops ...Option) *Batch {
	for _, do := range ops {
		do(&b.options)
	}
	b.committed = false
	if !b.options.ReadOnly {
		b.pendingWrites = make(map[string]*LogRecord)
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		b.batchId = node
	}
	return b
}

type Option func(*BatchOptions)

func WithSync(sync bool) Option {
	return func(opt *BatchOptions) {
		opt.Sync = sync
	}
}

func WithReadOnly(readOnly bool) Option {
	return func(opt *BatchOptions) {
		opt.ReadOnly = readOnly
	}
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
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
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

	// get from pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type == LogRecordDeleted {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
		b.mu.RUnlock()
	}

	// get from data file
	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
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
	if position := b.db.index.Get(key); position != nil {
		// write to pendingWrites if the key exists
		b.pendingWrites[string(key)] = &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		}
	} else {
		delete(b.pendingWrites, string(key))
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

	// check if the key exists in pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			return record.Type != LogRecordDeleted, nil
		}
		b.mu.RUnlock()
	}

	// check if the key exists in data file
	position := b.db.index.Get(key)
	return position != nil, nil
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

	// check if committed or discarded
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Generate()
	positions := make(map[string]*wal.ChunkPosition)

	// write to wal
	for _, record := range b.pendingWrites {
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record)
		pos, err := b.db.dataFiles.Write(encRecord)
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}

	// write a record to indicate the end of the batch
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	})
	if _, err := b.db.dataFiles.Write(endRecord); err != nil {
		return err
	}

	// flush wal if necessary
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// write to index
	for key, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, positions[key])
		}
	}

	b.committed = true
	return nil
}

// Rollback discards a uncommitted batch instance.
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

	if !b.options.ReadOnly {
		// clear pendingWrites
		b.pendingWrites = nil
	}

	b.rollbacked = true
	return nil
}
