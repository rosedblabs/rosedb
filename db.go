package rosedb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/rosedblabs/rosedb/v2/index"
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
)

const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

// DB represents a ROSEDB database instance.
// It is built on the bitcask model, which is a log-structured storage.
// It uses WAL to write data, and uses an in-memory index to store the key
// and the position of the data in the WAL,
// the index will be rebuilt when the database is opened.
//
// The main advantage of ROSEDB is that it is very fast to write, read, and delete data.
// Because it only needs one disk IO to complete a single operation.
//
// But since we should store all keys and their positions(index) in memory,
// our total data size is limited by the memory size.
//
// So if your memory can almost hold all the keys, ROSEDB is the perfect storage engine for you.
type DB struct {
	dataFiles        *wal.WAL // data files are a sets of segment files in WAL.
	hintFile         *wal.WAL // hint file is used to store the key and the position for fast startup.
	index            index.Indexer
	options          Options
	fileLock         *flock.Flock
	mu               sync.RWMutex
	closed           bool
	mergeRunning     uint32 // indicate if the database is merging
	batchPool        sync.Pool
	recordPool       sync.Pool
	encodeHeader     []byte
	watchCh          chan *Event // user consume channel for watch events
	watcher          *Watcher
	expiredCursorKey []byte     // the location to which DeleteExpiredKeys executes.
	cronScheduler    *cron.Cron // cron scheduler for auto merge task
}

// Stat represents the statistics of the database.
type Stat struct {
	// Total number of keys
	KeysNum int
	// Total disk size of database directory
	DiskSize int64
}

// Open a database with the specified options.
// If the database directory does not exist, it will be created automatically.
//
// Multiple processes can not use the same database directory at the same time,
// otherwise it will return ErrDatabaseIsUsing.
//
// It will open the wal files in the database directory and load the index from them.
// Return the DB instance, or an error if any.
func Open(options Options) (*DB, error) {
	// check options
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create data directory if not exist
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create file lock, prevent multiple processes from using the same database directory
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// load merge files if exists
	if err = loadMergeFiles(options.DirPath); err != nil {
		return nil, err
	}

	// init DB instance
	db := &DB{
		index:        index.NewIndexer(),
		options:      options,
		fileLock:     fileLock,
		batchPool:    sync.Pool{New: newBatch},
		recordPool:   sync.Pool{New: newRecord},
		encodeHeader: make([]byte, maxLogRecordHeaderSize),
	}

	// open data files
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}

	// load index
	if err = db.loadIndex(); err != nil {
		return nil, err
	}

	// enable watch
	if options.WatchQueueSize > 0 {
		db.watchCh = make(chan *Event, 100)
		db.watcher = NewWatcher(options.WatchQueueSize)
		// run a goroutine to synchronize event information
		go db.watcher.sendEvent(db.watchCh)
	}

	// enable auto merge task
	if len(options.AutoMergeCronExpr) > 0 {
		db.cronScheduler = cron.New(
			cron.WithParser(
				cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour |
					cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
			),
		)
		_, err = db.cronScheduler.AddFunc(options.AutoMergeCronExpr, func() {
			// maybe we should deal with different errors with different logic,
			// but a background task can't omit its error.
			// after auto merge, we should close and reopen the db.
			_ = db.Merge(true)
		})
		if err != nil {
			return nil, err
		}
		db.cronScheduler.Start()
	}

	return db, nil
}

func (db *DB) openWalFiles() (*wal.WAL, error) {
	// open data files from WAL
	walFiles, err := wal.Open(wal.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    db.options.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		Sync:           db.options.Sync,
		BytesPerSync:   db.options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

func (db *DB) loadIndex() error {
	// load index from hint file
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}
	// load index from data files
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}

// Close the database, close all data files and release file lock.
// Set the closed flag to true.
// The DB instance cannot be used after closing.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.closeFiles(); err != nil {
		return err
	}

	// release file lock
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	// close watch channel
	if db.options.WatchQueueSize > 0 {
		close(db.watchCh)
	}

	// close auto merge cron scheduler
	if db.cronScheduler != nil {
		db.cronScheduler.Stop()
	}

	db.closed = true
	return nil
}

// closeFiles close all data files and hint file
func (db *DB) closeFiles() error {
	// close wal
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	// close hint file if exists
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync all data files to the underlying storage.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.dataFiles.Sync()
}

// Stat returns the statistics of the database.
func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("rosedb: get database directory size error: %v", err))
	}

	return &Stat{
		KeysNum:  db.index.Size(),
		DiskSize: diskSize,
	}
}

// Put a key-value pair into the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Put operation.
func (db *DB) Put(key, value []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Put(key, value); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// PutWithTTL a key-value pair into the database, with a ttl.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one PutWithTTL operation.
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.PutWithTTL(key, value, ttl); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Get the value of the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Get operation.
func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

// Delete the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Delete operation.
func (db *DB) Delete(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Delete(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Exist checks if the specified key exists in the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Exist operation.
func (db *DB) Exist(key []byte) (bool, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Exist(key)
}

// Expire sets the ttl of the key.
func (db *DB) Expire(key []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single expire operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Expire(key, ttl); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// TTL get the ttl of the key.
func (db *DB) TTL(key []byte) (time.Duration, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.TTL(key)
}

// Persist removes the ttl of the key.
// If the key does not exist or expired, it will return ErrKeyNotFound.
func (db *DB) Persist(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single persist operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Persist(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

func (db *DB) Watch() (<-chan *Event, error) {
	if db.options.WatchQueueSize <= 0 {
		return nil, ErrWatchDisabled
	}
	return db.watchCh, nil
}

// Ascend calls handleFn for each key/value pair in the db in ascending order.
func (db *DB) Ascend(handleFn func(k, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendRange calls handleFn for each key/value pair in the db within the range [startKey, endKey] in ascending order.
func (db *DB) AscendRange(startKey, endKey []byte, handleFn func(k, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendGreaterOrEqual calls handleFn for each key/value pair in the db with keys greater than or equal to the given key.
func (db *DB) AscendGreaterOrEqual(key []byte, handleFn func(k, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendGreaterOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendKeys calls handleFn for each key in the db in ascending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) AscendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// AscendKeysRange calls handleFn for keys within a range in the db in ascending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) AscendKeysRange(startKey, endKey, pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// Descend calls handleFn for each key/value pair in the db in descending order.
func (db *DB) Descend(handleFn func(k, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendRange calls handleFn for each key/value pair in the db within the range [startKey, endKey] in descending order.
func (db *DB) DescendRange(startKey, endKey []byte, handleFn func(k, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendLessOrEqual calls handleFn for each key/value pair in the db with keys less than or equal to the given key.
func (db *DB) DescendLessOrEqual(key []byte, handleFn func(k, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendLessOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendKeys calls handleFn for each key in the db in descending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) DescendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// DescendKeysRange calls handleFn for keys within a range in the db in descending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) DescendKeysRange(startKey, endKey, pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

func (db *DB) checkValue(chunk []byte) []byte {
	record := decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type != LogRecordDeleted && !record.IsExpired(now) {
		return record.Value
	}
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.SegmentSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	if len(options.AutoMergeCronExpr) > 0 {
		if _, err := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).
			Parse(options.AutoMergeCronExpr); err != nil {
			return fmt.Errorf("database auto merge cron expression is invalid, err: %s", err)
		}
	}

	return nil
}

// loadIndexFromWAL loads index from WAL.
// It will iterate over all the WAL files and read data
// from them to rebuild the index.
func (db *DB) loadIndexFromWAL() error {
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}
	indexRecords := make(map[uint64][]*IndexRecord)
	now := time.Now().UnixNano()
	// get a reader for WAL
	reader := db.dataFiles.NewReader()
	db.dataFiles.SetIsStartupTraversal(true)
	for {
		// if the current segment id is less than the mergeFinSegmentId,
		// we can skip this segment because it has been merged,
		// and we can load index from the hint file directly.
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}

		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// decode and get log record
		record := decodeLogRecord(chunk)

		// if we get the end of a batch,
		// all records in this batch are ready to be indexed.
		if record.Type == LogRecordBatchFinished {
			batchId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.recordType == LogRecordNormal {
					db.index.Put(idxRecord.key, idxRecord.position)
				}
				if idxRecord.recordType == LogRecordDeleted {
					db.index.Delete(idxRecord.key)
				}
			}
			// delete indexRecords according to batchId after indexing
			delete(indexRecords, uint64(batchId))
		} else if record.Type == LogRecordNormal && record.BatchId == mergeFinishedBatchID {
			// if the record is a normal record and the batch id is 0,
			// it means that the record is involved in the merge operation.
			// so put the record into index directly.
			db.index.Put(record.Key, position)
		} else {
			// expired records should not be indexed
			if record.IsExpired(now) {
				db.index.Delete(record.Key)
				continue
			}
			// put the record into the temporary indexRecords
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}
	db.dataFiles.SetIsStartupTraversal(false)
	return nil
}

// DeleteExpiredKeys scan the entire index in ascending order to delete expired keys.
// It is a time-consuming operation, so we need to specify a timeout
// to prevent the DB from being unavailable for a long time.
func (db *DB) DeleteExpiredKeys(timeout time.Duration) error {
	// set timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)

	var innerErr error
	now := time.Now().UnixNano()
	go func(ctx context.Context) {
		db.mu.Lock()
		defer db.mu.Unlock()
		for {
			// select 100 keys from the db.index
			positions := make([]*wal.ChunkPosition, 0, 100)
			db.index.AscendGreaterOrEqual(db.expiredCursorKey, func(k []byte, pos *wal.ChunkPosition) (bool, error) {
				positions = append(positions, pos)
				if len(positions) >= 100 {
					return false, nil
				}
				return true, nil
			})

			// If keys in the db.index has been traversed, len(positions) will be 0.
			if len(positions) == 0 {
				db.expiredCursorKey = nil
				done <- struct{}{}
				return
			}

			// delete from index if the key is expired.
			for _, pos := range positions {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					innerErr = err
					done <- struct{}{}
					return
				}
				record := decodeLogRecord(chunk)
				if record.IsExpired(now) {
					db.index.Delete(record.Key)
				}
				db.expiredCursorKey = record.Key
			}
		}
	}(ctx)

	select {
	case <-ctx.Done():
		return innerErr
	case <-done:
		return innerErr
	}
}
