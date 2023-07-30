package rosedb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
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
// So if your memory can almost hold all the keys, ROSEDB is the perfect stroage engine for you.
type DB struct {
	dataFiles    *wal.WAL  // data files are a sets of segment files in WAL.
	hintFile     *wal.WAL  // hint file is used to store the key and the position for fast startup.
	batchPool    sync.Pool // batchPool is a pool for storing batches
	index        index.Indexer
	options      Options
	fileLock     *flock.Flock
	mu           sync.RWMutex
	closed       bool
	mergeRunning uint32 // indicate if the database is merging
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
// otherwise it will retrun ErrDatabaseIsUsing.
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

	// open data files from WAL
	walFiles, err := wal.Open(wal.Options{
		DirPath:       options.DirPath,
		SegmentSize:   options.SegmentSize,
		SementFileExt: dataFileNameSuffix,
		BlockCache:    options.BlockCache,
		Sync:          options.Sync,
		BytesPerSync:  options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}

	// init DB instance
	db := &DB{
		dataFiles: walFiles,
		batchPool: sync.Pool{New: NewBatch},
		index:     index.NewIndexer(),
		options:   options,
		fileLock:  fileLock,
	}

	// load index frm hint file
	if err = db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// load index from data files
	if err = db.loadIndexFromWAL(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close the database, close all data files and release file lock.
// Set the closed flag to true.
// The DB instance cannot be used after closing.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

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
	// release file lock
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	db.closed = true
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
func (db *DB) Put(key []byte, value []byte) error {
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch := db.batchPool.Get().(*Batch)
	batch.Init(WithSync(false),
		WithReadOnly(false)).
		WithDB(db)
	defer db.batchPool.Put(batch)
	if err := batch.Put(key, value); err != nil {
		return err
	}
	return batch.Commit()
}

// Get the value of the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Get operation.
func (db *DB) Get(key []byte) ([]byte, error) {
	// Read-only operation
	batch := db.batchPool.Get().(*Batch)
	batch.Init(WithSync(false),
		WithReadOnly(true)).
		WithDB(db)
	defer db.batchPool.Put(batch)
	defer func() {
		_ = batch.Commit()
	}()
	return batch.Get(key)
}

// Delete the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Delete operation.
func (db *DB) Delete(key []byte) error {
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch := db.batchPool.Get().(*Batch)
	batch.Init(WithSync(false),
		WithReadOnly(false)).
		WithDB(db)
	defer db.batchPool.Put(batch)
	if err := batch.Delete(key); err != nil {
		return err
	}
	return batch.Commit()
}

// Exist checks if the specified key exists in the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Exist operation.
func (db *DB) Exist(key []byte) (bool, error) {
	// Read-only operation
	batch := db.batchPool.Get().(*Batch)
	batch.Init(WithSync(false),
		WithReadOnly(true)).
		WithDB(db)
	defer db.batchPool.Put(batch)
	defer func() {
		_ = batch.Commit()
	}()
	return batch.Exist(key)
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.SegmentSize <= 0 {
		return errors.New("database data file size must be greater than 0")
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
	// get a reader for WAL
	reader := db.dataFiles.NewReader()
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
			// put the record into the temporary indexRecords
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}
	return nil
}
