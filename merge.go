package rosedb

import (
	"encoding/binary"
	"io"
	"path"
	"path/filepath"
	"sync/atomic"

	"github.com/rosedblabs/wal"
)

const (
	mergeDirSuffixName = "-merge"
)

// Merge merges all the data files in the database.
// It will iterate all the data files, find the valid data,
// and rewrite the data to the new data file.
//
// Merge operation maybe a very time-consuming operation when the database is large.
// So it is recommended to perform this operation when the database is idle.
func (db *DB) Merge() error {
	db.mu.Lock()
	// check if the daabase is closed
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	// check if the merge operation is running
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return ErrMergeRunning
	}
	// set the mergeRunning flag to true
	atomic.StoreUint32(&db.mergeRunning, 1)
	// set the mergeRunning flag to false when the merge operation is completed
	defer atomic.StoreUint32(&db.mergeRunning, 0)

	prevActiveSegId := db.dataFiles.ActiveSegmentID()
	// rotate the write-ahead log, and get the new write-ahead log file
	if err := db.dataFiles.OpenNewActiveSegment(); err != nil {
		return err
	}

	// we can unlock the mutex here, because the write-ahead log files has been rotated,
	// and the new active segment file will be used for the subsequent write operation.
	// Our Merge operation will only read from the older segment files.
	db.mu.Unlock()

	// open a merge db to write the data to the new data file.
	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	// iterate all the data files, and write the valid data to the new data file.
	reader := db.dataFiles.NewReaderWithMax(prevActiveSegId)
	for {
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLogRecord(chunk)
		// Only handle the normal log record,
		// LogRecordDeleted and LogRecordBatchFinished will be ignored.
		if record.Type == LogRecordNormal {
			indexPos := db.index.Get(record.Key)
			if indexPos != nil && positionEquals(indexPos, position) {
				newPosition, err := mergeDB.dataFiles.Write(chunk)
				if err != nil {
					return err
				}
				// Since the mergeDB will never be used for any read or write operations,
				// it is not necessary to update the index.
				//
				// And now we should write the new posistion to the write-ahead log,
				// which is so-called HINT FILE in bitcask paper.
				// The HINT FILE will be used to rebuild the index when the database is restarted.
				_, err = mergeDB.hintFile.Write(encodeHintRecord(record.Key, newPosition))
				if err != nil {
					return err
				}
			}
		}
	}

	// After rewrite all the data, we should add a file to indicate that the merge operation is completed.
	// So when we restart the database, we can know that the merge is completed,
	// otherwise, we will delete the merge directory and redo the merge operation.
	mergeFinFile, err := mergeDB.openMergeFinishedFile()
	if err != nil {
		return err
	}
	_, err = mergeFinFile.Write(encodeMergeFinRecord(prevActiveSegId))
	if err != nil {
		return err
	}
	// close the merge finished file
	if err := mergeFinFile.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) openMergeDB() (*DB, error) {
	options := db.options
	// we don't need to use the original sync policy,
	// because we can sync the data file manually after the merge operation is completed.
	options.Sync, options.BytesPerSync = false, 0
	options.DirPath = db.mergeDirPath()
	db, err := Open(options)
	if err != nil {
		return nil, err
	}

	// open the hint files to write the new position of the data.
	hintFile, err := wal.Open(wal.Options{
		DirPath:       options.DirPath,
		SementFileExt: hintFileNameSuffix,
		Sync:          false,
		BytesPerSync:  0,
		BlockCache:    0,
	})
	if err != nil {
		return nil, err
	}

	db.hintFile = hintFile
	return db, nil
}

func (db *DB) mergeDirPath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}

func (db *DB) openMergeFinishedFile() (*wal.WAL, error) {
	return wal.Open(wal.Options{
		DirPath:       db.options.DirPath,
		SementFileExt: mergeFinNameSuffix,
		Sync:          false,
		BytesPerSync:  0,
		BlockCache:    0,
	})
}

func positionEquals(a, b *wal.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

func encodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	// SegmentId BlockNumber ChunkOffset ChunkSize
	//    5          5           10          5      =    25
	// see binary.MaxVarintLen64 and binary.MaxVarintLen32
	buf := make([]byte, 25)
	var index = 0

	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(pos.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(pos.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(pos.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, index+len(key))
	copy(result, buf[:index])
	copy(result[index:], key)
	return result
}

func encodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
