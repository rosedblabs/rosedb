package rosedb

import (
	"encoding/binary"
	"errors"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/hash"
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/ds/set"
	"github.com/flower-corp/rosedb/ds/zset"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io/ioutil"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrKeyNotFound key not found
	ErrKeyNotFound = errors.New("key not found")

	// ErrLogFileNotFound log file not found
	ErrLogFileNotFound = errors.New("log file not found")
)

const (
	// size of each log file: 512MB
	logFileSize    = 512 << 20
	logFileTypeNum = 5
)

type (
	// RoseDB a db instance.
	RoseDB struct {
		activeLogFiles   map[DataType]*logfile.LogFile
		archivedLogFiles map[DataType]archivedFiles
		fidMap           map[DataType][]uint32 // only used at startup, never update even though log files changed.
		discard          *discard
		opts             Options
		strIndex         *strIndex  // String indexes(adaptive-radix-tree).
		listIndex        *listIndex // List indexes.
		hashIndex        *hashIndex // Hash indexes.
		setIndex         *setIndex  // Set indexes.
		zsetIndex        *zsetIndex // Sorted set indexes.
		mu               sync.RWMutex
		closed           uint32
	}

	archivedFiles map[uint32]*logfile.LogFile

	valuePos struct {
		fid    uint32
		offset int64
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	strIndexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
	}

	listIndex struct {
		mu      *sync.RWMutex
		indexes *list.List
	}

	hashIndex struct {
		mu      *sync.RWMutex
		indexes *hash.Hash
	}

	setIndex struct {
		mu      *sync.RWMutex
		indexes *set.Set
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{mu: new(sync.RWMutex), idxTree: art.NewART()}
}

func newListIdx() *listIndex {
	return &listIndex{
		indexes: list.New(), mu: new(sync.RWMutex),
	}
}

func newHashIdx() *hashIndex {
	return &hashIndex{indexes: hash.New(), mu: new(sync.RWMutex)}
}

func newSetIdx() *setIndex {
	return &setIndex{indexes: set.New(), mu: new(sync.RWMutex)}
}

func newZSetIdx() *zsetIndex {
	return &zsetIndex{indexes: zset.New(), mu: new(sync.RWMutex)}
}

// Open a rosedb instance. You must call Close after using it.
func Open(opts Options) (*RoseDB, error) {
	// create the dir path if not exists.
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	discard, err := newDiscard(opts.DBPath, discardFileName)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		discard:          discard,
		opts:             opts,
		strIndex:         newStrsIndex(),
		listIndex:        newListIdx(),
		hashIndex:        newHashIdx(),
		setIndex:         newSetIdx(),
		zsetIndex:        newZSetIdx(),
	}

	// load the log files from disk.
	if err := db.loadLogFiles(); err != nil {
		return nil, err
	}

	// load indexes from log files.
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// handle log files garbage collection.
	go db.handleLogFileGC()
	return db, nil
}

// Close db and save relative configs.
func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// close and sync the active file.

	// close the archived files.

	atomic.StoreUint32(&db.closed, 1)
	return nil
}

// Sync persist the db files to stable storage.
func (db *RoseDB) Sync() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// iterate and sync all the active files.
	if err != nil {
		return
	}
	return
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *RoseDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

func (db *RoseDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFiles[dataType] != nil {
		lf = db.archivedLogFiles[dataType][fid]
	}
	return lf
}

// write entry to log file.
func (db *RoseDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}

	opts := db.opts
	entBuf, esize := logfile.EncodeEntry(ent)
	if activeLogFile.WriteAt+int64(esize) > logFileSize {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}

		db.mu.Lock()
		// save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// open a new log file.
		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, logFileSize, ftype, iotype)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		if dataType == String {
			db.discard.setTotal(lf.Fid, logFileSize)
		}
		db.activeLogFiles[dataType] = lf
		activeLogFile = lf
		db.mu.Unlock()
	}

	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	// write entry and sync(if necessary)
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

func (db *RoseDB) loadLogFiles() error {
	fileInfos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}

	fidMap := make(map[DataType][]uint32)
	for _, file := range fileInfos {
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(logfile.FileTypesMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}
	db.fidMap = fidMap

	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// load log file in order.
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		opts := db.opts
		for i, fid := range fids {
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
			lf, err := logfile.OpenLogFile(opts.DBPath, fid, logFileSize, ftype, iotype)
			if err != nil {
				return err
			}
			// latest one is active log file.
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = lf
			} else {
				db.archivedLogFiles[dataType][fid] = lf
			}
		}
	}
	return nil
}

func (db *RoseDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	opts := db.opts
	ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
	lf, err := logfile.OpenLogFile(opts.DBPath, logfile.InitialLogFileId, logFileSize, ftype, iotype)
	if err != nil {
		return err
	}

	if dataType == String {
		db.discard.setTotal(lf.Fid, logFileSize)
	}
	db.activeLogFiles[dataType] = lf
	return nil
}

func (db *RoseDB) encodeKey(key, field []byte) []byte {
	header := make([]byte, hashHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(field)))
	length := len(key) + len(field)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], field)
		return buf
	}
	return header[:index]
}

func (db *RoseDB) decodeKey(hashKey []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(hashKey[index:])
	index += i
	_, i = binary.Varint(hashKey[index:])
	index += i
	sep := index + int(keySize)
	return hashKey[index:sep], hashKey[sep:]
}

func (db *RoseDB) handleLogFileGC() {
	if db.opts.LogFileGCInterval <= 0 {
		return
	}

	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := db.doRunGC(); err != nil {
				logger.Errorf("value log compaction err: %+v", err)
			}
		case <-quitSig:
			return
		}
	}
}

func (db *RoseDB) doRunGC() error {
	return nil
}
