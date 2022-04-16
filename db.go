package rosedb

import (
	"encoding/binary"
	"errors"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/ds/zset"
	"github.com/flower-corp/rosedb/ioselector"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
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

	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	// ErrIntegerOverflow overflows int64 limitations
	ErrIntegerOverflow = errors.New("increment or decrement overflow")

	// ErrWrongKeyType value is not a number
	ErrWrongKeyType = errors.New("value is not an integer")
)

const (
	logFileTypeNum   = 5
	encodeHeaderSize = 10
)

type (
	// RoseDB a db instance.
	RoseDB struct {
		activeLogFiles   map[DataType]*logfile.LogFile
		archivedLogFiles map[DataType]archivedFiles
		fidMap           map[DataType][]uint32 // only used at startup, never update even though log files changed.
		discard          *discard
		dumpState        ioselector.IOSelector
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
		fid     uint32
		offset  int64
		setSize int // only used by set and zset
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	listIndex struct {
		mu      *sync.RWMutex
		indexes *list.List
	}

	hashIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{indexes: list.New(), mu: new(sync.RWMutex)}
}

func newHashIdx() *hashIndex {
	return &hashIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func newSetIdx() *setIndex {
	return &setIndex{
		idxTree: art.NewART(),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

func newZSetIdx() *zsetIndex {
	return &zsetIndex{
		indexes: zset.New(),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
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
	if err := db.loadLogFiles(false); err != nil {
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
	for _, activeFile := range db.activeLogFiles {
		_ = activeFile.Close()
	}
	// close the archived files.
	for _, archived := range db.archivedLogFiles {
		for _, file := range archived {
			_ = file.Sync()
			_ = file.Close()
		}
	}
	atomic.StoreUint32(&db.closed, 1)
	return nil
}

// Sync persist the db files to stable storage.
func (db *RoseDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// iterate and sync all the active files.
	for _, activeFile := range db.activeLogFiles {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
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
	if activeLogFile.WriteAt+int64(esize) > opts.LogFileSizeThreshold {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}

		db.mu.Lock()
		// save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// open a new log file.
		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, ftype, iotype)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		if dataType == String {
			db.discard.setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
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

func (db *RoseDB) loadLogFiles(excludeStrs bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()
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
		if dataType == String && excludeStrs {
			continue
		}
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
			lf, err := logfile.OpenLogFile(opts.DBPath, fid, opts.LogFileSizeThreshold, ftype, iotype)
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
	lf, err := logfile.OpenLogFile(opts.DBPath, logfile.InitialLogFileId, opts.LogFileSizeThreshold, ftype, iotype)
	if err != nil {
		return err
	}

	if dataType == String {
		db.discard.setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
	}
	db.activeLogFiles[dataType] = lf
	return nil
}

func (db *RoseDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], subKey)
		return buf
	}
	return header[:index]
}

func (db *RoseDB) decodeKey(key []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(key[index:])
	index += i
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discard.valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
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
	maybeRewrite := func(fid uint32, offset int64, entry *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()
		value := db.strIndex.idxTree.Get(entry.Key)
		if value == nil {
			return nil
		}

		indexNode, _ := value.(*indexNode)
		if indexNode == nil {
			return nil
		}
		// rewrite valid entry.
		if indexNode.fid == fid && indexNode.offset == offset {
			pos, err := db.writeLogEntry(entry, String)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(entry, pos, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	iterateAndHandle := func(file *logfile.LogFile) error {
		var offset int64
		ts := time.Now().Unix()
		for {
			entry, size, err := file.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			eoff := offset
			offset += size
			if entry.Type == logfile.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt <= ts) {
				continue
			}
			if err := maybeRewrite(file.Fid, eoff, entry); err != nil {
				return err
			}
		}
		return nil
	}

	activeLogFile := db.getActiveLogFile(String)
	activeFid := activeLogFile.Fid
	ccl, err := db.discard.getCCL(activeFid, db.opts.LogFileGCRatio)
	if err != nil {
		return err
	}

	for _, fid := range ccl {
		logFile := db.getArchivedLogFile(String, fid)
		if logFile == nil {
			return ErrLogFileNotFound
		}
		if err := iterateAndHandle(logFile); err != nil {
			return err
		}

		db.mu.Lock()
		delete(db.archivedLogFiles[String], logFile.Fid)
		db.mu.Unlock()
		if err := logFile.Delete(); err != nil {
			return err
		}
		db.discard.clear(logFile.Fid)
	}
	return nil
}
