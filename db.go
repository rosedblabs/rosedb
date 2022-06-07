package rosedb

import (
	"encoding/binary"
	"errors"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/zset"
	"github.com/flower-corp/rosedb/flock"
	"github.com/flower-corp/rosedb/ioselector"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"path/filepath"
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

	// ErrWrongValueType value is not a number
	ErrWrongValueType = errors.New("value is not an integer")

	// ErrWrongIndex index is out of range
	ErrWrongIndex = errors.New("index is out of range")
	
	// ErrGCRunning log file gc is running
	ErrGCRunning = errors.New("log file gc is running, retry later")
)

const (
	logFileTypeNum   = 5
	encodeHeaderSize = 10
	initialListSeq   = math.MaxUint32 / 2
	discardFilePath  = "DISCARD"
	lockFileName     = "FLOCK"
)

type (
	// RoseDB a db instance.
	RoseDB struct {
		activeLogFiles   map[DataType]*logfile.LogFile
		archivedLogFiles map[DataType]archivedFiles
		fidMap           map[DataType][]uint32 // only used at startup, never update even though log files changed.
		discards         map[DataType]*discard
		dumpState        ioselector.IOSelector
		opts             Options
		strIndex         *strIndex  // String indexes(adaptive-radix-tree).
		listIndex        *listIndex // List indexes.
		hashIndex        *hashIndex // Hash indexes.
		setIndex         *setIndex  // Set indexes.
		zsetIndex        *zsetIndex // Sorted set indexes.
		mu               sync.RWMutex
		fileLock         *flock.FileLockGuard
		closed           uint32
		gcState          int32
	}

	archivedFiles map[uint32]*logfile.LogFile

	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
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
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newHashIdx() *hashIndex {
	return &hashIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newSetIdx() *setIndex {
	return &setIndex{
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

	// acquire file lock to prevent multiple processes from accessing the same directory.
	lockPath := filepath.Join(opts.DBPath, lockFileName)
	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		opts:             opts,
		fileLock:         lockGuard,
		strIndex:         newStrsIndex(),
		listIndex:        newListIdx(),
		hashIndex:        newHashIdx(),
		setIndex:         newSetIdx(),
		zsetIndex:        newZSetIdx(),
	}

	// init discard file.
	if err := db.initDiscard(); err != nil {
		return nil, err
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

	if db.fileLock != nil {
		_ = db.fileLock.Release()
	}
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
	// close discard files.
	for _, dis := range db.discards {
		_ = dis.close()
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
	// sync discard file.
	for _, dis := range db.discards {
		if err := dis.sync(); err != nil {
			return err
		}
	}
	return nil
}

// RunLogFileGC run log file garbage collection manually.
func (db *RoseDB) RunLogFileGC(dataType DataType, fid int, gcRatio float64) error {
	if atomic.LoadInt32(&db.gcState) > 0 {
		return ErrGCRunning
	}
	return db.doRunGC(dataType, fid, gcRatio)
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
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
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

	db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = lf
	return nil
}

func (db *RoseDB) initDiscard() error {
	discardPath := filepath.Join(db.opts.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	for i := String; i < logFileTypeNum; i++ {
		name := logfile.FileNamesMap[logfile.FileType(i)] + discardFileName
		dis, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = dis
	}
	db.discards = discards
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

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool, dataType DataType) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discards[dataType].valChan <- node:
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
			if atomic.LoadInt32(&db.gcState) > 0 {
				logger.Warn("log file gc is running, skip it")
				break
			}
			for dType := String; dType < logFileTypeNum; dType++ {
				go func(dataType DataType) {
					err := db.doRunGC(dataType, -1, db.opts.LogFileGCRatio)
					if err != nil {
						logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(dType)
			}
		case <-quitSig:
			return
		}
	}
}

func (db *RoseDB) doRunGC(dataType DataType, specifiedFid int, gcRatio float64) error {
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()
		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, String)
			if err != nil {
				return err
			}
			// update index
			if err = db.updateIndexTree(db.strIndex.idxTree, ent, valuePos, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteList := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()
		var listKey = ent.Key
		if ent.Type != logfile.TypeListMeta {
			listKey, _ = db.decodeListKey(ent.Key)
		}
		if db.listIndex.trees[string(listKey)] == nil {
			return nil
		}
		idxTree := db.listIndex.trees[string(listKey)]
		indexVal := idxTree.Get(listKey)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, ent, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()
		key, field := db.decodeKey(ent.Key)
		if db.hashIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.hashIndex.trees[string(key)]
		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, Hash)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: field, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Hash); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()
		if db.setIndex.trees[string(ent.Key)] == nil {
			return nil
		}
		idxTree := db.setIndex.trees[string(ent.Key)]
		if err := db.setIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, Set)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Set); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteZSet := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.zsetIndex.mu.Lock()
		defer db.zsetIndex.mu.Unlock()
		key, _ := db.decodeKey(ent.Key)
		if db.zsetIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.zsetIndex.trees[string(key)]
		if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.zsetIndex.murhash.EncodeSum128()
		db.zsetIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == node.offset {
			valuePos, err := db.writeLogEntry(ent, ZSet)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, ZSet); err != nil {
				return err
			}
		}
		return nil
	}

	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil
	}
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}
	ccl, err := db.discards[dataType].getCCL(activeLogFile.Fid, gcRatio)
	if err != nil {
		return err
	}

	for _, fid := range ccl {
		if specifiedFid >= 0 && uint32(specifiedFid) != fid {
			continue
		}
		archivedFile := db.getArchivedLogFile(dataType, fid)
		if archivedFile == nil {
			continue
		}

		var offset int64
		for {
			ent, size, err := archivedFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			var off = offset
			offset += size
			if ent.Type == logfile.TypeDelete {
				continue
			}
			ts := time.Now().Unix()
			if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
				continue
			}
			var rewriteErr error
			switch dataType {
			case String:
				rewriteErr = maybeRewriteStrs(archivedFile.Fid, off, ent)
			case List:
				rewriteErr = maybeRewriteList(archivedFile.Fid, off, ent)
			case Hash:
				rewriteErr = maybeRewriteHash(archivedFile.Fid, off, ent)
			case Set:
				rewriteErr = maybeRewriteSets(archivedFile.Fid, off, ent)
			case ZSet:
				rewriteErr = maybeRewriteZSet(archivedFile.Fid, off, ent)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}

		// delete older log file.
		db.mu.Lock()
		delete(db.archivedLogFiles[dataType], fid)
		_ = archivedFile.Delete()
		db.mu.Unlock()
		// clear discard state.
		db.discards[dataType].clear(fid)
	}
	return nil
}
