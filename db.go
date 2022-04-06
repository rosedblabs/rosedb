package rosedb

import (
	"encoding/binary"
	"errors"
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/hash"
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/ds/set"
	"github.com/flower-corp/rosedb/ds/zset"
	"github.com/flower-corp/rosedb/ioselector"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
	"io/ioutil"
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
)

const (
	logFileTypeNum   = 5
	dumpFilePath     = "dump"
	dumpStateFile    = "DUMP_STATE"
	dumpRecordSize   = 12
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
		expiredAt int64
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
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{indexes: list.New(), mu: new(sync.RWMutex)}
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

	// load dump state, must execute it before load log files.
	if err := db.loadDumpState(); err != nil {
		return nil, err
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

	// handle in memory data dumping(List, Hash, Set, and ZSet)
	go db.handleDumping()
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

func (db *RoseDB) handleDumping() {
	if db.opts.InMemoryDataDumpInterval <= 0 {
		return
	}

	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ticker := time.NewTicker(db.opts.InMemoryDataDumpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := db.doRunDump(); err != nil {
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

		indexNode, _ := value.(*strIndexNode)
		if indexNode == nil {
			return nil
		}
		// rewrite valid entry.
		if indexNode.fid == fid && indexNode.offset == offset {
			pos, err := db.writeLogEntry(entry, String)
			if err != nil {
				return err
			}
			if err = db.updateStrIndex(entry, pos, false); err != nil {
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

func (db *RoseDB) doRunDump() (err error) {
	dumpPath := filepath.Join(db.opts.DBPath, dumpFilePath)
	if err = os.MkdirAll(dumpPath, os.ModePerm); err != nil {
		return
	}
	defer func() {
		_ = os.RemoveAll(dumpPath)
	}()

	findDeletedAndRotateFiles := func(dType DataType) ([]*logfile.LogFile, error) {
		db.mu.Lock()
		defer db.mu.Unlock()
		var filesTobeDeleted []*logfile.LogFile
		// rotate log files.
		for _, lf := range db.archivedLogFiles[dType] {
			filesTobeDeleted = append(filesTobeDeleted, lf)
		}

		activeFile := db.activeLogFiles[dType]
		if activeFile == nil {
			return filesTobeDeleted, nil
		}
		ftype, iotype := logfile.FileType(dType), logfile.IOType(db.opts.IoType)
		lf, err := logfile.OpenLogFile(db.opts.DBPath, activeFile.Fid+1, db.opts.LogFileSizeThreshold, ftype, iotype)
		if err != nil {
			return nil, err
		}
		db.activeLogFiles[dType] = lf
		if db.archivedLogFiles[dType] == nil {
			db.archivedLogFiles[dType] = make(archivedFiles)
		}
		db.archivedLogFiles[dType][activeFile.Fid] = activeFile
		filesTobeDeleted = append(filesTobeDeleted, activeFile)
		return filesTobeDeleted, nil
	}

	wg := new(sync.WaitGroup)
	for dType := List; dType < logFileTypeNum; dType++ {
		wg.Add(1)
		go func(dataType DataType) {
			defer wg.Done()
			unlock := db.lockByType(dataType)
			defer unlock()
			deletedFiles, err := findDeletedAndRotateFiles(dataType)
			if err != nil {
				logger.Errorf("error occurred while find files [%v]: ", err)
				return
			}
			if len(deletedFiles) == 0 {
				return
			}
			// dump start
			if err = db.markDumpStart(dataType, deletedFiles[0].Fid, deletedFiles[len(deletedFiles)-1].Fid); err != nil {
				logger.Errorf("mark dump start err [%v]: ", err)
				return
			}
			if err = db.dumpInternal(dataType, deletedFiles); err != nil {
				logger.Errorf("error occurred while dump, type=[%v],err=[%v]: ", dataType, err)
				return
			}
		}(dType)
	}
	wg.Wait()

	// reload log files.
	if err := db.loadLogFiles(true); err != nil {
		return err
	}
	return nil
}

func (db *RoseDB) lockByType(dataType DataType) func() {
	var mu *sync.RWMutex
	switch dataType {
	case List:
		mu = db.listIndex.mu
	case Hash:
		mu = db.hashIndex.mu
	case Set:
		mu = db.setIndex.mu
	case ZSet:
		mu = db.zsetIndex.mu
	}
	mu.Lock()
	return mu.Unlock
}

func (db *RoseDB) dumpInternal(dataType DataType, deletedFiles []*logfile.LogFile) error {
	entriesChn := make(chan *logfile.LogEntry, 1024)
	switch dataType {
	case List:
		go db.iterateListAndSend(entriesChn)
	case Hash:
		go db.iterateHashAndSend(entriesChn, db.encodeKey)
	case Set:
		go db.iterateSetsAndSend(entriesChn)
	case ZSet:
		go db.iterateZsetAndSend(entriesChn, db.encodeKey)
	}

	var logFile *logfile.LogFile
	dumpPath := filepath.Join(db.opts.DBPath, dumpFilePath)
	rotateFile := func(size int) error {
		if logFile == nil || logFile.WriteAt+int64(size) > db.opts.LogFileSizeThreshold {
			var activeFid uint32 = logfile.InitialLogFileId
			if logFile != nil {
				activeFid = logFile.Fid + 1
				if err := logFile.Sync(); err != nil {
					return err
				}
				_ = logFile.Close()
			}
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(db.opts.IoType)
			lf, err := logfile.OpenLogFile(dumpPath, activeFid, db.opts.LogFileSizeThreshold, ftype, iotype)
			if err != nil {
				return err
			}
			logFile = lf
		}
		return nil
	}

	for entry := range entriesChn {
		buf, size := logfile.EncodeEntry(entry)
		if err := rotateFile(size); err != nil {
			return err
		}
		if err := logFile.Write(buf); err != nil {
			return err
		}
	}

	fileInfos, err := ioutil.ReadDir(dumpPath)
	if err != nil {
		return err
	}
	// mark dump has finished successfully.
	if err = db.markDumpFinish(dataType); err != nil {
		return err
	}

	// delete older log files.
	for _, lf := range deletedFiles {
		_ = lf.Delete()
		delete(db.archivedLogFiles[dataType], lf.Fid)
	}
	// rename log files in dump path.
	fileType := logfile.FileType(dataType)
	for _, file := range fileInfos {
		oldPath := filepath.Join(dumpPath, file.Name())
		newPath := filepath.Join(db.opts.DBPath, file.Name())
		if strings.HasPrefix(file.Name(), logfile.FileNamesMap[fileType]) {
			_ = os.Rename(oldPath, newPath)
		}
	}
	return nil
}
