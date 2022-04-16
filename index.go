package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/list"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/util"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// DataType Define the data structure type.
type DataType = int8

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *RoseDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	case List:
		db.buildListIndex(ent)
	case Hash:
		db.buildHashIndex(ent, pos)
	case Set:
		db.buildSetIndex(ent, pos)
	case ZSet:
		db.buildZSetIndex(ent, pos)
	}
}

func (db *RoseDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildListIndex(ent *logfile.LogEntry) {
	key, cmd := list.DecodeCommandKey(ent.Key)
	switch cmd {
	case list.LPush:
		db.listIndex.indexes.LPush(key, ent.Value)
	case list.RPush:
		db.listIndex.indexes.RPush(key, ent.Value)
	case list.LPop:
		db.listIndex.indexes.LPop(key)
	case list.RPop:
		db.listIndex.indexes.RPop(key)
	case list.LSet:
		rawKey, i := db.decodeKey(key)
		index, err := strconv.Atoi(string(i))
		if err != nil {
			logger.Errorf("decode lset key err at startup: %v", err)
		} else {
			db.listIndex.indexes.LSet(rawKey, index, ent.Value)
		}
	}
}

func (db *RoseDB) buildHashIndex(ent *logfile.LogEntry, pos *valuePos) {
	if ent.Type == logfile.TypeDelete {
		db.hashIndex.idxTree.Delete(ent.Key)
		return
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.hashIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	if db.setIndex.trees[string(ent.Key)] == nil {
		db.setIndex.trees[string(ent.Key)] = art.NewART()
	}
	db.setIndex.idxTree = db.setIndex.trees[string(ent.Key)]
	if ent.Type == logfile.TypeDelete {
		db.setIndex.idxTree.Delete(ent.Value)
		return
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	db.setIndex.idxTree.Put(ent.Value, idxNode)
}

func (db *RoseDB) buildZSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	if ent.Type == logfile.TypeDelete {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		db.zsetIndex.idxTree.Delete(ent.Value)
		return
	}

	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	db.zsetIndex.idxTree = db.zsetIndex.trees[string(key)]

	if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	db.zsetIndex.idxTree.Put(sum, idxNode)
}

func (db *RoseDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				logger.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				entry, esize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += esize
			}
			// set latest log file`s WriteAt.
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go iterateAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}

func (db *RoseDB) updateIndexTree(ent *logfile.LogEntry, pos *valuePos, sendDiscard bool, dType DataType) error {
	var size = pos.zsetSize
	if dType != ZSet {
		_, size = logfile.EncodeEntry(ent)
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}

	var idxTree *art.AdaptiveRadixTree
	switch dType {
	case String:
		idxTree = db.strIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case Set:
		idxTree = db.setIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}

	if dType == Set {
		idxNode.value, ent.Key = nil, ent.Value
	}
	oldVal, updated := idxTree.Put(ent.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated)
	}
	return nil
}

func (db *RoseDB) getVal(key []byte, dataType DataType) ([]byte, error) {
	// Get index info from a skip list in memory.
	var idxTree *art.AdaptiveRadixTree
	switch dataType {
	case String:
		idxTree = db.strIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}

	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, ErrKeyNotFound
	}
	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// In KeyOnlyMemMode, the value not in memory, so get the value from log file at the offset.
	logFile := db.getActiveLogFile(dataType)
	if logFile.Fid != idxNode.fid {
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}

	ent, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	// key exists, but is invalid(deleted or expired)
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		return nil, ErrKeyNotFound
	}
	return ent.Value, nil
}
