package logfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/flower-corp/rosedb/ioselector"
)

var (
	// ErrInvalidCrc invalid crc.
	ErrInvalidCrc = errors.New("logfile: invalid crc")

	// ErrWriteSizeNotEqual write size is not equal to entry size.
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	// ErrEndOfEntry end of entry in log file.
	ErrEndOfEntry = errors.New("logfile: end of entry in log file")

	// ErrUnsupportedIoType unsupported io type, only mmap and fileIO now.
	ErrUnsupportedIoType = errors.New("unsupported io type")

	// ErrUnsupportedLogFileType unsupported log file type, only WAL and ValueLog now.
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	// InitialLogFileId initial log file id: 0.
	InitialLogFileId = 0

	// FilePrefix log file prefix.
	FilePrefix = "log."
)

// FileType represents different types of log file: wal and value log.
type FileType int8

const (
	Strs FileType = iota
	List
	Hash
	Sets
	ZSet
)

var (
	FileNamesMap = map[FileType]string{
		Strs: "log.strs.",
		List: "log.list.",
		Hash: "log.hash.",
		Sets: "log.sets.",
		ZSet: "log.zset.",
	}

	// FileTypesMap name->type
	FileTypesMap = map[string]FileType{
		"strs": Strs,
		"list": List,
		"hash": Hash,
		"sets": Sets,
		"zset": ZSet,
	}
)

// IOType represents different types of file io: FileIO(standard file io) and MMap(Memory Map).
type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

// LogFile is an abstraction of a disk file, entry`s read and write will go through it.
type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IoSelector ioselector.IOSelector
}

// OpenLogFile open an existing or create a new log file.
// fsize must be a postitive number.And we will create io selector according to ioType.
func OpenLogFile(path string, fid uint32, fsize int64, ftype FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	fileName, err := lf.getLogFileName(path, fid, ftype)
	if err != nil {
		return nil, err
	}

	var selector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fsize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIoType
	}

	lf.IoSelector = selector
	return
}

// ReadLogEntry read a LogEntry from log file at offset.
// It returns a LogEntry, entry size and an error, if any.
// If offset is invalid, the err is io.EOF.
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// read entry header.
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, size := decodeHeader(headerBuf)
	// the end of entries.
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &LogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// read entry key and value.
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}

	// crc32 check.
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

// Read a byte slice in the log file at offset, slice length is the given size.
// It returns the byte slice and error, if any.
func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IoSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// Write a byte slice at the end of log file.
// Returns an error, if any.
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

// Sync commits the current contents of the log file to stable storage.
func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

// Close current log file.
func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

// Delete delete current log file.
// File can`t be retrieved if do this, so use it carefully.
func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}

func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

func (lf *LogFile) getLogFileName(path string, fid uint32, ftype FileType) (name string, err error) {
	if _, ok := FileNamesMap[ftype]; !ok {
		return "", ErrUnsupportedLogFileType
	}

	fname := FileNamesMap[ftype] + fmt.Sprintf("%09d", fid)
	name = filepath.Join(path, fname)
	return
}
