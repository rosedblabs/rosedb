package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/roseduan/mmap-go"
)

const (
	// FilePerm default permission of the newly created db file.
	FilePerm = 0644

	// PathSeparator the default path separator.
	PathSeparator = string(os.PathSeparator)

	mergeDir = "rosedb_merge"
)

var (
	// DBFileFormatNames name format of the db files.
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	// DBFileSuffixName represents the suffix names of the db files.
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

var (
	// ErrEmptyEntry the entry is empty.
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
	// ErrEmptyEntry the entry is empty.
	ErrEntryTooLarge = errors.New("storage/db_file: entry is too large to store in mmap mode")
)

// FileRWMethod db file read and write method.
type FileRWMethod uint8

const (

	// FileIO Indicates that data file read and write using system standard IO.
	FileIO FileRWMethod = iota

	// MMap Indicates that data file read and write using mmap.
	MMap
)

// DBFile define the data file of rosedb.
type DBFile struct {
	Id     uint32
	Path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

// NewDBFile create a new db file, truncate the file if rw method is mmap.
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, Path: path, Offset: stat.Size(), method: method}

	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

// Read read data from the db file, offset is the start position of reading.
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte

	// read entry header info.
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	// read key if necessary(by the KeySize).
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	// read value if necessary.
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = val
	}

	// read extra info if necessary.
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

// Write write data into db file from offset.
func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	encVal, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileIO {
		if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
			return err
		}
	}
	if method == MMap {
		if writeOff+int64(len(encVal)) > int64(len(df.mmap)) {
			return ErrEntryTooLarge
		}
		copy(df.mmap[writeOff:], encVal)
	}
	df.Offset += int64(e.Size())
	return nil
}

// Close close the db file, sync means whether to persist data before closing.
func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}

// Sync persist db file into disk.
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

// Build load all db files from disk.
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	// build merged files if necessary.
	// merge path is a sub directory in path.
	var (
		mergedFiles map[uint16]map[uint32]*DBFile
		mErr        error
	)
	for _, d := range dir {
		if d.IsDir() && strings.Contains(d.Name(), mergeDir) {
			mergePath := path + string(os.PathSeparator) + d.Name()
			if mergedFiles, _, mErr = Build(mergePath, method, blockSize); mErr != nil {
				return nil, nil, mErr
			}
		}
	}

	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			// find the different types of file.
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// load all the db files.
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0

		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs)-1])

			length := len(fileIDs) - 1
			if strings.Contains(path, mergeDir) {
				length++
			}
			for i := 0; i < length; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}

	// merged files are also archived files.
	if mergedFiles != nil {
		for dType, file := range archFiles {
			if mergedFile, ok := mergedFiles[dType]; ok {
				for id, f := range mergedFile {
					file[id] = f
				}
			}
		}
	}
	return archFiles, activeFileIds, nil
}
