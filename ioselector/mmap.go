package ioselector

import (
	"github.com/flower-corp/rosedb/mmap"
	"io"
	"os"
)

// MMapSelector represents using memory-mapped file I/O.
type MMapSelector struct {
	fd     *os.File
	buf    []byte // a buffer of mmap
	bufLen int64
}

// NewMMapSelector create a new mmap selector.
func NewMMapSelector(fName string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, fsize)
	if err != nil {
		return nil, err
	}

	return &MMapSelector{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

// Write copy slice b into mapped region(buf) at offset.
func (lm *MMapSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || length+offset > lm.bufLen {
		return 0, io.EOF
	}
	return copy(lm.buf[offset:], b), nil
}

// Read copy data from mapped region(buf) into slice b at offset.
func (lm *MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= lm.bufLen {
		return 0, io.EOF
	}
	return copy(b, lm.buf[offset:]), nil
}

// Sync synchronize the mapped buffer to the file's contents on disk.
func (lm *MMapSelector) Sync() error {
	return mmap.Msync(lm.buf)
}

// Close sync/unmap mapped buffer and close fd.
func (lm *MMapSelector) Close() error {
	if err := mmap.Msync(lm.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	return lm.fd.Close()
}

// Delete delete mapped buffer and remove file on disk.
func (lm *MMapSelector) Delete() error {
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	lm.buf = nil

	if err := lm.fd.Truncate(0); err != nil {
		return err
	}
	if err := lm.fd.Close(); err != nil {
		return err
	}
	return os.Remove(lm.fd.Name())
}
