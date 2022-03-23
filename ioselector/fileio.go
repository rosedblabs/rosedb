package ioselector

import "os"

// FileIOSelector represents using standard file I/O.
type FileIOSelector struct {
	fd *os.File // system file descriptor.
}

// NewFileIOSelector create a new file io selector.
func NewFileIOSelector(fName string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, nil
}

// Write is a wrapper of os.File WriteAt.
func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

// Read is a wrapper of os.File ReadAt.
func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Sync is a wrapper of os.File Sync.
func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

// Close is a wrapper of os.File Close.
func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

// Delete file descriptor if we don`t use it anymore.
func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
