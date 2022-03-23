package flock

import (
	"fmt"
	"os"
)

// FileLockGuard holds a lock of file on a directory.
type FileLockGuard struct {
	// file descirptor on directory.
	fd *os.File
}

// AcquireFileLock acquire the lock on the directory by syscall.Flock.
// Return a FileLockGuard or an error, if any.
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	var (
		flag int
		mode os.FileMode
	)
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
		mode = os.ModeExclusive
	}

	file, err := os.OpenFile(path, flag, mode)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, mode|0644)
	}
	if err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil
}

// SyncDir commits the current contents of the directory to stable storage.
func SyncDir(path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	err = fd.Sync()
	closeErr := fd.Close()
	if err != nil {
		return fmt.Errorf("sync dir err: %+v", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close dir err: %+v", err)
	}
	return nil
}

// Release release the file lock.
func (fl *FileLockGuard) Release() error {
	return fl.fd.Close()
}
