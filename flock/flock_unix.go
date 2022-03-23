// +build !windows,!plan9

package flock

import (
	"fmt"
	"os"
	"syscall"
)

// FileLockGuard holds a lock of file on a directory.
type FileLockGuard struct {
	// file descirptor on directory.
	fd *os.File
}

// AcquireFileLock acquire the lock on the directory by syscall.Flock.
// Return a FileLockGuard or an error, if any.
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	var flag = os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	file, err := os.OpenFile(path, flag, 0)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return nil, err
	}

	var how = syscall.LOCK_EX | syscall.LOCK_NB
	if readOnly {
		how = syscall.LOCK_SH | syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), how); err != nil {
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
	how := syscall.LOCK_UN | syscall.LOCK_NB
	if err := syscall.Flock(int(fl.fd.Fd()), how); err != nil {
		return err
	}
	return fl.fd.Close()
}
