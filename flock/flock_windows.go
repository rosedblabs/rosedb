// +build windows

package flock

import (
	"syscall"
)

// FileLockGuard holds a lock of file on a directory.
type FileLockGuard struct {
	// file descirptor on directory.
	fd syscall.Handle
}

// AcquireFileLock acquire the lock on the directory by syscall.Flock.
// Return a FileLockGuard or an error, if any.
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	var access, mode uint32
	if readOnly {
		access = syscall.GENERIC_READ
		mode = syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE
	} else {
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}

	file, err := syscall.CreateFile(ptr, access, mode, nil,
		syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err == syscall.ERROR_FILE_NOT_FOUND {
		file, err = syscall.CreateFile(ptr, access, mode, nil,
			syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	}
	if err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil
}

// SyncDir do nothing in windows.
func SyncDir(name string) error {
	return nil
}

// Release release the file lock.
func (fl *FileLockGuard) Release() error {
	return syscall.Close(fl.fd)
}
