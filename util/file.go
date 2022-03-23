package util

import (
	"io"
	"io/ioutil"
	"os"
	"path"
)

// PathExist check if the directory or file exists.
func PathExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// CopyDir copy directory from src to dst.
func CopyDir(src string, dst string) error {
	var (
		err     error
		dir     []os.FileInfo
		srcInfo os.FileInfo
	)

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}
	if dir, err = ioutil.ReadDir(src); err != nil {
		return err
	}

	for _, fd := range dir {
		srcPath := path.Join(src, fd.Name())
		dstPath := path.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = CopyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err = CopyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// CopyFile copy a single file.
func CopyFile(src, dst string) error {
	var (
		err     error
		srcFile *os.File
		dstFie  *os.File
		srcInfo os.FileInfo
	)

	if srcFile, err = os.Open(src); err != nil {
		return err
	}
	defer srcFile.Close()

	if dstFie, err = os.Create(dst); err != nil {
		return err
	}
	defer dstFie.Close()

	if _, err = io.Copy(dstFie, srcFile); err != nil {
		return err
	}

	if srcInfo, err = os.Stat(src); err != nil {
		return err
	}

	return os.Chmod(dst, srcInfo.Mode())
}
