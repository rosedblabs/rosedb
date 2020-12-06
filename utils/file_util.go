package utils

import "os"

//文件操作工具

//目录是否存在
func Exist(path string) bool {
	stat, _ := os.Stat(path)
	return stat != nil && stat.IsDir()
}
