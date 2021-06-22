package index

import (
	"github.com/roseduan/rosedb/storage"
)

// Indexer 数据索引定义
// define the data index
type Indexer struct {
	Meta      *storage.Meta // 元数据信息             metadata info
	FileId    uint32        // 存储数据的文件id       the file id of storing the data
	EntrySize uint32        // 数据条目(Entry)的大小  the size of entry
	Offset    int64         // Entry数据的查询起始位置 entry data query start position
}
