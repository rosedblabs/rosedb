package index

import (
	"github.com/roseduan/rosedb/storage"
)

// Indexer the data index info, a component of data types index.
type Indexer struct {
	Meta   *storage.Meta // metadata info.
	FileId uint32        // the file id of storing the data.
	Offset int64         // entry data query start position.
}
