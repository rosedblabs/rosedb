package index

//数据索引定义
type Indexer struct {
	Key    []byte
	Value  []byte
	Size   uint32
	Offset int64
	FileId uint8
}
