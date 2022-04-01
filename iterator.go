package rosedb

type IteratorOptions struct {
	PrefetchSize int
	Reverse      bool
}

type Iterator struct {
}

func (it *Iterator) Rewind() {
}

func (it *Iterator) Valid() bool {
	return false
}

func (it *Iterator) Seek(key []byte) {
}

func (it *Iterator) Next() {
}

func (it *Iterator) Key() []byte {
	return nil
}

func (it *Iterator) Value() []byte {
	return nil
}
