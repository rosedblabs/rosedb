package rosedb

import (
	"time"

	"github.com/spf13/afero"
)

type DBReadOnly interface {
	Close() error
	Fs() afero.Fs
	Stat() *Stat
	Get(key []byte) ([]byte, error)
	Exist(key []byte) (bool, error)
	Ascend(handleFn func(k []byte, v []byte) (bool, error))
	AscendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error))
	AscendGreaterOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error))
	AscendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error))
	Descend(handleFn func(k []byte, v []byte) (bool, error))
	DescendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error))
	DescendLessOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error))
	DescendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error))
}
type DB interface {
	DBReadOnly
	Sync() error
	Watch() (<-chan *Event, error)
	Put(key []byte, value []byte) error
	PutWithTTL(key []byte, value []byte, ttl time.Duration) error
	Delete(key []byte) error
	Expire(key []byte, ttl time.Duration) error
	TTL(key []byte) (time.Duration, error)
	Persist(key []byte) error
	DeleteExpiredKeys(timeout time.Duration) error

	NewBatch(options BatchOptions) *Batch

	Merge(reopenAfterDone bool) error
}

type Lock interface {
	TryLock() (bool, error)
	Unlock() error
}
