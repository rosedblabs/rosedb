package rosedb

import "errors"

var (
	ErrKeyIsEmpty      = errors.New("the key is empty")
	ErrKeyNotFound     = errors.New("key not found in database")
	ErrDatabaseIsUsing = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch   = errors.New("the batch is read only")
	ErrBatchCommitted  = errors.New("the batch is committed")
	ErrBatchRollbacked = errors.New("the batch is rollbacked")
	ErrDBClosed        = errors.New("the database is closed")
	ErrMergeRunning    = errors.New("the merge operation is running")
)
