# Release 2.2.2(2023-08-05)
## 🚀 New Features
* Watch Key [feature support watch event by key #227](https://github.com/rosedblabs/rosedb/issues/227) @Jeremy-Run 

## 🎄 Enhancements

* Batch Optimiztion [use sync.Pool to optimize db.Put operation #235](https://github.com/rosedblabs/rosedb/issues/235)
* Optimize memory usage [enhancement: high memory usage of rosedb #236](https://github.com/rosedblabs/rosedb/issues/236)

## 🎠 Community
* Thanks to @kebukeYi 
    * Change Variable name in openMergeDB (https://github.com/rosedblabs/rosedb/pull/228)
    * Avoid parsing wal files repeatedly. (https://github.com/rosedblabs/rosedb/pull/229)
* Thanks to @Jeremy-Run 
    * Deleted data cannot exist in the index (https://github.com/rosedblabs/rosedb/pull/232)
    * fix: solve data race (https://github.com/rosedblabs/rosedb/pull/234)
    * fix: destFile may be not exist (https://github.com/rosedblabs/rosedb/pull/243)
* Thanks to @rfyiamcool 
    * fix: format code comment for rand_kv (https://github.com/rosedblabs/rosedb/pull/240)

# Release 2.2.1(2023-07-03)

## 🎠 Community
* Thanks to @rfyiamcool for PR
  * feature: Add rollback function to discard all buffered data and release the lock([#217](https://github.com/rosedblabs/rosedb/pull/217))
  * fix: clear db after benchmark ([#224](https://github.com/rosedblabs/rosedb/pull/224))


# Release 2.2.0(2023-06-21)

## 🚀 New Features
* Support Merge operation, to reclaim disk space.
  * `Merge` will rewrite all the valid data into new file, and delete the old files.
  * It maybe a very time-consuming operation, so it is recommended to use it when the database is idle.
* Add tests in windows, with worlflow.

# Release 2.1.0(2023-06-15)

## 🚀 New Features

* Support iterator in rosedb, it can traverse the data in database in order.
  And the methods are as follows:

  * Rewind
  * Seek
  * Next
  * Key
  * Value
  * Close

And the prefix scan is also supported.

## 🐞Bug Fix

* Thanks to @rfyiamcool for PR
  * [#216](https://github.com/rosedblabs/rosedb/pull/216) fix: update committed flag after batch commit

# Release 2.0.0(2023-06-13)

## 🚀 New Features
* Basic operations, `Put/Get/Delete/Exist` key value pairs.
* Batch operations, `Put/Get/Delete/Exist` key value pairs, and `Commit`.
* DB functions, `Open/Close/Sync/Stat`.
