# Release 2.3.4(2024-01-07)

## 🎄 Enhancements
* use wal write batch to optimize performance.
* optimize memory usage.

## 🎠 Community
* Thanks to @LindaSummer
  * add auto merge(https://github.com/rosedblabs/rosedb/commit/f31d45ef0cc3e738bbfe547df41fdfc23817bc4a)
* Thanks to @justforward
  * clarify file error(https://github.com/rosedblabs/rosedb/commit/b00612621aa9c27e79b4a012b53f5f1af1dd41bd)
* Thanks to @lyonzhi
  * approce test case for windows(https://github.com/rosedblabs/rosedb/commit/7d8c6c0e09bd556b65f11b37eca12cfdcb81b567)
* Thanks to @246859
  * fix(watch): make channnel that DB.Watch returns is readonly (https://github.com/rosedblabs/rosedb/pull/294)

# Release 2.3.3(2023-09-16)
## 🚀 New Features
* add filterExpired for ascend/descend keys
* Add persist function to remove the TTL of the key

# Release 2.3.2(2023-08-30)
## 🚀 New Features
* add AscendKeys and DescnedKeys
* Add Expire and TTL functions (https://github.com/rosedblabs/rosedb/pull/278)

## 🎄 Enhancements
* fix expire bug and add examples
* add iterate examples

## 🎠 Community
* Thanks to @Jeremy-Run 
    * Delete expired key of the index (https://github.com/rosedblabs/rosedb/pull/269)
    * New: Delete Expired Keys (https://github.com/rosedblabs/rosedb/pull/280)
* Thanks to @LEAVING-7 
    * Fix potential deadlock in merge.go (https://github.com/rosedblabs/rosedb/pull/279)

## 🐞 Bug Fixes
* fix reput ttl bug

# Release 2.3.1(2023-08-21)
## 🚀 New Features
* Support key expire
  * You can call `PutWithTTL` to set the expire time for a key.

## 🎠 Community
* Thanks to @weijiew 
    * Add more BTree functions #264

# Release 2.3.0(2023-08-18)
## 🚀 New Features
* use BTree as the default memory data structure.
  * the old Radix will be removed, and the iterator too.

## 🎠 Community
* Thanks to @Jeremy-Run 
    * remove merge file after tests (https://github.com/rosedblabs/rosedb/pull/250)
    * replace original file and rebuilt index after merge (https://github.com/rosedblabs/rosedb/pull/255)
* Thanks to @SYaoJun 
    * fix: single quote error in README (https://github.com/rosedblabs/rosedb/pull/256)
* Thanks to @weijiew 
    * add btree Ascend、Descend method and unitest. (https://github.com/rosedblabs/rosedb/pull/257)

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
