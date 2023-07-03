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
