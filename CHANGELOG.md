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
