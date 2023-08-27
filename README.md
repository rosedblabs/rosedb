<div align="center">
<strong>
<samp>

[English](https://github.com/rosedblabs/rosedb/blob/main/README.md) · [简体中文](https://github.com/rosedblabs/rosedb/blob/main/README-CN.md)

</samp>
</strong>
</div>

## What is ROSEDB

rosedb is a lightweight, fast and reliable key/value storage engine based on [Bitcask](https://riak.com/assets/bitcask-intro.pdf) storage model.

The design of Bitcask was inspired, in part, by log-structured filesystems and log file merging.

## Status
rosedb is well tested and ready for production use. There are serveral projects using rosedb in production as a storage engine.

**Didn't find the feature you want? Feel free to open an issue or PR, we are in active development.**

## Design overview

![](https://github.com/rosedblabs/rosedb/blob/main/docs/imgs/design-overview-rosedb.png)

RoseDB log files are using the WAL(Write Ahead Log) as backend, which are append-only files with block cache.

> wal: https://github.com/rosedblabs/wal

## Key features

### Strengths

<details>
    <summary><b>Low latency per item read or written</b></summary>
    This is due to the write-once, append-only nature of Bitcask database files.
</details>

<details>
    <summary><b>High throughput, especially when writing an incoming stream of random items</b></summary>
    Write operations to RoseDB generally saturate I/O and disk bandwidth, which is a good thing from a performance perspective. This saturation occurs for two reasons: because (1) data that is written to RoseDB doesn't need to be ordered on disk, and (2) the log-structured design of Bitcask allows for minimal disk head movement during writes.
</details>    

<details>
    <summary><b>Ability to handle datasets larger than RAM without degradation</b></summary>
    Access to data in RoseDB involves direct lookup from an in-memory index data structure. This makes finding data very efficient, even when datasets are very large.
</details>

<details>
    <summary><b>Single seek to retrieve any value</b></summary>
    RoseDB's in-memory index data structure of keys points directly to locations on disk where the data lives. RoseDB never uses more than one disk seek to read a value and sometimes even that isn't necessary due to filesystem caching done by the operating system.
</details>

<details>
    <summary><b>Predictable lookup and insert performance</b></summary>
    For the reasons listed above, read operations from RoseDB have fixed, predictable behavior. This is also true of writes to RoseDB because write operations require, at most, one seek to the end of the current open file followed by and append to that file.
</details>

<details>
    <summary><b>Fast, bounded crash recovery</b></summary>
    Crash recovery is easy and fast with RoseDB because RoseDB files are append only and write once. The only items that may be lost are partially written records at the tail of the last file that was opened for writes. Recovery operations need to review the record and verify CRC data to ensure that the data is consistent.
</details>

<details>
    <summary><b>Easy Backup</b></summary>
    In most systems, backup can be very complicated. RoseDB simplifies this process due to its append-only, write-once disk format. Any utility that archives or copies files in disk-block order will properly back up or copy a RoseDB database.
</details>

<details>
    <summary><b>Batch options which guarantee atomicity, consistency, and durability</b></summary>
	RoseDB supports batch operations which are atomic, consistent, and durable. The new writes in batch are cached in memory before committing. If the batch is committed successfully, all the writes in the batch will be persisted to disk. If the batch fails, all the writes in the batch will be discarded.
</details>

<details>
    <summary><b>Support iterator for forward and backward</b></summary>
	RoseDB supports iterator for forward and backward. The iterator is based on the in-memory index data structure of keys, which points directly to locations on disk where the data lives. The iterator is very efficient, even when datasets are very large.
</details>

<details>
    <summary><b>Support key watch</b></summary>
	RoseDB supports key watch, you can get the notification if keys changed in db.
</details>

<details>
    <summary><b>Support key expire</b></summary>
	RoseDB supports key expire, you can set the expire time for keys.
</details>

### Weaknesses

<details>
    <summary><b>Keys must fit in memory</b></summary>
    RoseDB keeps all keys in memory at all times, which means that your system must have enough memory to contain your entire keyspace, plus additional space for other operational components and operating- system-resident filesystem buffer space.
</details>

## Gettings Started

### Basic operations

```go
package main

import "github.com/rosedblabs/rosedb/v2"

func main() {
	// specify the options
	options := rosedb.DefaultOptions
	options.DirPath = "/tmp/rosedb_basic"

	// open a database
	db, err := rosedb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// set a key
	err = db.Put([]byte("name"), []byte("rosedb"))
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```

### Batch operations

```go
	// create a batch
	batch := db.NewBatch(rosedb.DefaultBatchOptions)

	// set a key
	_ = batch.Put([]byte("name"), []byte("rosedb"))

	// get a key
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// delete a key
	_ = batch.Delete([]byte("name"))

	// commit the batch
	_ = batch.Commit()
```

see the [examples](https://github.com/rosedblabs/rosedb/tree/main/examples) for more details.

## Community
Welcome to join the [Slack](https://join.slack.com/t/rosedblabs/shared_invite/zt-19oj8ecqb-V02ycMV0BH1~Tn6tfeTz6A) channel and [Discussions](https://github.com/orgs/rosedblabs/discussions) to connect with RoseDB team developers and other users.

## Contributors
[![](https://opencollective.com/rosedb/contributors.svg?width=890&button=false)](https://github.com/rosedblabs/rosedb/graphs/contributors)
