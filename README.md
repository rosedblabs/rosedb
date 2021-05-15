![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/roseduan/rosedb/branch/main/graph/badge.svg?token=YZUB9QT6XF)](https://codecov.io/gh/roseduan/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/roseduan/rosedb/badge)](https://www.codefactor.io/repository/github/roseduan/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) 

[English](https://github.com/roseduan/rosedb#rosedb) | [简体中文](https://github.com/roseduan/rosedb/blob/main/README-CN.md)

rosedb is an embedded k-v database based on LSM+WAL, so it has good write performance and high throughput. It also supports many kinds of data structures such as `string`, `list`, `hash`, `set`, `zset`，and the API name style is similar to Redis.

rosedb is in pure `Go`, simple and easy to understand for using or learning.

## Feature

* Support rich data structure :  `string`, `list`, `hash`, `set`, `zset`.
* Support expiration and TTL.
* Has builtin rosedb-cli for command line.
* Easy to embedded (`import "github.com/roseduan/rosedb"`).
* Low latency and high throughput.

## Usage

### Cli example

Change the directory to rosedb/cmd/server.

Run the `main.go`

![Xnip2021-04-14_14-33-11.png](https://i.loli.net/2021/04/14/EsMFv48YB3P9j7k.png)

Open a new shell, and change the directory to rosedb/cmd/cli, and run the `main.go`：

![Xnip2021-04-14_14-35-50.png](https://i.loli.net/2021/04/14/9uh1ElVF3C4D6dM.png)

### Embedded example

Import rosedb in the application:

```go
import "github.com/roseduan/rosedb"
```

And open a database:

```go
package main

import (
	"github.com/roseduan/rosedb"
	"log"
)

func main() {
	config := rosedb.DefaultConfig()
	db, err := rosedb.Open(config)
	
	if err != nil {
		log.Fatal(err)
	}
	
  // don`t forget to close!
	defer db.Close()
	
	//...
}
```

## Command

### String

* Set
* SetNx
* Get
* GetSet
* Append
* StrLen
* StrExists
* StrRem
* PrefixScan
* RangeScan
* Expire
* Persist
* TTL

### List

* LPush
* RPush
* LPop
* RPop
* LIndex
* LRem
* LInsert
* LSet
* LTrim
* LRange
* LLen

### Hash

* HSet
* HSetNx
* HGet
* HGetAll
* HDel
* HExists
* HLen
* HKeys
* HValues

### Set

* SAdd
* SPop
* SIsMember
* SRandMember
* SRem
* SMove
* SCard
* SMembers
* SUnion
* SDiff

### Zset

* ZAdd
* ZScore
* ZCard
* ZRank
* ZRevRank
* ZIncrBy
* ZRange
* ZRevRange
* ZRem
* ZGetByRank
* ZRevGetByRank
* ZScoreRange
* ZRevScoreRange

## TODO

+ [x] Support expiration and TTL
+ [ ] Support transaction, ACID features
+ [ ] Compress the written data
+ [x] Add prefix scan and range scan for string type
+ [ ] Add cache elimination strategy (LRU, LFU, Random)
+ [x] Cli for command line use.
+ [ ] Improve related documents

## Benchmark

### Benchmark Environment

* System: macOS Catalina 10.15.7
* CPU: 2.6GHz 
* Memory: 16 GB 2667 MHz DDR4

### Benchmark Result

**In the case of a specified time duration(3s):** 

```
go test -bench=. -benchtime=3s
badger 2021/05/16 00:02:30 INFO: All 0 tables opened in 0s
badger 2021/05/16 00:02:30 INFO: Discard stats nextEmptySlot: 0
badger 2021/05/16 00:02:30 INFO: Set nextTxnTs to 0
goos: darwin
goarch: amd64
pkg: rosedb-bench
BenchmarkPutValue_BadgerDB-12                     276902             11482 ns/op            1629 B/op         45 allocs/op
BenchmarkGetValue_BadgerDB-12                    2363458              1504 ns/op             457 B/op         11 allocs/op
BenchmarkPutValue_GoLevelDB-12                    844111              4653 ns/op             372 B/op          9 allocs/op
BenchmarkGetValue_GoLevelDB-12                   2043241              1690 ns/op             415 B/op          8 allocs/op
BenchmarkPutValue_Pudge-12                        470827              8316 ns/op             776 B/op         22 allocs/op
BenchmarkGetValue_Pudge-12                       6904564               483 ns/op             125 B/op          5 allocs/op
BenchmarkPutValue_RoseDB_KeyValRam-12             901753              4550 ns/op             565 B/op         10 allocs/op
BenchmarkGetValue_RoseDB_KeyValRam-12            7288071               466 ns/op              56 B/op          3 allocs/op
BenchmarkPutValue_RoseDB_KeyOnlyRam-12            963763              4198 ns/op             565 B/op         10 allocs/op
BenchmarkGetValue_RoseDB_KeyOnlyRam-12           1866518              1659 ns/op             188 B/op          5 allocs/op
PASS
ok      rosedb-bench    59.091s

```

**In the case of a specified execute times(200w):**

```
go test -bench=. -benchtime=2000000x
badger 2021/05/16 00:09:59 INFO: All 0 tables opened in 0s
badger 2021/05/16 00:09:59 INFO: Discard stats nextEmptySlot: 0
badger 2021/05/16 00:09:59 INFO: Set nextTxnTs to 0
goos: darwin
goarch: amd64
pkg: rosedb-bench
BenchmarkPutValue_BadgerDB-12                    2000000             11667 ns/op            2108 B/op         46 allocs/op
BenchmarkGetValue_BadgerDB-12                    2000000              4127 ns/op            1212 B/op         20 allocs/op
BenchmarkPutValue_GoLevelDB-12                   2000000              4593 ns/op             341 B/op          9 allocs/op
BenchmarkGetValue_GoLevelDB-12                   2000000              2855 ns/op             972 B/op         15 allocs/op
BenchmarkPutValue_Pudge-12                       2000000              8973 ns/op             788 B/op         22 allocs/op
BenchmarkGetValue_Pudge-12                       2000000              1258 ns/op             200 B/op          6 allocs/op
BenchmarkPutValue_RoseDB_KeyValRam-12            2000000              4440 ns/op             566 B/op         11 allocs/op
BenchmarkGetValue_RoseDB_KeyValRam-12            2000000               508 ns/op              56 B/op          3 allocs/op
BenchmarkPutValue_RoseDB_KeyOnlyRam-12           2000000              4258 ns/op             566 B/op         11 allocs/op
BenchmarkGetValue_RoseDB_KeyOnlyRam-12           2000000              2980 ns/op             312 B/op          8 allocs/op
PASS
ok      rosedb-bench    94.468s

```

### Benchmark Conclusion



## Contributing

If you are intrested in contributing to rosedb, please see here: [CONTRIBUTING](https://github.com/roseduan/rosedb/blob/main/CONTRIBUTING.md)

## Contact me

If you have any questions, you can contact me by email: roseduan520@gmail.com

## License

rosedb is licensed under the term of the [MIT License](https://github.com/roseduan/rosedb/blob/main/LICENSE)

