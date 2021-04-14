# rosedb ![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)

[English](https://github.com/roseduan/rosedb#rosedb) | [简体中文](https://github.com/roseduan/rosedb/blob/main/README.md)

rosedb is an embedded k-v database based on LSM+WAL, so it has good write performance and high throughput. It also supports many kinds of data structures such as `string`, `list`, `hash`, `set`, `zset`，and the API name style is similar to Redis.

rosedb is in pure `Go`, simple and easy to understand for use or learning.

## Feature

* Support rich data structure :  `string`, `list`, `hash`, `set`, `zset`.
* Support expiration and TTL.
* Has builtin rosedb-cli for command line use.
* Easy to embedded (`import "github.com/roseduan/rosedb"`).
* Low latency and high throughput.

## Usage

### Cli example



### Embedded example



## TODO

+ [x] Support expiration and TTL
+ [ ] Support transaction, ACID features
+ [ ] Compress the written data
+ [x] Add prefix scan and range scan for string type
+ [ ] Add elimination strategy (LRU)
+ [x] Cli for command line use.
+ [ ] Improve related documents

## License

rosedb is licensed under the term of the [MIT License](https://github.com/roseduan/rosedb/blob/main/LICENSE)

