# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).

## [1.1.0] - 2022-06-16

### 🚀 Features

* Support `Redis` protocol(not fully)
  * see [supported commands in Redis](https://github.com/flower-corp/rosedb/wiki/Commands)
* Add more `Hash`, `List`, and `Set` commands
* Fix unit tests fit for `Windows`
* Add CI and an issue template

## [1.0.0] - 2022-05-08

### 🚀 Features

- Support `String`, `List`, ` Hash`, ` Set`, and `ZSet`
  - see [supported commands in Redis](https://github.com/flower-corp/rosedb/wiki/Commands)

- Logfile garbage collection automatically
  - see option `LogFileGCRatio` and `LogFileGCInterval`

- Support standard `FileIO` and `MMap`
- Support different index mode
  - `KeyOnlyMemMode`: only store keys in memory and values are in disk
  - `KeyValueMemMode`: both keys and values will store in memory
