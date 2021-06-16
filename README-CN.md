![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/roseduan/rosedb/branch/main/graph/badge.svg?token=YZUB9QT6XF)](https://codecov.io/gh/roseduan/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/roseduan/rosedb/badge)](https://www.codefactor.io/repository/github/roseduan/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) 


[English](https://github.com/roseduan/rosedb#rosedb) | [简体中文](https://github.com/roseduan/rosedb/blob/main/README.md)

rosedb 是一个稳定、高性能、快速、内嵌的 k-v 数据库，支持多种数据结构，包含 `String`、`List`、`Hash`、`Set`、`Sorted Set`，接口名称风格和 Redis 类似。

rosedb 基于简单的 bitcask 模型，数据文件布局类似 LSM Tree 中的 WAL 日志，纯 `Golang` 实现，易于使用、扩展。

我们的愿景是打造一个高效的 k-v 存储引擎，你可以给我们提任何建议，也请给我们一个 start ✨ 吧，非常感谢！

 ![Stargazers over time](https://starchart.cc/roseduan/rosedb.svg)

## 特性

* 支持丰富的数据结构：字符串、列表、哈希表、集合、有序集合。
* 内嵌使用简单至极，无需任何安装部署（`import "github.com/roseduan/rosedb"`）。
* 低延迟、高吞吐（具体请见英文 README 的 Benchmark）。
* 不同数据类型的操作可以完全并行。
* 支持客户端命令行操作。
* 支持过期时间。
* `String` 数据类型支持前缀和范围扫描。

## 介绍

一个 rosedb 实例，其实就是系统上的一个文件夹，在这个文件夹中，除了一些配置外，最主要的便是数据文件。一个实例中，只会存在一个活跃的数据文件进行写操作，如果这个文件的大小达到了设置的上限，那么这个文件会被关闭，然后创建一个新的活跃文件。

其余的文件，称之为已归档文件，这些文件都是已经被关闭，不能在上面进行写操作，但是可以进行读操作。

所以整个数据库实例就是当前活跃文件、已归档文件和其他配置的一个集合：

![db_instance.png](https://i.loli.net/2021/03/14/2WpobcYO43x1FHR.png)

在每一个文件中，写数据的操作只会追加到文件的末尾，这保证了写操作不会进行额外的磁盘寻址。写入的数据是以一个个被称为 Entry 的结构组织起来的，Entry 的主要数据结构如下：

![entry.png](https://i.loli.net/2021/03/14/cVIGPa14feKloJ2.png)

因此一个数据文件可以看做是多个 Entry 的集合：

![db_file.png](https://i.loli.net/2021/03/14/f3KOnNgEhmbetxa.png)

所有的写入、删除、更新操作，都会被封装成一个 Entry，追加到数据文件的末尾。一个 key 可能会被多次更新，或者被删除，因此数据文件当中可能存在冗余的 Entry 数据。在这种情况下，我们需要合并数据文件，来清除冗余的 Entry 数据，回收磁盘空间。

## 使用

### 命令行操作

切换目录到 `rosedb/cmd/server`

运行 server 目录下的 `main.go`

![Xnip2021-04-14_14-33-11.png](https://i.loli.net/2021/04/14/EsMFv48YB3P9j7k.png)

打开一个新的窗口，切换目录到 `rosedb/cmd/cli`

运行目录下的 `main.go`

![Xnip2021-04-14_14-35-50.png](https://i.loli.net/2021/04/14/9uh1ElVF3C4D6dM.png)

也可以直接使用 redis-cli:

![2021-05-14 上午11.19.24.png](https://i.loli.net/2021/05/14/eYkMyTzl5CXUN83.png)

### 内嵌使用

在项目中导入 rosedb：

```go
import "github.com/roseduan/rosedb"
```

然后打开一个数据库并执行相应的操作：

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
	
  // 别忘记关闭数据库哦！
	defer db.Close()
	
	//...
}
```

### 在Docker上部署与运行

```shell
docker build -t="rosedb:v1.2.7" .
docker run --name=rosedb -itd -p 5200:5200 rosedb:v1.2.7
docker exec -it rosedb sh

$ rosedb-cli
127.0.0.1:5200>set hello rosedb
OK
127.0.0.1:5200>get hello
rosedb
```

## 支持的命令

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
* LKeyExists
* LValExists

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
* ZRangeWithScores
* ZRevRange
* ZRevRangeWithScores
* ZRem
* ZGetByRank
* ZRevGetByRank
* ZScoreRange
* ZRevScoreRange

## 待办

这个项目其实还有很多可以完善的地方，比如下面列举到的一些，如果你对这个项目比较熟悉了，可以挑选一个自己感兴趣的 Todo List，自己去实现，然后提 Pr，成为这个项目的 Contributor。

+ [x] 支持 TTL
+ [x] String 类型 key 加入前缀扫描
+ [x] 写一个简单的客户端，支持命令行操作
+ [ ] 数据库启动优化
+ [ ] reclaim 性能优化
+ [ ] 支持事务，ACID 特性
+ [ ] 文件数据压缩存储（snappy、zstd、zlib）
+ [ ] 缓存淘汰策略（LRU、LFU、Random）
+ [ ] 支持更多的命令操作（type，keys，mset，mget，zcount，etc...）
+ [ ] 完善相关文档

## 教程

我在 B 站录制了这个项目的视频，你可以跟着视频来学习这个项目，期待你给这个项目提出宝贵的意见和建议！

[使用 Go 写一个数据库—1 基本结构](https://www.bilibili.com/video/BV1aZ4y1w7Uz)

[使用 Go 写一个数据库—2 基本数据操作](https://www.bilibili.com/video/BV15p4y1a7kD?spm_id_from=333.788.b_636f6d6d656e74.30)

[使用 Go 写一个数据库—3 数据库操作](https://www.bilibili.com/video/BV1qb4y1D7dG?spm_id_from=333.788.b_636f6d6d656e74.31)

[使用 Go 写一个数据库—4 数据结构](https://www.bilibili.com/video/BV18V411J7Dr?spm_id_from=333.788.b_636f6d6d656e74.32)

[使用 Go 写一个数据库—5 命令行](https://www.bilibili.com/video/BV1gN411f7SR?spm_id_from=333.788.b_636f6d6d656e74.33)

[使用 Go 写一个数据库—6 完结撒花](https://www.bilibili.com/video/BV1VQ4y1o7AV/?spm_id_from=333.788.recommend_more_video.5)

## 参与贡献

感谢你的参与，你可以给这个项目：

* 提 bug 或者 issue
* 关于代码，性能各方面的建议
* 参与进来，完善功能

完整的步骤及规范，请参考：[CONTRIBUTING](https://github.com/roseduan/rosedb/blob/main/CONTRIBUTING.md)

## 联系我

欢迎加我微信，拉你进 rosedb 项目交流群，和大牛一起交流学习。

| <img src="https://i.loli.net/2021/05/06/tGTH7SXg8w95slA.jpg" width="200px" align="left"/> |
| ------------------------------------------------------------ |
| 添加时请备注【Github】哦。                                   |

## License

rosedb 根据 MIT License 许可证授权，有关完整许可证文本，请参阅 [LICENSE](https://github.com/roseduan/rosedb/blob/main/LICENSE)。

