![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/roseduan/rosedb/branch/main/graph/badge.svg?token=YZUB9QT6XF)](https://codecov.io/gh/roseduan/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/roseduan/rosedb/badge)](https://www.codefactor.io/repository/github/roseduan/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) 


[English](https://github.com/roseduan/rosedb#rosedb) | [简体中文](https://github.com/roseduan/rosedb/blob/main/README.md)

rosedb 是一个简单、内嵌的 k-v 数据库，支持多种数据结构，包含 `String`、`List`、`Hash`、`Set`、`Sorted Set`，接口名称风格和 Redis 类似。

rosedb 基于 LSM Tree 模型和 WAL，纯 `Golang` 实现，易于学习、使用、扩展。

## 介绍

一个 rosedb 实例，其实就是系统上的一个文件夹，在这个文件夹中，除了一些配置外，最主要的便是数据文件。一个实例中，只会存在一个活跃的数据文件进行写操作，如果这个文件的大小达到了设置的上限，那么这个文件会被关闭，然后创建一个新的活跃文件。

其余的文件，我称之为已归档文件，这些文件都是已经被关闭，不能在上面进行写操作，但是可以进行读操作。

所以整个数据库实例就是当前活跃文件、已归档文件、其他配置的一个集合：

![db_instance.png](https://i.loli.net/2021/03/14/2WpobcYO43x1FHR.png)

在每一个文件中，写数据的操作只会追加到文件的末尾，这保证了写操作不会进行额外的磁盘寻址。写入的数据是以一个个被称为 Entry 的结构组织起来的，Entry 的主要数据结构如下：

![entry.png](https://i.loli.net/2021/03/14/cVIGPa14feKloJ2.png)

因此一个数据文件可以看做是多个 Entry 的集合：

![db_file.png](https://i.loli.net/2021/03/14/f3KOnNgEhmbetxa.png)

当写入数据时，如果是 String 类型，为了支持 string 类型的 key 前缀扫描和范围扫描，我将 key 存放到了跳表中，如果是其他类型的数据，则直接存放至对应的数据结构中。然后将 key、value 等信息，封装成 Entry 持久化到数据文件中。

如果是删除操作，那么也会被封装成一个 Entry，标记其是一个删除操作，然后持久化到数据文件中，这样的话就会带来一个问题，数据文件中可能会存在大量的冗余数据，造成不必要的磁盘空间浪费。为了解决这个问题，我写了一个 reclaim 方法，你可以将其理解为对数据文件进行重新整理，使其变得更加的紧凑。

reclaim 方法的执行流程也比较的简单，首先建立一个临时的文件夹，用于存放临时数据文件。然后遍历整个数据库实例中的所有已归档文件，依次遍历数据文件中的每个 Entry，将有效的 Entry 写到新的临时数据文件中，最后将临时文件拷贝为新的数据文件，原数据文件则删除。

这样便使得数据文件的内容更加紧凑，并且去除了无用的 Entry，避免占据额外的磁盘空间。

## 使用

### 命令行操作

切换目录到 `rosedb/cmd/server`

运行 server 目录下的 `main.go`

![Xnip2021-04-14_14-33-11.png](https://i.loli.net/2021/04/14/EsMFv48YB3P9j7k.png)

打开一个新的窗口，切换目录到 `rosedb/cmd/cli`

运行目录下的 `main.go`

![Xnip2021-04-14_14-35-50.png](https://i.loli.net/2021/04/14/9uh1ElVF3C4D6dM.png)

### 内嵌使用

在项目中 import 我的项目：

```go
import "github.com/roseduan/rosedb"
```

然后打开数据库并执行相应的操作：

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

## 待办

这个项目其实还有很多可以完善的地方，比如下面列举到的一些，如果你对这个项目比较熟悉了，可以挑选一个自己感兴趣的 Todo List，自己去实现，然后提 Pr，成为这个项目的 Contributor，我相信这一定会对你有帮助的，赶快行动起来吧！

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

完整的步骤及规范，请参考：

## 联系我

欢迎加我微信，拉你进 rosedb 项目交流群，和大牛一起交流学习。

<img src="https://i.loli.net/2021/05/06/tGTH7SXg8w95slA.jpg" width="200px" align="left"/>

添加时请备注【Github】。

## License

rosedb 根据 MIT License 许可证授权，有关完整许可证文本，请参阅 [LICENSE](https://github.com/roseduan/rosedb/blob/main/LICENSE)。

