![rosedb_ico.png](https://i.loli.net/2021/04/28/gIL2FXZcOesPmyD.png)

![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)[![codecov](https://codecov.io/gh/roseduan/rosedb/branch/main/graph/badge.svg?token=YZUB9QT6XF)](https://codecov.io/gh/roseduan/rosedb) [![CodeFactor](https://www.codefactor.io/repository/github/roseduan/rosedb/badge)](https://www.codefactor.io/repository/github/roseduan/rosedb) [![Go Reference](https://pkg.go.dev/badge/github.com/roseduan/rosedb.svg)](https://pkg.go.dev/github.com/roseduan/rosedb) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database) 


[English](https://github.com/roseduan/rosedb#rosedb) | [简体中文](https://github.com/roseduan/rosedb/blob/main/README.md)

### **rosedb 暂时不能用于生产环境，正在完善测试中！**

***

rosedb 是一个稳定、高性能、快速、内嵌的 k-v 存储引擎，支持多种数据结构，包含 `String`、`List`、`Hash`、`Set`、`Sorted Set`，接口名称风格和 Redis 类似。

rosedb 基于简单的 bitcask 模型，数据文件布局类似 LSM Tree 中的 WAL 日志，纯 `Golang` 实现，易于理解和使用。

我们的愿景是打造一个高效的 k-v 存储引擎，你可以给我们提任何建议，也请给我们一个 star ✨ 哦，非常感谢！

 ![Stargazers over time](https://starchart.cc/roseduan/rosedb.svg)

## 特性

* 支持丰富的数据结构：字符串、列表、哈希表、集合、有序集合。
* 内嵌使用简单至极，无需任何安装部署（`import "github.com/roseduan/rosedb"`）。
* 低延迟、高吞吐（具体请见英文 README 的 Benchmark）。
* 不同数据类型的操作可以完全并行。
* 支持客户端命令行操作。
* 支持过期时间。
* `String` 数据类型支持前缀和范围扫描。
* 支持简单的事务操作，ACID 特性。
* 数据文件 merge 可手动停止。

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
docker build -t="rosedb:v1.2.9" .
docker run --name=rosedb -itd -p 5200:5200 rosedb:v1.2.9
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
* HVals

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

可以参考这里：[待办](https://github.com/roseduan/rosedb/projects/1)。

## 教程

我在 B 站录制了这个项目的视频，你可以跟着视频来学习这个项目。

[使用 Go 写一个数据库—1 基本结构](https://www.bilibili.com/video/BV1aZ4y1w7Uz)

[使用 Go 写一个数据库—2 基本数据操作](https://www.bilibili.com/video/BV15p4y1a7kD?spm_id_from=333.788.b_636f6d6d656e74.30)

[使用 Go 写一个数据库—3 数据库操作](https://www.bilibili.com/video/BV1qb4y1D7dG?spm_id_from=333.788.b_636f6d6d656e74.31)

[使用 Go 写一个数据库—4 数据结构](https://www.bilibili.com/video/BV18V411J7Dr?spm_id_from=333.788.b_636f6d6d656e74.32)

[使用 Go 写一个数据库—5 命令行](https://www.bilibili.com/video/BV1gN411f7SR?spm_id_from=333.788.b_636f6d6d656e74.33)

[使用 Go 写一个数据库—6 完结撒花](https://www.bilibili.com/video/BV1VQ4y1o7AV/?spm_id_from=333.788.recommend_more_video.5)

**做为一个个人开源项目，rosedb 还有很多不完善的地方，期待你提出宝贵的意见和建议，感兴趣的话也非常欢迎参与到 rosedb 的开发中！**

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

