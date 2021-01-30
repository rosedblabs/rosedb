# rosedb
rosedb 是一个简单、高效的 k-v 数据库，使用 `Golang` 实现，支持多种数据结构，包含 `String`、`List`、`Hash`、`Set`、`Sorted Set`，接口名称风格和 Redis 类似，如果你对 redis 比较熟悉，那么使用起来会毫无违和感。

## 为什么会做这个项目

大概半年前（2020 年中），我刚开始学习 Go 语言，由于之前有 Java 语言的经验，加上 Go 的基本语法较简单，上手还是很快，但是学完基础的语法知识之后，就不知道下一步应该做什么了。

一个偶然的机会，我在网上看到了一篇介绍数据库模型的文章，文章很简单，理解起来也很容易，加上我对于数据库还是比较感兴趣的，因此想着可以自己实现一个，造个轮子来玩玩，借此巩固自己的一些基础知识。

因此这个项目也是学习并巩固 Go 相关知识的不错的素材，通过实践这个项目，你至少可以学到：

* Golang 大多数基础语法，以及一些高级特性
* 数据结构及算法相关知识，链表，哈希表，跳表等
* 操作系统的一些知识，特别是对文件系统，内存映射相关的

由于个人能力有限，因此欢迎大家提 Issue 和 Pr，一起完善这个项目。

## 介绍

一个 rosedb 实例，其实就是系统上的一个文件夹，在这个文件夹中，除了一些配置外，最主要的便是数据文件。一个实例中，只会存在一个活跃的数据文件进行写操作，如果这个文件的大小达到了设置的上限，那么这个文件会被关闭，然后创建一个新的活跃文件。

其余的文件，我称之为已归档文件，这些文件都是已经被关闭，不能在上面进行写操作，但是可以进行读操作。

所以整个数据库实例就是这样的：

![](https://github.com/roseduan/rosedb/blob/main/resource/img/db_instance.png)

在每一个文件中，写数据的操作只会追加到文件的末尾，这保证了写操作不会进行额外的磁盘寻址。写入的数据是以一个个被称为 Entry 的结构组织起来的，Entry 的主要数据结构如下：

![](https://github.com/roseduan/rosedb/blob/main/resource/img/entry.png)

因此一个数据文件可以看做是多个 Entry 的集合：

![](https://github.com/roseduan/rosedb/blob/main/resource/img/db_file.png)

当写入数据时，如果是 String 类型，为了支持 string 类型的 key 前缀扫描，我将 key 存放到了跳表中，如果是其他类型的数据，则直接存放至对应的数据结构中。然后将 key、value 等信息，封装成 Entry 持久化到数据文件中。

如果是删除操作，那么也会被封装成一个 Entry，标记为其是一个删除操作，然后也需要持久化到数据文件中，这样的话就会带来一个问题，数据文件中可能会存在大量的冗余数据，占用不必要的磁盘空间。为了解决这个问题，我写了一个 reclaim 方法，你可以将其理解为对数据文件进行重新整理，使其变得更加的紧凑。

reclaim 方法的执行流程也比较的简单，首先建立一个临时的文件夹，用于存放临时数据文件。然后遍历整个数据库实例中的所有已归档文件，依次遍历数据文件中的每个 Entry，将有效的 Entry 写到新的临时数据文件中，最后将临时文件拷贝为新的数据文件，原数据文件则删除。

这样便使得数据文件的内容更加紧凑，并且去除了无用的 Entry。

## 安装

项目基于 Go 1.14.4 开发，首先需要确保安装了 Golang 环境，安装请参考 [Golang 官网](https://golang.org/)。

使用 `go get github.com/roseduan/rosedb` 安装，然后在你的项目中 import 即可：

```go
import (
    github.com/roseduan/rosedb
)
```

## 使用

### 初始化

初始化默认配置数据库：

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
	
	defer db.Close()
	
	//...
}
```

可配置的选项如下：

```go
type Config struct {
   DirPath          string               `json:"dir_path"`   				//数据库数据存储目录
   BlockSize        int64                `json:"block_size"` 				//每个数据块文件的大小
   RwMethod         storage.FileRWMethod `json:"rw_method"`  				//数据读写模式
   IdxMode          DataIndexMode        `json:"idx_mode"`   				//数据索引模式
   MaxKeySize       uint32               `json:"max_key_size"`			//key的最大size
   MaxValueSize     uint32               `json:"max_value_size"`		//value的最大size
   Sync             bool                 `json:"sync"`              //每次写数据是否持久化
   ReclaimThreshold int                  `json:"reclaim_threshold"` //回收磁盘空间的阈值
}
```

默认配置如下：

```go
func DefaultConfig() Config {
   return Config{
      DirPath:          os.TempDir(),							//操作系统临时目录，这个配置最好自定义
      BlockSize:        DefaultBlockSize,					//16MB
      RwMethod:         storage.FileIO,
      IdxMode:          KeyValueRamMode,
      MaxKeySize:       DefaultMaxKeySize,				//128字节
      MaxValueSize:     DefaultMaxValueSize,			//1MB
      Sync:             false,
      ReclaimThreshold: DefaultReclaimThreshold,	//4 当已封存文件个数到达 4 时，可进行回收
   }
}
```

### String



### List

### Hash

### Set

### Sorted Set

## 待办

+ [ ] 支持 TTL
+ [ ] 支持事务，ACID 特性
+ [ ] 数据压缩
+ [x] String 类型 key 加入前缀扫描
+ [ ] 缓存淘汰策略
+ [ ] 写一个简单的客户端，支持命令行操作
+ [ ] 完善相关文档

