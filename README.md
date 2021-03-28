# rosedb ![](https://img.shields.io/github/license/roseduan/rosedb)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/roseduan/rosedb)&nbsp;](https://goreportcard.com/report/github.com/roseduan/rosedb)![GitHub top language](https://img.shields.io/github/languages/top/roseduan/rosedb)&nbsp;[![GitHub stars](https://img.shields.io/github/stars/roseduan/rosedb)&nbsp;](https://github.com/roseduan/rosedb/stargazers)

rosedb 是一个简单、内嵌的 k-v 数据库，使用 `Golang` 实现，支持多种数据结构，包含 `String`、`List`、`Hash`、`Set`、`Sorted Set`，接口名称风格和 Redis 类似，如果你对 redis 比较熟悉，那么使用起来会毫无违和感。

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
   DirPath          string               `json:"dir_path"`//数据库数据存储目录
   BlockSize        int64                `json:"block_size"`//每个数据块文件的大小
   RwMethod         storage.FileRWMethod `json:"rw_method"`//数据读写模式
   IdxMode          DataIndexMode        `json:"idx_mode"`//数据索引模式
   MaxKeySize       uint32               `json:"max_key_size"`//key的最大size
   MaxValueSize     uint32               `json:"max_value_size"`//value的最大size
   Sync             bool                 `json:"sync"`//每次写数据是否持久化
   ReclaimThreshold int                  `json:"reclaim_threshold"`//回收磁盘空间的阈值
}
```

默认配置如下：

```go
func DefaultConfig() Config {
   return Config{
      DirPath:          os.TempDir(),//操作系统临时目录，这个配置最好自定义
      BlockSize:        DefaultBlockSize,//16MB
      RwMethod:         storage.FileIO,
      IdxMode:          KeyValueRamMode,
      MaxKeySize:       DefaultMaxKeySize,//128字节
      MaxValueSize:     DefaultMaxValueSize,//1MB
      Sync:             false,
      ReclaimThreshold: DefaultReclaimThreshold,//4 当已封存文件个数到达 4 时，可进行回收
   }
}
```

### String

#### Set

设置普通的键值对数据，如果 key 已经存在，则覆盖原来的 value。

```go
err := db.Set([]byte("test_key"), []byte("I am roseduan"))
if err != nil {
   log.Fatal("write data error ", err)
}
```

#### SetNx

如果对应的 key 不存在才添加，否则不做任何操作。

#### Get

获取 key 对应的 value

```go
_ = db.Set([]byte("test_key"), []byte("test_value"))
_ =db.SetNx([]byte("test_key"), []byte("value_001"))
_ =db.SetNx([]byte("test_key_new"), []byte("value_002"))

val1, _ := db.Get([]byte("test_key"))
val2, _ := db.Get([]byte("test_key_new"))
```

#### GetSet

将键 key 的值设为 value ， 并返回键 key 在被设置之前的旧值。

```go
val, err := db.GetSet([]byte("test_key001"), []byte("test_new_val_001"))
if err != nil {
   log.Fatal(err)
}
t.Log("original val : ", string(val))

val, _ = db.Get([]byte("test_key001"))
t.Log("new val : ", string(val))
```

#### Append

如果 key 存在，则将 value 追加至原来的 value 末尾，否则相当于 Set 方法。

```go
_ = db.Set([]byte("my_name"), []byte("roseduan"))
_ = db.Append([]byte("my_name"), []byte(" append some val"))

val, _ := db.Get([]byte("my_name"))
```

#### StrLen

返回 key 存储的字符串值的长度。

#### StrExists

判断 key 是否存在。

#### StrRem

删除 key 及其数据。

```go
_ = db.Set([]byte("my_name"), []byte("roseduan"))
_ = db.StrRem([]byte("my_name"))
val, _ := db.Get([]byte("my_name"))    //val == nil
```

#### PrefixScan

根据前缀查找所有匹配的 key 对应的 value。

```go
db.Set([]byte("ac"), []byte("3"))
db.Set([]byte("aa"), []byte("1"))
db.Set([]byte("ae"), []byte("4"))
db.Set([]byte("ar"), []byte("6"))
db.Set([]byte("ba"), []byte("7"))
db.Set([]byte("ab"), []byte("2"))
db.Set([]byte("af"), []byte("5"))

findPrefix := func(limit, offset int) {
   values, err := db.PrefixScan("a", limit, offset)
   if err != nil {
      log.Fatal(err)
   }

   if len(values) > 0 {
      for _, v := range values {
         t.Log(string(v))
      }
   }
}

//findPrefix(-1, 0)
//findPrefix(2, 0)
//findPrefix(2, 2)
//findPrefix(1, 3)
findPrefix(1, 20)
```

#### RangeScan

范围扫描，查找 key 是从 start 到 end 之间的数据。

```go
_ = db.Set([]byte("100054"), []byte("ddfd"))
_ = db.Set([]byte("100009"), []byte("dfad"))
_ = db.Set([]byte("100007"), []byte("rrwe"))
_ = db.Set([]byte("100011"), []byte("eeda"))
_ = db.Set([]byte("100023"), []byte("ghtr"))
_ = db.Set([]byte("100056"), []byte("yhtb"))

val, err := db.RangeScan([]byte("100007"), []byte("100030"))
if err != nil {
   log.Fatal(err)
}

if len(val) > 0 {
   for _, v := range val {
      t.Log(string(v))
   }
}
```

#### Expire

#### TTL

#### Persist

### List

#### LPush

在列表的头部添加元素，返回添加后的列表长度。

#### RPush

在列表的尾部添加元素，返回添加后的列表长度。

#### LPop

取出列表头部的元素。

#### RPop

取出列表尾部的元素。

```go
key := []byte("my_list")
db.LPush(key, []byte("list_data_001"), []byte("list_data_002"))
db.RPush(key, []byte("list_data_003"), []byte("list_data_004"))

val1, _ := db.LPop(key)
t.Log(string(val1))

val2, _ := db.RPop(key)
t.Log(string(val2))
```

#### LInsert

将值 val 插入到列表 key 当中，位于值 pivot 之前或之后。

```go
key := []byte("my_list")
db.LInsert(string(key), 0, []byte("new val"), []byte("list_data_003"))
```

#### LSet

将列表 key 下标为 index 的元素的值设置为 val。

```go
key := []byte("my_list")
ok, err := db.LSet(key, 0, []byte("new val"))
```

#### LRem

移除列表中与参数 value 相等的元素。

```go
key := []byte("my_list")

printAll := func() {
   vals, _ := db.LRange(key, 0, -1)
   for _, v := range vals {
      t.Logf("%s ", string(v))
   }
   t.Log()
}

db.LPush(key, []byte("11"), []byte("12"), []byte("23"), []byte("11"))
printAll()
db.LRem(key, []byte("11"), 0)
printAll()
```

#### LTrim

对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。

```go
db.LTrim(key, 3, 5)
```

#### LRange

返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定。

### Hash

#### HSet

将哈希表 hash 中域 field 的值设置为 value。

#### HSetNx

当且仅当域 field 尚未存在于哈希表的情况下， 将它的值设置为 value。

#### HGet

返回哈希表中给定域的值。

#### HGetAll

返回哈希表 key 中，所有的域和值。

#### HLen

返回哈希表 key 中域的数量。

```go
key := []byte("my_set")
db.HSet(key, []byte("name"), []byte("roseduan"))
db.HSet(key, []byte("age"), []byte("24"))
db.HSet(key, []byte("hobbies"), []byte("coding writing football"))
db.HSet(key, []byte("dream"), []byte("be better"))

db.HSetNx(key, []byte("dream"), []byte("dream at day"))
db.HSetNx(key, []byte("height"), []byte("1.75"))

dream := db.HGet(key, []byte("dream"))
t.Log("my dream is ", string(dream))

l := db.HLen(key)
t.Log(l)

all := db.HGetAll(key)
for _, v := range all {
   t.Log(string(v))
}
```

#### HExists

检查给定域 field 是否存在于 key 对应的哈希表中。

#### HKeys

返回哈希表 key 中的所有域。

#### HValues

返回哈希表 key 中的所有域对应的值。

```go
key := []byte("my_set")
keys := db.HKeys(key)
values := db.HValues(key)
```

### Set

#### SAdd

添加元素，返回添加后的集合中的元素个数。

#### SPop

随机移除并返回集合中的 count 个元素。

#### SIsMember

判断 member 元素是不是集合 key 的成员。

```go
key := []byte("my_set")
db.SAdd(key, []byte("set_data_001"), []byte("set_data_002"), []byte("set_data_003"))
values, _ := db.SPop(key, 2)

t.Log(len(values))
for _, v := range values {
   t.Log(string(v))
}

ok := db.SIsMember(key, []byte("set_data_001"))
ok = db.SIsMember(key, []byte("set_data_003"))
t.Log(ok)
```

#### SRandMember

从集合中返回随机元素。

```go
key := []byte("my_set")
members := db.SRandMember(key, 5)
for _, m := range members {
   t.Log(string(m))
}
```

#### SRem

移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。

#### SMove

#### SCard

#### SMembers

#### SUnion

#### SDiff

### Sorted Set

#### ZAdd

#### ZScore

#### ZCard

#### ZRank

#### ZRevRank

#### ZIncrBy

#### ZRange

## 待办

+ [x] 支持 TTL
+ [ ] 支持事务，ACID 特性
+ [ ] 文件数据压缩存储
+ [x] String 类型 key 加入前缀扫描
+ [ ] 缓存淘汰策略
+ [ ] 写一个简单的客户端，支持命令行操作
+ [ ] 完善相关文档

## License

rosedb 根据 MIT License 许可证授权，有关完整许可证文本，请参阅 [LICENSE](https://github.com/roseduan/rosedb/blob/main/LICENSE)。

