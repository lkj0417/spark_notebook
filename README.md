# Apache Spark 学习笔记

> 本文是 Apache Spark 的入门与实践笔记，示例主要基于 Spark 2.4.3、Scala 2.11 和 Java 8。Spark 及其生态版本迭代较快；使用示例前，请先核对集群实际版本、语言版本和对应的官方文档。

## 目录

- [一、Spark 介绍](#一-spark-介绍)
- [二、Spark 快速入门](#二-spark-快速入门)
- [三、Spark Core 工程开发](#三-spark-core-工程开发)
- [四、弹性分布式数据集 RDD](#四-弹性分布式数据集-rdd)
- [五、Spark 程序运行](#五-spark-程序运行)
- [六、Spark SQL](#apache-spark-sql)
- [七、Spark Streaming](#apache-spark-streaming)
- [八、Spark 项目实践](#六-spark项目)
- [附录](#附录)

## 一、 Spark 介绍

> 官网：https://spark.apache.org/

Apache Spark 是一个支持多语言 API 的分布式计算引擎，可用于批处理、SQL、流处理、机器学习和图计算。它既能利用内存加速重复计算，也会根据存储级别将数据写入磁盘；因此，“基于内存”不代表所有数据和中间结果都必须常驻内存。

Spark 提供多个面向不同任务的组件：Spark Core 提供基础执行能力，Spark SQL 处理结构化数据，Structured Streaming 提供流处理 API，MLlib 提供机器学习功能，GraphX 提供图计算 API。旧版 Spark Streaming 采用微批处理模型；新项目通常优先评估 Structured Streaming。

### 1. Spark的核心模块

<img src="./Apache Spark.assets/image-20250318213623587.png" alt="image-20250318213623587" style="zoom:80%;" />

- Spark core：Spark Core中提供了Spark最基础与最核⼼的功能，Spark 其他的功能如：Spark SQL、Spark Streaming、GraphX、MLlib 都是在 Spark Core的基础上进⾏扩展的。
- Spark SQL：通过SQL的方式来操作Spark读取的数据
- Spark Streaming：旧版 DStream API 以微批次方式处理流数据。它适合了解传统 Spark 流处理模型；新项目通常优先考虑 Structured Streaming。
- Spark MLlib：机器学习相关的算法库。MLlib 不仅提供了模型评估、数据导⼊等额外的功能，还提供了⼀些更底层的机器学习原语。
- Spark GraphX：面向图计算的一些框架和算法库

### 2. Spark 的特点

- 开发上手快：哪怕没有学习过MapReduce，只需要简单几行代码，就可以完成一个计算流程。（因为Spark封装了很多的方法以及算子）
- Spark 支持 Scala、Java、Python、R 和 SQL 等接口，具体功能支持因 API 和版本而异。
- Spark 可以运行在 Standalone、YARN、Kubernetes 等集群管理器上，也可读取 HDFS 等 Hadoop 生态系统中的数据；它不要求必须依赖 Hadoop。
- Spark 的 DAG 调度、缓存和 SQL 优化器等功能可提升多种工作负载的开发效率，但实际性能取决于数据规模、分区、资源和工作负载，不能仅依据框架名称判断。

### 3. Spark和MapReduce的对比

Hadoop MapReduce 通常将 map 与 reduce 之间的中间结果写入磁盘。该模型简单且具有成熟的容错机制，但多轮迭代会产生较多 I/O。

Spark 使用 DAG 执行模型；RDD 或 DataFrame 在需要重复使用时可以缓存，以减少重复读取和计算。Spark 也会将 shuffle 数据写入磁盘，内存不足时还可能溢写，因此不能假设中间数据始终在内存中或 Spark 一定减少 shuffle。

| 场景 | Hadoop MapReduce | Apache Spark |
| :-- | :-- | :-- |
| 批处理 | 成熟的磁盘中间结果模型，适合批量作业 | DAG 执行；可缓存复用的数据，适用于多种批处理任务 |
| 迭代计算 | 反复读写中间结果时开销较大 | 缓存中间数据可减少重复计算，需结合内存容量评估 |
| 流处理 | 经典 MapReduce 面向有限数据集，不提供原生连续流 API | 可使用 Structured Streaming；Spark Streaming 是旧版微批 API |
| SQL 分析 | 通常需借助其他 SQL 引擎 | Spark SQL 提供 DataFrame、SQL 和优化执行计划 |

> 两者定位和使用场景并不完全相同。选择时应同时考虑延迟要求、容错语义、运维成本、数据源和团队经验。

## 二、 Spark的快速入门

### 1. spark 的安装

如果在一个没有安装过 Spark 的系统中，需要安装 Spark，可以遵循以下步骤：

1. 根据目标集群选择 Spark、Scala、Java 和 Hadoop 兼容版本，并查看该版本的官方安装文档。
2. 下载对应发行包，解压到目标目录。
3. 配置 `JAVA_HOME`，并按运行模式配置必要的 Hadoop 环境变量和配置文件。
4. 先使用本地模式验证安装，再连接集群运行。

> Spark 发行包名称中的 Hadoop 版本通常表示其 Hadoop 客户端依赖版本；请以发行包说明和集群配置为准，不要只凭版本号推断兼容性。

### 2. 进入Spark的交互式终端

```bash
[hadoop@hadoop bin]$ spark-shell --master local[2]
25/03/18 21:55:37 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://hadoop:4040
Spark context available as 'sc' (master = local[2], app id = local-1742306146943).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/
         
Using Scala version 2.11.12 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_341)
Type in expressions to have them evaluated.
Type :help for more information.

scala> 
```

### 3. 基础功能演示

```scala
scala> println("hello world")
hello world             

scala> sc.textFile("file:///home/hadoop/word.txt")
res1: org.apache.spark.rdd.RDD[String] = file:///home/hadoop/word.txt MapPartitionsRDD[1] at textFile at <console>:25

scala> res1.count
res3: Long = 3

scala> res1.filter
   def filter(f: String => Boolean): org.apache.spark.rdd.RDD[String]

scala> val myTextFile = sc.textFile("file:///home/hadoop/word.txt")
myTextFile: org.apache.spark.rdd.RDD[String] = file:///home/hadoop/word.txt MapPartitionsRDD[3] at textFile at <console>:24

scala> myTextFile.count
res4: Long = 3

scala> val fileLineCount = myTextFile.count
fileLineCount: Long = 3

scala> val containMe = myTextFile.filter( line => line.contains("me")   )
containMe: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[4] at filter at <console>:25

scala> val collect = containMe.collect()
collect: Array[String] = Array(hello me)
```

## 三、 Spark的工程开发

> 以 Spark Core 为例

### 1. 创建 Maven 工程

```xml
<!-- 示例版本适用于本文中的 Spark 2.4.3 / Scala 2.11 笔记 -->
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_2.11</artifactId>
  <version>2.4.3</version>
</dependency>
```

部署到已安装 Spark 的集群时，通常将 Spark 依赖设为 `provided`，避免把集群已提供的 Spark 库重复打入应用包；本地运行则需确保运行时 classpath 中有相应依赖。

### 2. 创建sparkContext对象

```java
SparkConf sparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("my-spark-app");
JavaSparkContext sc = new JavaSparkContext(sparkConf);
```

`local[*]` 表示本地使用可用处理器核心数。提交到集群时，通常不在代码中固定 `master`，而由 `spark-submit --master` 指定；完成后应在 `finally` 中调用 `sc.stop()` 释放资源。

### 3. 获取数据

```java
sc.textFile("file:///path/to/file");
sc.textFile("hdfs://192.168.56.101:8020/path/to/file");
```

### 4. 通用计算

RDD 操作分为 transformation（转换）和 action（行动）。转换会构造新的 RDD，action 会触发计算；具体操作见下文。

### 5. 输出数据

1. 输出到控制台（一般用于开发的时候调试）
2. 输出到文件（本地文件/hdfs/hive...）

<img src="./Apache Spark.assets/image-20250320211933693.png" alt="image-20250320211933693" style="zoom:80%;" />

### 6. 回收sc对象

`sc.stop()`

## 四、 弹性分布式数据集 RDD

> Spark 围绕弹性分布式数据集（RDD）的概念展开，它是一组可以并行操作的容错元素集合。
>
> 创建 RDD 有两种方式：在驱动程序中并行化现有的集合，或者引用外部存储系统中的数据集，例如共享文件系统、HDFS、HBase 或任何提供 Hadoop InputFormat 的数据源。

Resilient Distributed Datasets

### 1. RDD 的核心特性

- 分布式(Distributed)：RDD区别于传统的数据集（集合），最大的特点就是它是分布式的，也就是说，它的数据会被切割成多个分区（partition），每个分区可以分布在集群中的不同节点上进行运算。 这样做的好处是可以并行的大规模处理数据，第二个好处就是方便编程。

<img src="./Apache Spark.assets/image-20250320214805778.png" alt="image-20250320214805778" style="zoom:80%;" />

- 弹性（Resilient）：容错性。如果某个rdd转换的过程中，部分分区数据丢失，spark可以通过rdd的关系（血统lineage）信息重新计算丢失的分区，而不需要重新计算整个数据集。
- 不可变性（immutable）：RDD是不可变的，一旦被创建之后，就不能再修改了，所有的转换操作都会生成一个新的RDD，而不是修改原始的RDD。
- 类型化（Typed）：RDD和集合一样，都是强类型的，可以存储任何类型的数据，但是一个RDD中的数据类型必须都相同。

### 2. RDD 的创建方式

1. 通过并行化接口从集合创建RDD （常用于测试）
2. 通过外部数据创建（本地文件/hdfs文件）

### 3.  RDD 的操作

RDD支持两种操作方式：transformation和action

RDD的转换操作是**惰性求值**的，意思是所有的转换操作，在代码执行过程中，不会立即执行，而是记录下操作逻辑，直到遇到了action才会触发计算。

因为如果没有action，说明这个计算结果没有人使用，那么就不计算了。

1. Transformation（转换操作）

| Transformation                                               | Meaning                                                      |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| **map**(*func*)                                              | Return a new distributed dataset formed by passing each element of the source through a function *func*. 返回由通过函数 func 对源数据集中的每个元素进行传递而形成的新分布式数据集。 |
| **filter**(*func*)                                           | Return a new dataset formed by selecting those elements of the source on which *func* returns true. |
| **flatMap**(*func*)                                          | Similar to map, but each input item can be mapped to 0 or more output items (so *func* should return a Seq rather than a single item). |
| **mapPartitions**(*func*)                                    | Similar to map, but runs separately on each partition (block) of the RDD, so *func* must be of type Iterator<T> => Iterator<U> when running on an RDD of type T. |
| **groupByKey**([*numPartitions*])                            | When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. **Note:** If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using `reduceByKey` or `aggregateByKey` will yield much better performance. **Note:** By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional `numPartitions` argument to set a different number of tasks. |
| **reduceByKey**(*func*, [*numPartitions*])                   | When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function *func*, which must be of type (V,V) => V. Like in `groupByKey`, the number of reduce tasks is configurable through an optional second argument. |
| **aggregateByKey**(*zeroValue*)(*seqOp*, *combOp*, [*numPartitions*]) | When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in `groupByKey`, the number of reduce tasks is configurable through an optional second argument. |
| **repartition**(*numPartitions*)                             | Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network. |
| **coalesce**(*numPartitions*)                                | 减少分区数；默认不进行完整 shuffle，可能导致分区数据不均。需要重新均衡分区时可选择带 shuffle 的方式。 |

> `reduceByKey` 和 `aggregateByKey` 通常比先 `groupByKey` 再聚合更高效，因为它们可以先在 map 端合并部分结果。转换是否触发 shuffle 取决于具体算子和参数。

2. action

   action会触发真正的计算，并将结果返回到 Driver或者保存到外部进行存储。

| **reduce**(*func*)                                 | Aggregate the elements of the dataset using a function *func* (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. |
| -------------------------------------------------- | ------------------------------------------------------------ |
| **collect**()                                      | Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. |
| **count**()                                        | Return the number of elements in the dataset.                |
| **first**()                                        | Return the first element of the dataset (similar to take(1)). |
| **take**(*n*)                                      | Return an array with the first *n* elements of the dataset.  |
| **takeSample**(*withReplacement*, *num*, [*seed*]) | Return an array with a random sample of *num* elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed. |
| **takeOrdered**(*n*, *[ordering]*)                 | Return the first *n* elements of the RDD using either their natural order or a custom comparator. |
| **saveAsTextFile**(*path*)                         | Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. |
| **saveAsSequenceFile**(*path*) (Java and Scala)    | Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). |
| **saveAsObjectFile**(*path*) (Java and Scala)      | Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using `SparkContext.objectFile()`. |
| **countByKey**()                                   | Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. |
| **foreach**(*func*)                                | Run a function *func* on each element of the dataset. This is usually done for side effects such as updating an [Accumulator](https://archive.apache.org/dist/spark/docs/2.4.3/rdd-programming-guide.html#accumulators) or interacting with external storage systems. **Note**: modifying variables other than Accumulators outside of the `foreach()` may result in undefined behavior. See [Understanding closures ](https://archive.apache.org/dist/spark/docs/2.4.3/rdd-programming-guide.html#understanding-closures-a-nameclosureslinka)for more details. |

### 4. 依赖关系

宽窄依赖 --> stage划分

- 宽依赖（Wide Dependency）

  每个父RDD的分区可能被多个子RDD的分区使用，这种就叫做宽依赖。

  通常会产生宽依赖的算子包含：  `groupByKey`、`reduceByKey`、`repartition`、`distinct`

  <img src="./Apache Spark.assets/image-20250322101350837.png" alt="image-20250322101350837" style="zoom:80%;" />

  

  - 窄依赖（Narrow Dependency）

    每个父RDD的分区最多被一个子RDD的分区使用，这种就叫做窄依赖。

    通常会产生窄依赖的算子包含：  `map`、`filter`、`mapPartition`、`sample`、`union`

    <img src="./Apache Spark.assets/image-20250322101650945.png" alt="image-20250322101650945" style="zoom:80%;" />

  宽依赖必然会有shuffle过程，shuffle的本质是数据的跨节点计算，因此在划分stage的时候，遇到了shuffle（宽依赖）就会切割stage（切割血缘）

### 5. 分区和并行度

在 Spark 中，分区（partition）是 RDD 或 DataFrame 的逻辑数据划分，也是 stage 中 task 的基本调度单位。一个 task 通常处理一个分区；分区数会影响 task 数量，但分区不一定与 HDFS 文件块或物理节点一一对应。

并行度描述可同时执行的 task 数量，受分区数、可用 executor 核心数和资源调度等因素影响。提高并行度并不一定提升性能，还要考虑数据倾斜、task 开销和集群资源。

- 分区
  - 文件读取分区数受文件系统、文件大小、输入格式和读取参数等影响。
  - 并行化集合时可指定分区数；默认值与 Spark 配置及运行环境有关。
  - RDD 和 Spark SQL 的 shuffle 分区使用不同配置。本文 Spark 2.4 中，SQL 默认 shuffle 分区配置为 `spark.sql.shuffle.partitions`（默认值 200）；RDD 算子的分区数由算子参数、分区器和相关配置决定。
  - 可通过 `repartition`、`coalesce` 或自定义分区器调整分区，但应在观察任务运行和数据分布后决定。

> 调优时可结合 Spark UI 查看 stage、task 数量、数据倾斜和 shuffle 读写量。不存在适用于所有集群的固定分区倍数。

### 6. 持久化和缓存

缓存和持久化（`cache` / `persist`）用于在同一个 Spark 应用内复用计算结果，不等同于长期数据存储。Spark 可以按存储级别保留分区在内存或磁盘中；未能保留的分区可能需要重新计算。应用结束后，这些缓存数据会被清理。

常见存储级别包括 `MEMORY_ONLY` 和 `MEMORY_AND_DISK`。选择时需权衡内存占用、序列化开销、磁盘 I/O 与重算成本。

<img src="./Apache Spark.assets/image-20250322115136723.png" alt="image-20250322115136723" style="zoom:80%;" />

cache本质就是 StorageLevel.MEMORY_ONLY 的persist

| Storage Level                          | Meaning                                                      |
| -------------------------------------- | ------------------------------------------------------------ |
| MEMORY_ONLY                            | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level. |
| MEMORY_AND_DISK                        | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed. |
| MEMORY_ONLY_SER (Java and Scala)       | Store RDD as *serialized* Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a [fast serializer](https://spark.apache.org/docs/latest/tuning.html), but more CPU-intensive to read. |
| MEMORY_AND_DISK_SER (Java and Scala)   | Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed. |
| DISK_ONLY                              | Store the RDD partitions only on disk.                       |
| MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc. | Same as the levels above, but replicate each partition on two cluster nodes. |
| OFF_HEAP (experimental)                | Similar to MEMORY_ONLY_SER, but store the data in [off-heap memory](https://spark.apache.org/docs/latest/configuration.html#memory-management). This requires off-heap memory to be enabled. |

需要注意，这里的持久化不是真的持久化，这个持久化只在spark application的生命周期中有效，一旦application结束，persist也会被清理。

### 7. checkpoint

> 很少用

RDD checkpoint 会将计算结果写入可靠存储（例如 HDFS），并截断 RDD 的 lineage。使用前需通过 `SparkContext.setCheckpointDir(path)` 设置目录，再对目标 RDD 调用 `checkpoint()`；检查点通常在后续 action 执行时写入。它会产生额外的计算和存储开销，应在 lineage 过长或恢复成本较高时使用。

Structured Streaming 的检查点还会保存查询进度和状态，以支持故障恢复；它与 RDD checkpoint 的用途和格式不同。检查点目录应使用可靠存储，并遵循具体 API 的恢复要求。

## 五、spark 程序运行

通常在开发的时候，会设置 master为 local，这样做是为了快速的在本地运行spark程序进行验证。

真实的工作中，开发完spark程序后，需要将程序打包并提交到集群中运行。

### 1. 打包程序

1. 本地测试使用的 `setMaster("local[*]")` 不应覆盖集群提交参数；建议将 master 通过 `spark-submit` 指定。
2. 通过 Maven 打包：`mvn clean package`。
3. 明确依赖的打包策略：集群已提供的 Spark 依赖通常设为 `provided`，业务依赖则按发行包和集群要求打包或提供。

### 2. 提交任务到yarn

spark on yarn

```bash
spark-submit \
--class org.example.App2 \
--master yarn \
--deploy-mode client \
--executor-memory 512M \
--num-executors 1 \
--executor-cores 2 \
spark-learning-1.0-SNAPSHOT.jar

spark-submit --class org.example.App2 --master yarn --deploy-mode client --executor-memory 512M --num-executors 1 --executor-cores 2  spark-learning-1.0-SNAPSHOT.jar
```

| 参数            | 示例                   | 描述                                                         |
| --------------- | ---------------------- | ------------------------------------------------------------ |
| class           | com.example.WordCount2 | 作业的主类。                                                 |
| master          | yarn                   | 在企业中多使用 Yarn 模式。                                   |
| deploy-mode     | client                 | Driver 在提交端进程中运行；提交端需在应用运行期间保持可用。 |
|                 | cluster                | Driver 在集群中运行，提交客户端退出后应用仍可继续运行。 |
| driver-memory   | 4g                     | Driver 可用内存上限之一；实际资源还受集群管理器和部署配置约束。 |
| num-executors   | 2                      | 创建 Executor 的个数。                                       |
| executor-memory | 2g                     | 各个 Executor 请求的堆内存；容器总内存还可能包括额外开销。 |
| executor-cores  | 2                      | 各个 Executor 使用的并发线程数目，即每个 Executor 最大可并发执行的 Task 数目。 |

<img src="./Apache Spark.assets/image-20250322110355871.png" alt="image-20250322110355871" style="zoom:80%;" />

> Spark 2.4 仍可见 `yarn-client` / `yarn-cluster` 等旧写法；新命令建议显式使用 `--master yarn` 和 `--deploy-mode client|cluster`。部署模式、资源参数及可用选项以当前 Spark 版本和集群策略为准。


涉及的一些概念：

- **Application**:一个main函数(一般来讲是一个SparkContext)所包含的所有代码就是一个Spark Application;
- **Job**:每执行一个action都会生成一个Job;
- **Stage**:一个Spark的job根据是否有宽依赖，划分stage。一般包含一到多个Stage。 
- **Task**:rdd的partition决定了task的数量，一个Stage包含一到多个Task，通过多个Task实现并行运行的功能。

完整运行过程说明如下:

1. `spark-submit` 启动应用 Driver。Driver 执行应用入口代码并创建 `SparkContext`，初始化调度器等组件。
2. 集群管理器根据部署模式和资源配置分配 Executor 资源。YARN、Standalone 和 Kubernetes 的资源申请与进程启动流程并不相同。
3. Executor 启动后向 Driver 注册并等待任务。
4. action 触发 Job；DAGScheduler 根据依赖关系划分 stages，TaskScheduler 将每个 stage 的 tasks 分发给 Executor。
5. Executor 在 task 线程中处理分区数据，并向 Driver 汇报执行状态和结果。

> Application、Job、Stage 和 Task 是不同层次的执行概念：一个应用可包含多个 Job；Job 通常拆分为多个 Stage；每个 Stage 包含若干 Task，Task 数量通常与该 Stage 的分区数对应。





# Apache Spark SQL

## 一、 SparkSQL 介绍

Spark SQL 是 Spark 用来处理**结构化数据**的模块，可通过 SQL 和 DataFrame API 访问数据。DataFrame 带有列名和类型等 schema 信息；Spark SQL 通过 Catalyst 优化器和执行引擎生成执行计划。DataFrame 不宜简单等同于“RDD 加 schema”，因为两者执行抽象、优化能力和 API 都不同。

Spark SQL 的前身项目是 Shark。当前 Spark SQL 可连接多种数据源，也可与 Hive metastore 集成；数据格式、catalog 和连接器支持应根据 Spark 版本确认。

SparkSQL的特点：

1. 多数据源支持

   比如 json、csv、jdbc、hive等等结构化数据源都可以支持，包括数据的读和写以及数据分析

2. 无缝集成RDD

   Spark SQL 可以与 RDD API 互操作，但转换会影响优化边界；需要利用 DataFrame/SQL 的优化能力时，尽量在该抽象中完成可表达的操作。



## 二、 SparkSQL 编程模型

Spark SQL使用的数据抽象并非是RDD,而是DataFrame。在Spark1.3.0版本之前，DataFrame被称为 SchemaRDD。DataFrame使Spark具备了处理大規模结构化数据的能力。在Spark中，DataFrame是一种以RDD 为基础的分布式数据集，因此DataFrame可以完成RDD的绝大多数功能，在开发使用时，也可以调用方法将RDD 和DataFrame进行相互转换。DataFrame的结构类似于传统数据库的二维表格，并且可以从很多数据源中创建， 如结构化文件、外部数据库、Hive表等数据源。DataFrame与RDD在结构上的区别如下所示。

<img src="./Apache Spark.assets/image-20250322145823260.png" alt="image-20250322145823260" style="zoom:50%;" />

RDD是分布式的Java对象的集合，如上图所示的RDD[Person]数据集，虽然它以Person为类型参数，但是对象内部 之间的结构相对于Spark框架本身是无法得知的，这样在转换数据形式时效率相对较低。DataFrame除了提供比RDD更丰富的算子以外，更重要的特点是提升Spark框架执行效率、减少数据读 取时间以及优化执行计划。有了DataFrame这个更高层次的抽象后，处理数据就更加简单了,甚至可以直接用SQL来 处理数据，这对于开发者来说，易用性有了很大的提升。



Spark SQL 程序通常通过 `SparkSession` 访问 SQL 功能。`SparkSession` 提供 DataFrame、SQL、catalog 等接口，并可通过 `spark.sparkContext` 获取底层 `SparkContext`。



DataFrame和dataset的关系

```scala
  type DataFrame = Dataset[Row]
```

可以认为，Spark中的DataFrame就是特殊的dataset（类型为Row的dataset）。

在 Scala/Java API 中，`Dataset[T]` 可以使用类型化对象；`DataFrame` 是 `Dataset[Row]` 的别名，通常通过列名访问字段。Python API 的 DataFrame 同样基于 Row 结构，不提供 Scala/Java Dataset 的静态类型安全保证。

### 1. 创建DataFrame的方式

1. 通过自定义schema结构来创建一个DataFrame
2. 通过实体类创建DataFrame
3. 通过外部文件创建DataFrame
4. 通过jdbc读取数据库的表（外部连接器）（MongoDB、es  https://spark.apache.org/third-party-projects.html）

### 2. 对DataFrame做操作

可使用 DataFrame DSL（领域特定语言）或 SQL 操作数据。两种方式最终由 Spark SQL 构建执行计划；选择更便于表达和维护的方式即可。

> 可使用 `explain()` 查看逻辑或物理计划，使用 Spark UI 观察实际运行情况；对大规模数据谨慎使用 `collect()`，因为它会将全部结果拉回 Driver。

### 3. 输出

1. 输出到控制台（`show` 用于预览；`collect` 会将结果传到 Driver，应仅用于结果集足够小的情况）
2. 保存到文件
3. 保存到外部连接（jdbc、hive）

### 4. rdd和DataFrame相互转换

```java
        RDD<Row> rdd = dataframeFromJdbc.rdd();
        Dataset<Row> dataFrame2 = spark.createDataFrame(rdd, schema);
```



# Apache Spark Streaming

## 一、实时流处理

实时流处理，就是一种 处理连续、动态数据流的 计算技术，核心特点如下：

- 低延迟：数据输入后能够快速相应和处理
- 持续处理：能够连续处理**无边界**的数据流
- 动态计算：实时对数据进行分析、聚合和转换等

> 流处理系统通常还需明确事件时间或处理时间、乱序数据处理、状态管理、容错与端到端交付语义。“实时”延迟取决于系统设计和资源配置，并不意味着零延迟。

应用场景

- 实时推荐系统
- 金融交易监控
- 网络安全监控
- 社交媒体趋势分析
- ....

## 二、 Spark streaming介绍

> 本节讨论 Spark Streaming 的 DStream API（Spark 2.4 时代的微批处理接口）。这是旧式 API；新应用应优先评估 Structured Streaming，并确认所需数据源与 sink 的版本支持。

<img src="./Apache Spark.assets/image-20250325211945822.png" alt="image-20250325211945822" style="zoom:80%;" />

数据是源源不断产生的，我们通过SparkStreaming实时接收这种数据，并通过将数据进行切分的方式来处理。

### 1. 流处理思想

一个无边界的数据流可按固定时间间隔切分为一批有边界的数据；在 DStream 中，每个批次对应一个 RDD。批次间隔会影响处理延迟和调度开销，需根据负载测试选择。

<img src="./Apache Spark.assets/image-20250325213017249.png" alt="image-20250325213017249" style="zoom:80%;" />

`JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(5));`

第二个参数是批次间隔，通常按秒配置；实际间隔应结合单批处理耗时和目标延迟设置，避免批次持续积压。

### 2. DStream概念

SparkStreaming中的数据抽象叫做DStream，英文全称  Discretized Stream（离散流），它代表一个持续不断的数据流。

- 代表连续的数据流

- 底层基于RDD实现

- 支持RDD所支持的各种transformation和action

  <img src="./Apache Spark.assets/image-20250325213321364.png" alt="image-20250325213321364" style="zoom:80%;" />

### 3. DStream的操作

1. 无状态转换（跟RDD基本没有区别）

   ```scala
           JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("localhost", 9999);
   				JavaPairDStream<String, Integer> result = dStream
                   .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                   .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                   .reduceByKey((a, b) -> a + b);
   ```

2. 有状态转换

   1. 无状态：只对当前窗口的数据进行处理，不会依赖任何的历史时间窗口处理过的数据，这就是无状态计算（简单，但是不够丰富）
   2. 有状态（累积结果）：除了对当前窗口的数据进行处理之外，还需要依赖历史的窗口处理的数据结果，这就是有状态计算

- updateStateByKey  和  mapWIthState（Experimental）

  ```java
  // updateStateByKey
  				Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = (values, state) -> {
              Integer newSum = state.orElse(0); // 如果state存在，则取state，否则取0
              for (Integer i : values) {
                  newSum = i + newSum;
              }
              return Optional.of(newSum);
          };
          // 将当前的新数据和历史的状态数据进行累加，得到的结果作为新的状态返回。  （shuffle）
          JavaPairDStream<String, Integer> newResult = result.<Integer>updateStateByKey(updateFunction);
  
  ```

  - 每个批次处理时，会对所有的已存在的key重新计算状态，全量更新的方式，会导致即使某些key没有新数据，也会进行处理（效率低）
  - 使用上比较简单，但是状态会无限增长（也就是key的个数会膨胀）

  ```java
  // mapWithState  https://blog.yuvalitzchakov.com/exploring-stateful-streaming-with-apache-spark/
  // Function3<String, Option<Integer>,State<Integer>,Tuple2<String,Integer>>
          // (KeyType, Option[ValueType], State[StateType]) => MappedType
          StateSpec<String, Integer, Integer, Tuple2<String, Integer>> specFunction = StateSpec.function(
                  (Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>)
                          (word, value, state) -> {
                              int newState = value.orElse(0) + (state.exists() ? state.get() : 0);
                              state.update(newState);
                              return new Tuple2<>(word, newState);
                          });
  
          JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> newResult =
                  result.mapWithState(specFunction);
  ```

  - 增量更新状态，只会处理当前批次有变化的key
  - 使用时需要定义StateSpec函数，泛型包含 key类型，value类型，状态类型以及 MappedType map中每条数据的类型
  - 可以配置状态的超时时间，超时后自动清除状态，防止状态无限膨胀
  - 可以控制状态类型，不一定要跟数据的value类型一致
  - 实现上比较复杂，适用于大规模的状态管理（实验性接口）

若状态随 key 数量持续增长，应定义状态过期与清理策略，并评估状态规模和恢复语义。外部存储可以用于业务状态管理，但会引入额外的读写和一致性设计，不应简单视作 Spark 状态管理的替代品。



3. 窗口操作

   > 滑动窗口

   每间隔多长时间，统计多大时间窗口的数据

   比如：每5分钟 统计过去一小时的销售量/额

<img src="./Apache Spark.assets/image-20250329104359232.png" alt="image-20250329104359232" style="zoom:80%;" />

4. 累加器、广播变量、Checkpoint故障恢复

Accumulators, Broadcast Variables, and Checkpoints

- 累加器（executor只写）
  - 适合在 executor 端进行任务级计数和调试统计，driver 端读取最终值。
  - 任务重试或 stage 重算可能影响累加器的更新次数；不要用它实现精确业务账目或依赖副作用的更新。
- 广播变量（executor只读）
  - 广播变量由driver创建并广播，executor只能读取值，不能修改值
  - 广播变量可减少多个 task 重复传送小型只读数据的开销；是否广播应结合数据大小和集群内存评估。
  - 生命周期由应用管理，使用完可调用 `unpersist()` 或 `destroy()` 释放资源。

- 从 checkpoint 恢复 Spark Streaming 程序时，恢复能力取决于 checkpoint 内容、输入源和输出端的语义。
  - Socket 等不支持 offset 或重放的数据源无法保证故障期间的数据完整性；需要可靠恢复时，应选择支持重放的数据源。
  - 外部状态需单独定义持久化及恢复策略；checkpoint 不会自动保存任意外部系统中的业务状态。



### 4. 数据的输出

1. println打印到控制台 （本地调试）

2. 保存到文件 （streaming用的比较少，spark core用的多，尤其是保存到hdfs）  saveAsTextFiles  saveAsNewAPIHadoopFiles saveAsHadoopFiles

3. 保存到外部的存储（数据库、kv存储）

   ```java
   resultDstream.foreachRDD(rdd -> {
     rdd.foreachPartition(records -> {
       // 每个分区复用连接，并批量写入；妥善处理连接关闭、重试和幂等性。
       savePartitionToDb(records);
     })
   });
   ```

   > 避免为每条记录创建数据库连接。Spark task 可能重试，因此 sink 写入逻辑应设计为幂等，或明确重复写入的处理方式。

### 5. SQL 的方式处理DStream

工作原理：

1. DStream是基于 rdd 的数据流，当我们使用foreachRDD的时候，就变成了 rdd

2. rdd + schema 信息 就转成了 DataFrame，结构化，可以使用 SQL的方式处理

```
//    sparkSession            sparkSession            sparkContext    streamingContext
// 注册一个 words 临时表  <---  dataframe(dataset)  <--- rdd + schema  <--- dStream
```

## 六、spark项目

整体架构图

<img src="./Apache Spark.assets/image-20250330100511730.png" alt="image-20250330100511730" style="zoom:80%;" />

### 1. 数据集介绍

来源：开源数据集 https://files.grouplens.org/datasets/movielens/ml-25m.zip

- movies.csv

  该文件是电影数据，对应为维读表，包含62423多部电影，movies.csv 的数据格式为：`movieId,title,genres`

  `1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy`

- ratings.csv

  电影的评分数据，对应为事实表数据，包好25000095评分数据，ratings.csv 的数据格式为： `userId,movieId,rating,timestamp`

  `1,307,5.0,1147868828`

### 2. 需求

- 需求1：查找电影评分个数超过5000，并且平均分较高的前十部电影名称及其对应的平均评分
- 需求2：查找每个电影类别及其对应的平均分
- 需求3：查找被评分次数最多的前十部电影

### 3. 开发流程

1. 搭建项目
2. 读取数据源（hdfs上面）
3. 分别实现三个需求
4. 讲结果保存到外部（MySQL）

### 4. 打包上线

1. scala程序需要添加scala的打包插件

   ```xml
           <sourceDirectory>src/main/scala</sourceDirectory>
           <plugins>
               <!-- Scala 编译插件 -->
               <plugin>
                   <groupId>net.alchim31.maven</groupId>
                   <artifactId>scala-maven-plugin</artifactId>
                   <version>4.8.1</version>
                   <executions>
                       <execution>
                           <goals>
                               <goal>compile</goal>
                               <goal>testCompile</goal>
                           </goals>
                       </execution>
                   </executions>
               </plugin>
               <!-- 将依赖一起打进jar包的插件，另一种常用的插件是shaded -->
               <plugin>
                   <artifactId>maven-assembly-plugin</artifactId>
                   <configuration>
                       <descriptorRefs>
                           <descriptorRef>jar-with-dependencies</descriptorRef>
                       </descriptorRefs>
                   </configuration>
                   <executions>
                       <execution>
                           <id>make-assembly</id>
                           <phase>package</phase>
                           <goals>
                               <goal>single</goal>
                           </goals>
                       </execution>
                   </executions>
               </plugin>
           </plugins>
   
       
   ```

   

2. 可以通过指定profile的方式控制哪些包需要打进 jar 包中。（spark的相关包不需要引入，因为集群已经自带了）

<img src="./Apache Spark.assets/image-20250330115734974.png" alt="image-20250330115734974" style="zoom:80%;" />

3. 提交到集群，命令如下：（128 cores -->256线程）

   ```bash
   spark-submit --class com.example.spark.ClusterApp --master yarn --deploy-mode cluster --executor-memory 512M --num-executors 1 --executor-cores 2 spark_project-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://hadoop:9000/data/movies.csv hdfs://hadoop:9000/data/ratings_all.csv
   ```

   如果发现有问题，想在生产测试一下sql的结果，可以使用 --deploy-mode client ，会将一些driver的日志输出到控制台。

4. 后续，一般是会通过调度系统定时调度（比如airflow等）

## 附录

### 1. spark的historyServer

```bash
vim spark-defaults.conf
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://hadoop:9000/sparkHistory

vim spark-evn.sh
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://hadoop:9000/sparkHistory/"
```

创建hdfs目录： `hadoop fs -mkdir hdfs://hadoop:9000/sparkHistory/`

启动： `sh sbin/start-history-server.sh`



### 2. Spark的thriftserver

> spark on hive ： 通过spark来执行任务（spark作为sql入口），解析sql和执行sql都是由spark来完成，但是底层表的一些元数据信息由hive来提供（metastore）

跟hiveserver2一样，Spark也可以启动一个thriftserver进程，用于直接使用SparkSQL（不再需要创建项目，获取sparkSession之后再写SQL）

启动thriftServer之前需要先进行配置（已经在集成环境中配置好了）

1. 将hadoop和hive的配置文件放到spark的conf目录下（如果需要使用spark连接hive做操作的话）
2. 将hive-site.xml中的  `hive.metastore.schema.verification`设置为false
3. 将MySQL的驱动包放到spark的jars下
4. 启动 `sh sbin/start-thriftserver.sh`
5. 通过 spark安装目录下的 bin下的beeline进行连接  `bin/beeline -u jdbc:hive2://hadoop:10000 `

### 3. 项目jdk版本问题

<img src="./Apache Spark.assets/image-20250325210340576.png" alt="image-20250325210340576" style="zoom:80%;" />

### 4. checkpoint+kafka恢复任务

整体思路： 设置检查点 + 数据重放

spark streaming + kafka

1. 任务本身开启了checkpoint（在生产环境中，checkpoint路径一般是hdfs上的，利用hdfs的分布式和副本机制）

   ```java
   JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
           // spark 开启checkpoint检查点机制
           ssc.checkpoint(checkpointDirectory);
   ```

2. 重启任务的时候，一定是从上次结束的地方（检查点 checkpoint）继续

   ```java
           JavaStreamingContext ssc =
                   // 有就获取 —> checkpoint中有，就从checkpoint中获取
                   // 没有就新建
                   JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
   ```

3. 数据消费的时候，只有消费成功的时候才提交offset信息到kafka中

   ```java
   // Kafka参数配置
               Map<String, Object> kafkaParams = new HashMap<>();
               kafkaParams.put("bootstrap.servers", "kafka-broker1:9092,kafka-broker2:9092");
               kafkaParams.put("key.deserializer", StringDeserializer.class);
               kafkaParams.put("value.deserializer", StringDeserializer.class);
               kafkaParams.put("group.id", "spark-streaming-group");
               kafkaParams.put("auto.offset.reset", <从指定offset启动>);
               kafkaParams.put("enable.auto.commit", false); // 关闭自动提交
   // 中间处理数据
   // 处理完数据之后再提交offset
   ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
   ```

   
