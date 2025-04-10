# Apache Spark 学习笔记

## 一、 Spark 介绍

> 官网：https://spark.apache.org/

Spark 是一个支持 多语言 客户端开发 的 **基于内存的** 通用并行计算框架，目的是 让数据分析更加快速。跟它相对的是其它的传统⼤数据技术 Hadoop的MapReduce以及Flink流式实时计算引擎等。

Spark包含了⼤数据领域常⻅的各种计算框架：⽐如Spark Core⽤于离线计算，Spark SQL⽤于SQL的交互式查询，Spark Streaming⽤于实时流式计算，Spark MLlib⽤于机器学习，Spark GraphX⽤于图计算等。

### 1. Spark的核心模块

<img src="./Apache Spark.assets/image-20250318213623587.png" alt="image-20250318213623587" style="zoom:80%;" />

- Spark core：Spark Core中提供了Spark最基础与最核⼼的功能，Spark 其他的功能如：Spark SQL、Spark Streaming、GraphX、MLlib 都是在 Spark Core的基础上进⾏扩展的。
- Spark SQL：通过SQL的方式来操作Spark读取的数据
- Spark Streaming：SparkStreaming 用于实时计算中（流处理），Spark streaming的处理思想是：只要我处理的批次间隔足够小，那么我就是实时处理（微批处理）
- Spark MLlib：机器学习相关的算法库。MLlib 不仅提供了模型评估、数据导⼊等额外的功能，还提供了⼀些更底层的机器学习原语。
- Spark GraphX：面向图计算的一些框架和算法库

### 2. Spark的特点

- 开发上手快：哪怕没有学习过MapReduce，只需要简单几行代码，就可以完成一个计算流程。（因为Spark封装了很多的方法以及算子）
- Spark支持多语言开发：java、scala、python、SQL
- 与hadoop可以无缝集成：Spark是一个通用的计算框架，本身是在hadoop之后发展出来的，spark+hadoop（hive）的组合是行业内大数据的主流组合。
- 活跃度高：虽然已经诞生很多年，但是目前还是非常火热的一个计算框架，经过了很多公司的生产实践。

### 3. Spark和MapReduce的对比

MapReduce: 基于磁盘的批处理模型，分为 Map和 Reduce两阶段，中间数据需写入**磁盘**。这种设计导致大量 I/O 开销，尤其对迭代计算效率较低。

Spark: 采用 内存计算和弹性分布式数据集（RDD）模型，中间结果可缓存于**内存**中复用，减少磁盘交互。通过 DAG（有向无环图）调度优化任务执行顺序，减少 Shuffle 次数。

|    **场景**    |        **MapReduce**（慢、稳）         |    **Spark**（快、不是特别稳）     |
| :------------: | :------------------------------------: | :--------------------------------: |
| **离线批处理** | ✔️ 海量数据 ETL、日志分析（低成本存储） | ✔️ 中小规模数据批处理（高速度要求） |
|  **迭代计算**  |       ❌ 效率低（需多次磁盘读写）       |  ✔️ 机器学习、图计算（如 GraphX）   |
| **实时流处理** |                ❌ 不支持                |   ✔️ 微批处理（Spark Streaming）    |
| **交互式查询** |                ❌ 延迟高                | ✔️ Spark SQL（低延迟 Ad-hoc 查询）  |

## 二、 Spark的快速入门

### 1. spark 的安装

如果在一个没有安装过Spark的系统中，需要安装Spark的话，可以遵循以下步骤

需要注意 Spark 和 hadoop 有版本对应关系

1. 下载安装包
2. 上传并解压

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

### 1. 创建maven工程

```xml
<!-- 需要引入spark-core的依赖 -->
		<dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_2.11</artifactId>
      <version>2.4.3</version>
    </dependency>
```

### 2. 创建sparkContext对象

```java
SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("my spark app");
JavaSparkContext sc = new JavaSparkContext(sparkConf);
```

### 3. 获取数据

```java
sc.textFile("file:///path/to/file");
sc.textFile("hdfs://192.168.56.101:8020/path/to/file");
```

### 4. 通用计算

transformation、action

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
| **coalesce**(*numPartitions*)                                | Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset. |

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

在spark中，所谓的分区（partition）就是指的是数据分布的物理单元。分区的数量会影响任务的数量，分区越多，task越多

并行度（parallelism）是任务执行的并发能力，并行度指的是同一时间内，有多少个task参与计算，并行度越高，通常任务的性能越好。

- 分区
  - 读取文件时，分区数按照文件的快大小分隔分区（hdfs）
  - 并行化集合时：由参数numSlices指定分区数（不指定的情况下默认为CPU核心数）
  - shuffle后：  `spark.sql.shuffle.partitions` 默认200，可以在spark sql客户端中通过set来设置
  - 自定义分区器：按照 numPartitions来控制分区数

- 并行度
  - 并行度取决于 executor数量（人） * 每个executor的CPU核心数（每个人同一时间能做几件事情）

合理的调整分区数大小和并行度大小，可以达到优化任务执行性能的效果

分区数不是越多越好，太多的话每个task操作的数据量很小，并且task需要多轮才能执行完成，太少的话，部分executor没活干（无法充分利用集群资源）

一般情况下，我们尽量让分区数接近spark集群运行时的可用核心数，避免资源限制或任务过载。

通常分区数为总并行度的1~4倍较为合理，当然也要考虑每个分区处理的数据量。





cache或者persist接口只有在遇到了action之后，才会触发真正的执行。

### 6. 持久化和缓存

持久化：就是把数据存储到一个地方（磁盘），需要用的时候再拿出来

缓存：就是把数据存储到一个地方（内存），需要用的时候再拿出来

通常持久化就是保留到永久存储介质，通常为磁盘；缓存就是保存到内存中。但是在spark中，不管是内存还是磁盘，在任务结束的时候都会销毁，所以spark中的持久化和缓存其实是相同的概念，只不过cache是特殊的persist

常用的是  MEMORY_ONLY（消耗内存多，但是块）、MEMORY_AND_DISK

<img src="./Apache Spark.assets/image-20250322115136723.png" alt="image-20250322115136723" style="zoom:80%;" />

cache本质就是 StorageLevel.MEMORY_ONLY 的persist

| torage Level                           | Meaning                                                      |
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

设置检查点（checkpoint）方式，本质上是将RDD写入磁盘进行存储。当RDD在进行宽依赖运算时，只需要在中间阶段设置一个检查点进行容错，即通过 Spark中的sparkContext对象调用setCheckpoint()方法，设置一个容错文件系统目录(如 HDFS)作为检查点checkpoint，将checkpoint 的数据写入之前设置的容错文件系统中进行高可用的持久化存储， 若是后面有节点出现宕机导致分区数据丢失,则可以从作为检查点的RDD开始重新计算，不需要进行从头到尾的计 算，这样就会减少开销。

简单理解，**Checkpoint** 就是一种将 RDD 或 DataFrame/Dataset 持久化到可靠存储（如 HDFS 或本地文件系统）的机制，用于切断 RDD 的血缘关系（Lineage），避免任务失败时从头重新计算。

checkpoint还有一个作用，可以用于中断任务后，重启任务加载上个任务的数据



通常的使用场景 是在 sparkStreaming中。

## 五、spark 程序运行

通常在开发的时候，会设置 master为 local，这样做是为了快速的在本地运行spark程序进行验证。

真实的工作中，开发完spark程序后，需要将程序打包并提交到集群中运行。

### 1. 打包程序

1. 将 `setMaster("local") `注释掉，因为我们要提交到集群中运行
2. 通过maven进行打包  `mvn clean pacakge `

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
|                 | yarn-client            | 等同于 `--master yarn --deploy-mode client`此时不需要指定 `deploy-mode`。 |
|                 | yarn-cluster           | 等同于 `--master yarn --deploy-mode cluster`此时不需要指定 `deploy-mode`。 |
| deploy-mode     | client                 | client 模式表示作业的 AM 会放在 Master 节点上运行（提交作业的节点本地jvm中）。如果设置此参数，需要指定 Master 为 yarn。 |
|                 | cluster                | cluster 模式表示 AM 会随机的在 Worker 节点中的任意一台上启动运行。如果设置此参数，需要指定 Master 为 yarn。 |
| driver-memory   | 4g                     | Driver 使用的内存，不可超过单机的总内存。                    |
| num-executors   | 2                      | 创建 Executor 的个数。                                       |
| executor-memory | 2g                     | 各个 Executor 使用的最大内存，不可以超过单机的最大可使用内存。 |
| executor-cores  | 2                      | 各个 Executor 使用的并发线程数目，即每个 Executor 最大可并发执行的 Task 数目。 |

<img src="./Apache Spark.assets/image-20250322110355871.png" alt="image-20250322110355871" style="zoom:80%;" />



涉及的一些概念：

- **Application**:一个main函数(一般来讲是一个SparkContext)所包含的所有代码就是一个Spark Application;
- **Job**:每执行一个action都会生成一个Job;
- **Stage**:一个Spark的job根据是否有宽依赖，划分stage。一般包含一到多个Stage。 
- **Task**:rdd的partition决定了task的数量，一个Stage包含一到多个Task，通过多个Task实现并行运行的功能。

完整运行过程说明如下:

**1.**用户提交任务 用户通过spark-submit提交任务，首先会启动一个Driver进程，其实就是我们编写的Spark程序的main()函数，同时会初始化SparkContext，进而初始化DAGScheduler和TaskScheduler等Spark内部关键组件;

**2.Driver**申请资源

Driver会向master申请资源，准备去执行Spark算子操作逻辑;

**3.Master**下发任务

Master收到Driver提交的作业请求之后，向Worker节点指派任务，其实就是让其启动对应的Executor进程;

**4.Worker**启动**Executor**进程

Worker节点收到Master节点发来的启动Executor进程任务，就启动对应的Executor进程，同时向Master汇 报启动成功，处于可以接收任务的状态;

**5.Executor**向**Driver**反向注册

当Executor进程启动成功后，就向Driver进程反向注册，以此来告诉Driver，谁可以接收任务，执行Spark作 业;Driver接收到注册之后，就知道了向谁发送Spark作业，这样在Spark集群中就有一组独立的executor进程为该driver服务;

**6.Driver**进行**Stage**划分和**Task**分发

SparkContext重要组件运行—DAGScheduler和TaskScheduler，DAGScheduler根据宽依赖将作业划分为若干stage，并为每一个阶段组装一批Task组成TaskSet(TaskSet里面就包含了序列化之后的我们编写的Spark transformation);然后将TaskSet交给TaskScheduler，由其将任务分发给对应的Executor;

**7.Executor**运行**Task** Executor进程接收到Driver发送过来的Task，进行反序列化，然后将这些Task放到本地线程池中，调度我们

的作业的执行。





# Apache Spark SQL

## 一、 SparkSQL 介绍

SparkSQL 是Spark 用来处理**结构化数据**的一个模块，可以通过SQL的方式访问和处理数据。它提供了一个叫做DataFrame的编程抽象结构数据模型，可以简单理解为 DataFrame = rdd + schema信息。SparkSQL底层有一个SQL的查询引擎，帮助用户将SQL翻译成底层的RDD编程模型，从而执行任务。

SparkSQL 前身是shark，但是shark过度依赖hive，导致很多方面无法进一步优化。

SparkSQL的特点：

1. 多数据源支持

   比如 json、csv、jdbc、hive等等结构化数据源都可以支持，包括数据的读和写以及数据分析

2. 无缝集成RDD

   SparkSQL虽然编程对象是DataFrame，但是他能够很轻松的转换为RDD。RDD也可以通过附加schema来转换为DataFrame。



## 二、 SparkSQL 编程模型

Spark SQL使用的数据抽象并非是RDD,而是DataFrame。在Spark1.3.0版本之前，DataFrame被称为 SchemaRDD。DataFrame使Spark具备了处理大規模结构化数据的能力。在Spark中，DataFrame是一种以RDD 为基础的分布式数据集，因此DataFrame可以完成RDD的绝大多数功能，在开发使用时，也可以调用方法将RDD 和DataFrame进行相互转换。DataFrame的结构类似于传统数据库的二维表格，并且可以从很多数据源中创建， 如结构化文件、外部数据库、Hive表等数据源。DataFrame与RDD在结构上的区别如下所示。

<img src="./Apache Spark.assets/image-20250322145823260.png" alt="image-20250322145823260" style="zoom:50%;" />

RDD是分布式的Java对象的集合，如上图所示的RDD[Person]数据集，虽然它以Person为类型参数，但是对象内部 之间的结构相对于Spark框架本身是无法得知的，这样在转换数据形式时效率相对较低。DataFrame除了提供比RDD更丰富的算子以外，更重要的特点是提升Spark框架执行效率、减少数据读 取时间以及优化执行计划。有了DataFrame这个更高层次的抽象后，处理数据就更加简单了,甚至可以直接用SQL来 处理数据，这对于开发者来说，易用性有了很大的提升。



SparkSQL 程序的入口不再是 `SparkContext`，而是`SparkSession`，SparkSession是在sparkcontext的基础上进一步封装，换句话说，sparkSession持有sparkContext并且还有其他功能。 



DataFrame和dataset的关系

```scala
  type DataFrame = Dataset[Row]
```

可以认为，Spark中的DataFrame就是特殊的dataset（类型为Row的dataset）。

Dataset是类型安全的（或者说Dataset是强类型的），而DataFrame则是早期只有类型为Row的一种数据结构，后续被Dataset取代。

### 1. 创建DataFrame的方式

1. 通过自定义schema结构来创建一个DataFrame
2. 通过实体类创建DataFrame
3. 通过外部文件创建DataFrame
4. 通过jdbc读取数据库的表（外部连接器）（MongoDB、es  https://spark.apache.org/third-party-projects.html）

### 2. 对DataFrame做操作

有两种方式可以对DataFrame进行操作，一种叫 DSL（domain spec language），一种叫SQL。通常来说，我们习惯直接使用SQL的方式进行操作，DSL比较少用。

具体用法参考代码。

### 3. 输出

1. 输出到控制台（show、collect+print）
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

应用场景

- 实时推荐系统
- 金融交易监控
- 网络安全监控
- 社交媒体趋势分析
- ....

## 二、 Spark streaming介绍

<img src="./Apache Spark.assets/image-20250325211945822.png" alt="image-20250325211945822" style="zoom:80%;" />

数据是源源不断产生的，我们通过SparkStreaming实时接收这种数据，并通过将数据进行切分的方式来处理。

### 1. 流处理思想

一个无边界的数据流，只要我们按照时间片段（一般是比较短的时间片段）进行切割，就可以变成无数多个 有边界的数据，这个有边界的数据在Spark中就是 RDD

<img src="./Apache Spark.assets/image-20250325213017249.png" alt="image-20250325213017249" style="zoom:80%;" />

`JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(5));`

这里的第二个参数，就是控制时间片段的大小，通常是按照秒级切分

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

如果即想使用状态，又怕状态无限膨胀，最佳实践是使用额外存储作为状态后端。



3. 窗口操作

   > 滑动窗口

   每间隔多长时间，统计多大时间窗口的数据

   比如：每5分钟 统计过去一小时的销售量/额

<img src="./Apache Spark.assets/image-20250329104359232.png" alt="image-20250329104359232" style="zoom:80%;" />

4. 累加器、广播变量、Checkpoint故障恢复

Accumulators, Broadcast Variables, and Checkpoints

- 累加器（executor只写）
  - executor只能对累加器做累加的动作
  - 多个executor在处理数据的时候，都对累加器做了操作，但是不会产生安全问题，因为spark帮我们保证了（一致性协议）
  - 一般用于统计
- 广播变量（executor只读）
  - 广播变量由driver创建并广播，executor只能读取值，不能修改值
  - 广播变量广播之后，会在每个executor中保存一份
  - 广播变量发送给executor只会发送一次，不会因为每个executor由多个task而发送多次

- 从checkpoint重启spark streaming程序
  - 累加状态被重置了
    - 依赖外部存储（redis、hbase），重启的时候从外部初始化初始值
  - 假设streaming挂了，但是socket还在不断的发数据
    挂了到重启的这段时间的数据就会丢失
    - 回放数据（socket数据源不支持） --> 使用支持回放的数据源，比如kafka，通过offset机制来保证 



### 4. 数据的输出

1. println打印到控制台 （本地调试）

2. 保存到文件 （streaming用的比较少，spark core用的多，尤其是保存到hdfs）  saveAsTextFiles  saveAsNewAPIHadoopFiles saveAsHadoopFiles

3. 保存到外部的存储（数据库、kv存储）

   ```java
   resultDstream.foreachRDD(rdd -> {
     // 获取数据库连接（连接池中get connection
     // insert or update 到数据库
     rdd.foreach(record ->{
     	saveToDb()
     })
   });
   ```

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

   

