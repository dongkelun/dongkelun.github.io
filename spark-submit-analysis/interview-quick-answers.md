# 大数据平台研发岗 · 口述答题模板

> 面试口头直接说，**拒绝大段背诵，口语化，结合线上生产经验**
> 只记核心要点，面试打分看逻辑，不是背诵原文

---

## 一、Flink

### 1. Flink Checkpoint 原理

> Checkpoint 是 Flink 的分布式快照，用来实现故障恢复，保障端到端一致性。
>
> JobManager 的 CheckpointCoordinator 定时触发，向 Source 发送信号；Source 生成 barrier 插入数据流，沿拓扑向下游传播。
>
> 算子收到 barrier 后把自己的状态保存到 StateBackend（HashMap 或 RocksDB），Checkpoint Storage 决定快照写到哪——一般配 `state.checkpoints.dir` 存 HDFS。
>
> 对齐 checkpoint 会阻塞通道等所有 barrier 到齐，反压高时建议开非对齐（barrier 到了就快照，不阻塞）。
>
> 注意：仅靠 checkpoint 只能保证内部状态一致，想要 Exactly-Once，还需要 sink 端配合二阶段提交。

### 2. Checkpoint 和 Savepoint 区别

> Checkpoint 是自动周期性触发，用于任务故障恢复，用 StateBackend 原生格式，不能跨版本兼容。Savepoint 是手动触发，用于升级、迁移，默认 CANONICAL 格式可跨版本兼容，也可指定 NATIVE 格式（同 Checkpoint 性能）。

### 3. 反压是什么，怎么排查

> 反压就是上游生产速度大于下游处理速度，数据堆积。
>
> Flink 自带反压机制会向上游限速。排查先看 webui 的反压指标，定位是算子逻辑慢、外部 IO 慢，还是 checkpoint 压力大，再针对性调优。

### 4. State 状态后端

> 现在 Flink 只有两种：HashMapStateBackend（小状态，存堆内存）和 EmbeddedRocksDBStateBackend（大状态，存本地磁盘 + 增量 checkpoint，生产最常用）。旧版的 Memory、FileSystem 已弃用，它们各自被拆分为 StateBackend + Checkpoint Storage 两个概念。

### 5. Watermark 水印

> 水印用来处理乱序数据，标记当前事件时间水位；水印到达窗口结束时间，才触发窗口计算。设置允许的乱序时间，迟到数据可以配置侧输出流收集。

---

## 二、Spark

### 1. Cache/Persist 和 Checkpoint 区别

> Cache/Persist 是内存磁盘缓存，保留 RDD 血缘，作业结束数据自动清理，做加速计算。
>
> Checkpoint 把数据持久化到 HDFS，会切断血缘，用于容错，数据不会随任务结束删除。

### 2. Spark Shuffle 原理、数据倾斜

> Shuffle 是数据重分区，把相同 key 的数据分发到同一个下游节点。
>
> 数据倾斜是个别 key 数据量远大于其他 key。
>
> 排查看任务各个 stage 的 task 数据量。处理：过滤无效 key、加盐打散、大 key 拆分、调整并行度、开启倾斜优化。

### 3. Spark on Yarn Client 和 Cluster 区别

> Client 模式驱动跑在提交机器，方便调试日志；Cluster 模式 Driver 跑在 Yarn 容器，生产环境正式任务用 cluster。

### 4. Structured Streaming Checkpoint 作用

> 持久保存 kafka offset、算子状态。任务重启之后从该目录恢复，实现故障恢复；注意这个目录不能手动删除，删了任务无法恢复。

---

## 三、Hadoop 生态

### 1. HDFS 读写流程

> 客户端先请求 NameNode 获取元数据；读的时候直接去 DataNode 拉取块数据；写数据 NN 分配块位置，客户端写入多个副本。
>
> SecondaryNameNode 不是 NN 备份，只是做元数据合并检查点。

### 2. HDFS 小文件问题

> 大量小文件会压垮 NameNode 内存。
>
> 方案：源头尽量避免产生小文件；任务后合并小文件；使用 Combine 输入格式。

### 3. YARN 组件作用

> RM 全局资源管理器；NM 是节点代理；ApplicationMaster 是每个应用自己的管理者，向 RM 申请资源，管理本任务的容器。
>
> 调度器生产一般用 Capacity 调度器，支持多租户资源划分。

### 4. 任务 OOM 排查思路

> 第一步看日志分清是 Driver OOM 还是 Executor 容器 OOM。
>
> 先核对 yarn 容器内存、JVM 堆内存参数；看是否数据倾斜、shuffle 数据量过大；大状态任务检查状态后端配置。

### 5. ORC 和 Parquet 区别

> 都是列式存储。ORC 索引能力好，hive 生态适配更好；Parquet 跨引擎兼容性更强，Spark 用得多。两者都支持压缩、谓词下推。

---

## 四、Kafka

### 1. Kafka 分区、副本、ISR

> 分区实现并行消费；副本做高可用；ISR 是同步副本集合，只有 ISR 内副本才可以选 leader。
>
> 消费重复大多是 offset 提交时机问题；丢数据和 acks 参数有关系。

---

## 五、平台组件

### 1. DolphinScheduler 调度

> 大数据工作流调度平台，管理任务依赖、定时调度、失败重试、补数。可以调度 spark、flink、shell 任务。

### 2. Ranger 权限管控

> 做数据权限管控，可以给 Hive、HDFS、Kafka 配置列、行级别权限，策略下发到各个组件做鉴权，多租户平台常用。

---

## 六、集群迁移项目（核心加分项，高频深挖）

> 我做过大数据集群迁移项目，大概 1PB 数据，上千张 Hive 表。
>
> 整体方案先做元数据迁移，再分批迁移数据，双跑校验数据一致性；迁移过程重点管控业务任务，尽量减少业务影响。
>
> 过程遇到过小文件、任务参数不兼容问题，做了参数适配、小文件合并，最后完成全量业务切换。

---

## 七、万能话术（遇到没答上来的题）

> 这个场景我实际项目接触不多，原理我有了解，平时工作中主要聚焦在 xxx（马上转到你做过的 Flink/Spark 迁移开发），如果工作遇到这块，我可以快速学习落地。

---

### Spark 数据倾斜

> 数据倾斜是指个别 Task 数据量远大于其他 Task，导致任务被拖慢。
>
> 常见场景：join 时某个 key 数据量特别大，或聚合时某个维度数据量异常。
>
> 定位先看 Spark UI 的 Task 数据量，哪个 Task 数据量最大就是倾斜的 key。
>
> 处理方案：
> - 无效数据（null、空值）直接过滤
> - 小表 join 大表用 broadcast 避免 shuffle
> - 加盐打散，两阶段聚合
> - 大 key 单独处理
> - 打开 AQE，Spark 3.0 会自动处理倾斜
>
> 优先用 broadcast，不行再考虑加盐打散。AQE 打开后很多场景能自动优化。

## 八、使用提示

1. 不用一字不差背，记住关键词，口语说出来
2. 普通开发岗，不用讲底层源码、论文算法，**多带上生产、线上、踩坑调优**
3. 架构、管理类的问题直接坦诚说明没有相关经验，把话题拉回工程落地
