# 大数据平台研发岗 · 面试备考主清单

> 目标：联通软研院、国核信息等央企普通大数据平台开发岗（非架构师）
> 区分：平台研发偏底层底座（组件封装、任务调度、资源、权限），非业务数仓ETL

---

## 进度总表

| # | 考点 | 优先级 | 口述模板 | 详细总结 | 状态 |
|---|------|--------|----------|----------|------|
| | **一、Hadoop 生态基础** | | | | |
| 1 | HDFS 读写流程 / NameNode / DataNode / SecondaryNameNode | 高 | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 2 | HDFS 小文件问题（产生原因/危害/解决方案） | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 3 | HDFS 存储策略、权限、副本、线上故障 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 4 | YARN 组件：RM / NM / AM 工作流程 | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 5 | YARN 调度器：FIFO / Capacity / Fair + 多租户 | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 6 | 任务 OOM 排查（容器内存参数、-Xmx） | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 7 | YARN 资源抢占、节点标签 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 8 | Hive 存储格式：ORC vs Parquet | 中 | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 9 | Hive 分区/分桶/索引/Metastore | 低 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 10 | Hive 数据倾斜 / SQL 调优 / Ranger 权限 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| | **二、Spark** | | | | |
| 11 | RDD / Dataset / DataFrame 区别 + 惰性求值 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 12 | 宽窄依赖、血缘 Lineage | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 13 | Shuffle 原理、排序、调优、磁盘溢出 | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 14 | **数据倾斜**：产生原因、定位、全套解决方案 | **高** | ✅ 主清单 | ✅ 已完成 | ✅ 完成 |
| 15 | Cache/Persist vs Checkpoint | **高** | ✅ 已有文章 | ✅ 已总结 | ✅ 完成 |
| 16 | Spark on Yarn Client vs Cluster | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| | **三、Flink** | | | | |
| 17 | **Checkpoint 原理**（barrier 对齐/非对齐/RocksDB 增量） | **高** | ✅ 已有文章 | ✅ 源码分析系列 | 🔄 进行中 |
| 18 | Checkpoint vs Savepoint | 中 | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 19 | State 状态后端（Memory / FS / RocksDB） | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 20 | Watermark 水印、乱序数据处理 | 中 | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 21 | Flink on Yarn（session / per-job / application） | **高** | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 22 | **反压原理、排查定位** | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| | **四、数据同步 / 中间件** | | | | |
| 23 | Kafka 分区、副本、ISR 机制 | 中 | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 24 | Kafka 重复消费 / 丢数据场景 | 中 | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 25 | CDC（Flink-CDC binlog 增量同步） | **高** | ⏳ | ⏳ 待整理 | 📋 主清单 |
| | **五、平台类特有问题** | | | | |
| 26 | DolphinScheduler 调度原理（依赖/重试/补数） | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 27 | Ranger 权限体系（含 Kerberos/KDC/LDAP/Ranger-KMS） | **高** | ✅ 主清单 | ✅ topic-27 已整理 | ✅ 完成 |
| 28 | Atlas 元数据、血缘采集 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| 29 | 平台任务监控告警 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |
| | **六、项目&故障排查** | | | | |
| 30 | 集群迁移方案（1PB/上千Hive表） | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| 31 | 线上问题排查（OOM/慢任务/CP超时/Shuffle打满） | **高** | ✅ 主清单 | ⏳ 待整理 | 📋 主清单 |
| | **七、Java 基础** | | | | |
| 32 | JVM 内存模型、GC、OOM 场景 | 中 | ⏳ | ⏳ 待整理 | 📋 主清单 |

**状态说明：**
- 📋 主清单 — 考点在主清单中登记，待整理
- 🔄 进行中 — 已有部分总结，正在完善
- ✅ 完成 — 口述模板和详细总结均已完成

---

## 一、Hadoop 生态基础

### 1. HDFS 读写流程 / NameNode / DataNode / SecondaryNameNode

**优先级**：高

**掌握程度**：概念清晰，口述流畅，不用深入源码

**要点**：
- HDFS 读写流程（客户端 → NN 获取元数据 → DN 读写数据）
- NameNode 作用：管理元数据（文件目录树、块位置）
- DataNode 作用：存储实际数据块
- SecondaryNameNode：不是 NN 热备，只做元数据合并检查点（合并 edits + fsimage）

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 2. HDFS 小文件问题

**优先级**：高（高频）

**掌握程度**：能完整说出原因、危害、多种解决方案

**要点**：
- 产生原因：流式写入未合并、任务输出大量小文件
- 危害：压垮 NN 内存（每个文件约 150 字节）、MR 任务数过多
- 方案：源头避免、任务后合并、CombineFileInputFormat、Hive 合并、Spark 合并(coalesce/repartition)

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

**待详细总结**：⚠️ 需要展开

---

### 3. HDFS 存储策略、权限、副本、线上故障

**优先级**：中

**掌握程度**：结合实际踩坑说出 1-2 个案例即可

**要点**：
- 副本机制（默认 3 副本，机架感知）
- 存储策略（HOT/COLD/LAZY）
- 权限（POSIX 风格 + ACL）
- 线上故障：磁盘故障、DN 宕机、NN 元数据损坏

---

### 4. YARN 组件：RM / NM / AM 工作流程

**优先级**：高（平台岗超级高频）

**掌握程度**：三组件职责 + 交互流程清晰

**要点**：
- RM：全局资源管理器
- NM：节点代理，管理 container
- AM：每个应用的管家，向 RM 申请资源，管理本任务 container
- 交互流程：提交 → RM 分配 AM → AM 注册 → AM 申请 container → NM 启动

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 5. YARN 调度器

**优先级**：高

**掌握程度**：三种调度器区别 + 实际使用

**要点**：
- FIFO：先来先服务，不隔离
- Capacity：队列式划分，适合多租户（央企常用）
- Fair：按权重公平分配，支持抢占
- 多租户：YARN 队列划分 + ACL 控制

**注意**：多解释为什么央企平台用 Capacity（资源隔离好、管理简单）

---

### 6. 任务 OOM 排查思路

**优先级**：高

**掌握程度**：能说清楚定位步骤，结合生产经验

**要点**：
- 第一步：看日志区分 Driver OOM 还是 Executor OOM
- 核对 yarn 容器内存、JVM 堆内存参数
- 数据倾斜 / Shuffle 数据量大 → 增加分区
- 大状态任务 → RocksDB + 增量 Checkpoint

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 7. YARN 资源抢占、节点标签

**优先级**：中

**掌握程度**：了解概念和使用场景即可

---

### 8. Hive 存储格式：ORC vs Parquet

**优先级**：中

**掌握程度**：对比关键差异，不用深入底层

**要点**：
- 都是列式存储
- ORC：Hive 生态好，索引能力强
- Parquet：跨引擎兼容性强，Spark 用得多
- 两者都支持压缩、谓词下推

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 9. Hive 分区/分桶/索引/Metastore

**优先级**：低

**掌握程度**：概念了解

---

### 10. Hive 数据倾斜 / SQL 调优 / Ranger 权限

**优先级**：中

---

## 二、计算引擎 · Spark

### 11. RDD / Dataset / DataFrame 区别 + 惰性求值

**优先级**：中

---

### 12. 宽窄依赖、血缘 Lineage

**优先级**：中

**关联**：面试秒接上一篇 Cache/Persist vs Checkpoint（切血缘 vs 保留血缘）

---

### 13. Shuffle 原理、排序、调优、磁盘溢出

**优先级**：高（高频）

**要点**：
- Shuffle = 数据重分区（相同 key 分发到同一下游）
- 排序：sort-based shuffle（默认），支持 map-side combine
- 调优：缓冲区大小、merge 策略、溢出路径
- 磁盘溢出：spill 过多 → IO 瓶颈

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

**待详细总结**：⚠️ 需要展开（shuffle 写/读全链路）

---

### 14. 数据倾斜：产生原因、定位、全套解决方案

**优先级**：高（必问）

**要点**：
- 原因：个别 key 数据量远大于其他 key
- 定位：看 stages 中各 task 数据量（Web UI）
- 方案：过滤无效 key、加盐打散（两阶段聚合）、大 key 拆分、调整并行度、AQE 倾斜优化

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

**待详细总结**：⚠️ 需要展开（多种方案 + 适用场景）

---

### 15. Cache/Persist vs Checkpoint

**优先级**：高

**已有文章**：`_posts/spark/sparkSourceCode-sparkContext.md`
- Cache/Persist：内存/磁盘缓存，保留血缘（可重算），任务结束自动清理
- Checkpoint：持久化到 HDFS，切断血缘（容错），手动清理

**状态**：✅ 已有文章总结，无需额外整理

---

### 16. Spark on Yarn Client vs Cluster

**优先级**：高（平台开发重点）

**要点**：
- Client：Driver 跑在提交机器，方便调试日志
- Cluster：Driver 跑在 YARN 容器，生产用
- 平台通常用 Cluster 模式

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

**待详细总结**：⚠️ 需要展开（提交流程全链路）

---

## 三、计算引擎 · Flink

### 17. Checkpoint 原理

**优先级**：高（必问）

**已有文章**：`_posts/flink/flink-checkpoint-concept.md`（系列第一篇）
- Barrier 机制、对齐/非对齐
- RocksDB 增量 Checkpoint
- Chandy-Lamport 分布式快照算法
- 完整执行流程（7 阶段）

**状态**：🔄 进行中（已有一篇概念篇，后续源码分析篇待完成）

**下一阶段**：
- 第二篇：CheckpointCoordinator 源码分析（触发链路）
- 第三篇：Barrier 对齐与状态快照源码
- 第四篇：StateBackend 源码分析（HashMap vs RocksDB）

---

### 18. Checkpoint vs Savepoint

**优先级**：中

**要点**：
- Checkpoint：自动周期性触发，用于故障自动恢复
- Savepoint：手动触发，用于版本升级、任务停改
- Savepoint 存储格式更重，可跨版本恢复

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 19. State 状态后端

**优先级**：高

**要点**：
- Memory：测试用，状态存 TM 内存
- FileSystem：小状态，存 HDFS
- RocksDB：大状态，支持增量 Checkpoint，生产最常用

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

**待详细总结**：⚠️ 需要展开（三种后端对比 + 选型依据）

---

### 20. Watermark 水印、乱序数据处理

**优先级**：中

**要点**：
- 水印标记事件时间水位
- 水印到达窗口结束时间才触发窗口计算
- 迟到数据可配置侧输出流收集

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 21. Flink on Yarn（session / per-job / application）

**优先级**：高

---

### 22. 反压原理、排查定位

**优先级**：高（必问）

**要点**：
- 上游生产 > 下游处理能力
- Flink 自带反压机制向上游限速
- 排查：Web UI 反压指标 → 定位慢算子 / 外部 IO / Checkpoint 压力

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

**待详细总结**：⚠️ 需要展开（反压传播机制 + 定位方法论）

---

## 四、数据同步 / 中间件

### 23. Kafka 分区、副本、ISR 机制

**优先级**：中

**要点**：
- 分区 = 并行度
- 副本 = 高可用
- ISR = 同步副本集合，只有 ISR 内副本可选 leader

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 24. Kafka 重复消费 / 丢数据场景

**优先级**：中

**要点**：
- 重复消费 → offset 提交时机问题
- 丢数据 → acks 参数、min.insync.replicas

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 25. CDC（Flink-CDC binlog 增量同步）

**优先级**：高（你的加分项）

**已有文章**：`_posts/flink/` 目录下有 flinkOracleCDC.md、flinkCDC2Kafka.md 等

**待详细总结**：⚠️ 需要整理成面试答题版本

---

## 五、平台类特有问题

### 26. DolphinScheduler 调度原理

**优先级**：高

**要点**：
- 大数据工作流调度平台
- 管理任务依赖、定时调度、失败重试、补数
- 可调度 spark/flink/shell 任务

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 27. Ranger 权限体系

**优先级**：高（央企必用）

**要点**：
- Hive / HDFS / Kafka 列、行级别权限
- 策略下发到各组件做鉴权
- 多租户平台常用

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

### 28. Atlas 元数据、血缘采集

**优先级**：中

---

### 29. 平台任务监控告警

**优先级**：中

---

## 六、项目 & 故障排查

### 30. 集群迁移方案

**优先级**：高（你的核心加分项）

**口述版本**（见 interview-quick-answers.md）：
- 元数据迁移 → 分批数据迁移 → 双跑校验 → 业务切换
- 1PB 数据、上千 Hive 表
- 遇到小文件、参数不兼容 → 适配解决

**待详细总结**：⚠️ 需要展开成完整的项目介绍

---

### 31. 线上问题排查

**优先级**：高

**要点**：
- 任务 OOM 排查
- 任务慢定位步骤
- Checkpoint 超时失败排查
- Shuffle 磁盘打满、小文件泛滥处理

**已有资料**：面试快速答题模板（见 interview-quick-answers.md）

---

## 七、Java 基础

### 32. JVM 内存模型、GC、OOM 场景

**优先级**：中

**要点**：
- 堆内存、栈、方法区
- GC 算法（CMS/G1/ZGC）
- OOM 常见场景（堆溢出、直接内存、元空间）
- 与大数据结合（Spark Executor 内存模型）

---

## 八、面试技巧

### 遇到没答上来的题

> 这个场景我实际项目接触不多，原理我有了解，平时工作中主要聚焦在 xxx（马上转到你做过的 Flink/Spark 迁移开发），如果工作遇到这块，我可以快速学习落地。

### 加分技巧

1. 不用一字不差背，记住关键词，口语化表达
2. 不要讲底层源码和论文算法（那是架构师岗）
3. **多带上生产、线上、踩坑调优**，这是 10 年经验优势
4. 架构和管理类问题坦诚说明没有经验，拉回工程落地

---

## 备考取舍建议

### 优先重点（高频必问）
YARN 调度机制、HDFS 小文件、Spark Shuffle & 数据倾斜、Flink Checkpoint & State & 反压、Ranger 权限、DolphinScheduler 调度、CDC 同步、集群迁移真实项目

### 次重点
Kafka、Hive 存储格式、Atlas 血缘、JVM 调优

### 了解即可（不花大量时间）
底层源码深度阅读、平台整体架构设计（架构师范畴）
