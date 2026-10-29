# Kafka 第一篇《架构与核心概念》— 观点 ↔ 源码映射（2.8.2）

用途：记录文章每个观点的源码依据，供后续源码分析篇引用和复核。

- 源码仓库：`D:\workspace\learning\kafka`
- 版本：tag `2.8.2`（detached HEAD，commit `3146c6ff4a`）
- 行号为 2.8.2 源码中的行号，仅作定位参考

## 二、整体架构

| 文章观点 | 源码依据 |
| --- | --- |
| ZooKeeper 存储集群元数据（Topic、分区、副本分布）、选举 Controller、管理 Broker 上下线 | `core/src/main/scala/kafka/zk/KafkaZkClient.scala`：`tryCreateControllerZNodeAndIncrementEpoch`（141）原子创建 `/controller` 临时节点并递增 `/controller_epoch` 完成 Controller 选举；Broker 注册在 `/brokers/ids` 节点 |
| Controller 是某个 Broker 承担的职责，负责所有分区的 Leader 选举和 ISR 变更 | `core/src/main/scala/kafka/controller/KafkaController.scala`：`class KafkaController`（71）、`onBrokerFailure`（581）处理 Broker 故障并触发分区状态变更 |
| KRaft 模式可去掉 ZooKeeper，2.8.2 中是实验特性 | `core/src/main/scala/kafka/server/KafkaRaftServer.scala`（2.8.2 已存在 KRaft 入口，但默认仍走 ZooKeeper 模式） |

## 3.1 Topic 与 Partition

| 文章观点 | 源码依据 |
| --- | --- |
| 每个 Partition 是有序的、追加式日志，Offset 单调递增 | `core/src/main/scala/kafka/log/Log.scala`：`appendAsLeader`（1133）/ `appendAsFollower`（1148）/ `append`（1175），消息只追加不修改 |
| 改分区数会改变 key 到分区的映射 | `clients/src/main/java/org/apache/kafka/common/partition/DefaultPartitioner.java`：`partition()` 用 `numPartitions` 参与哈希取模 |

## 3.2 消息与 Offset

| 文章观点 | 源码依据 |
| --- | --- |
| 相同 key 落到同一分区 | `DefaultPartitioner.partition()`：`Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions` |
| 无 key 时粘住一个分区攒 batch，换新 batch 时换分区 | `DefaultPartitioner`：`StickyPartitionCache` 缓存当前分区；`KafkaProducer.doSend`：`result.abortForNewBatch` 时调 `partitioner.onNewBatch(topic, metadata)` 重新选分区 |
| 消费者记住 offset，故障后从该 offset 继续 | `KafkaConsumer.commitSync/commitAsync` → `core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala`：`handleCommitOffsets`（688）/ `doCommitOffsets`（751） |

## 3.3 副本：Leader 与 Follower

| 文章观点 | 源码依据 |
| --- | --- |
| Leader 处理该分区的所有读写请求 | `core/src/main/scala/kafka/cluster/Partition.scala`：`appendRecordsToLeader`（1054）、`readRecords`（1082）；非 Leader 写入被 `NotLeaderOrFollowerException` 拒绝（`core/src/main/scala/kafka/server/ReplicaManager.scala` 705） |
| 消费者拉取只从该分区 Leader 副本读（标准消费者），Follower 只复制不供数 | `ReplicaManager.scala`：`fetchMessages`（1018）→ `isFromFollower`/`isFromConsumer`（1028-1029）、`fetchOnlyFromLeader` 在「isFromFollower 或（isFromConsumer 且 clientMetadata 为空）」时为 true（1038）；标准消费者不带 ClientMetadata（消费端代码不引用）→ `fetchOnlyFromLeader=true`，`Partition.getLocalLog`（378-379）非 Leader 返回 `NOT_LEADER_OR_FOLLOWER`，客户端更新元数据后重发 Leader；非默认高级特性：broker 配 `ReplicaSelector` + 客户端带 rack 元数据时，Leader 返回 `preferredReadReplica` 提示客户端改发某 Follower（1136-1155） |
| Follower 从 Leader 拉取数据，被动复制 | `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala`：`class ReplicaFetcherThread`（46），继承 `AbstractFetcherThread`，周期性向 Leader 发 Fetch 请求 |
| RF=1 时没有 Follower，唯一副本自己就是 Leader | `core/src/main/scala/kafka/controller/PartitionStateMachine.scala`：`initializeLeaderAndIsrForPartitions`（270）初始化时取第一个存活副本为 Leader |
| Leader 宕机后 Controller 从 ISR 中选举新 Leader | `KafkaController.onBrokerFailure`（581）→ `PartitionStateMachine.handleStateChanges`（OnlinePartition 分支 → `electLeaderForPartitions`，324）→ `core/src/main/scala/kafka/controller/Election.scala`：`offlinePartitionLeaderElection`（38-39）从 ISR 选新 Leader |

## 3.4 ISR

| 文章观点 | 源码依据 |
| --- | --- |
| Follower 必须在规定时间内追上 Leader，否则被移出 ISR | `Partition.scala`：`maybeShrinkIsr`（941）、`isFollowerOutOfSync`（976，按 `replicaLagTimeMaxMs` 判断）、`getOutOfSyncReplicas`（998）；`replica.lag.time.max.ms` 默认 30000（`core/src/main/scala/kafka/server/KafkaConfig.scala` 149：`Defaults.ReplicaLagTimeMaxMs = 30000L`） |
| 选主只从 ISR 中选 | `Election.scala`：`PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection`（unclean 选举默认关闭时只从 ISR 候选） |
| min.insync.replicas + acks=all 保证不丢已确认数据 | `Partition.appendRecordsToLeader`（1054-1063）：`isr.size < minIsr && requiredAcks == -1` 时抛 `NotEnoughReplicasException`；`min.insync.replicas` 默认 1（`KafkaConfig.scala`：`Defaults.MinInSyncReplicas = 1`） |

## 3.5 消费组

| 文章观点 | 源码依据 |
| --- | --- |
| 每个分区只分配给组内一个消费者 | `clients/src/main/java/org/apache/kafka/clients/consumer/RangeAssignor.java`：`class RangeAssignor`（67，默认分配器）、`assign`（88）每个分区只分给一个消费者 |
| 消费者超过分区数时多出的空闲 | `RangeAssignor.assign`：分区只分给前 N 个消费者，其余消费者拿到空分配 |
| 不同消费组各自维护独立 offset | `GroupCoordinator.scala`：`class GroupCoordinator`（52）；offset 按 (group, topic, partition) 存于 `__consumer_offsets` |
| 消费者加入、离开、宕机触发 Rebalance | `GroupCoordinator`（心跳检查 + `session.timeout.ms`）+ `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java`（JoinGroup/LeaveGroup 再均衡协议） |

## 四、消息的完整流转

| 文章观点 | 源码依据 |
| --- | --- |
| 分区选择（显式指定 / key 哈希 / 无 key） | 同 3.2 |
| 写入 Leader | 同 3.3 |
| 副本同步 | 同 3.3（`ReplicaFetcherThread`） |
| 按 acks 配置确认返回 | `ReplicaManager.scala`：`appendRecords`（605）+ `DelayedProduce`（658，`delayedProducePurgatory` 212） |
| Consumer 从 Leader 副本按 offset 拉取（只走 Leader，详见 3.3） | `ReplicaManager.fetchMessages`（1018）→ `Partition.readRecords`（1082） |
| offset 提交到 `__consumer_offsets` | `clients/src/main/java/org/apache/kafka/common/internals/Topic.java`：`GROUP_METADATA_TOPIC_NAME = "__consumer_offsets"`（27）+ `GroupCoordinator.handleCommitOffsets`（688） |
| 拉模式 + long polling | `ReplicaManager.scala`：`delayedFetchPurgatory` / `DelayedFetch`（239）、`fetchMessages`（1018） |

## 五、基本使用

| 文章观点 | 源码依据 |
| --- | --- |
| Broker ID：`server.properties` 配置 `broker.id`，不配置时自动生成（从 1001 开始） | `KafkaConfig.scala`：`Defaults.BrokerId = -1`（66）、`BrokerIdGenerationEnable = true`（64）、`MaxReservedBrokerId = 1000`（65）；`core/src/main/scala/kafka/server/KafkaServer.scala`：`getOrGenerateBrokerId`（221），`generateBrokerId()` = ZK 序列号 + 1000，持久化到 `log.dirs/meta.properties` |
| `--describe` 输出：头部行含 `Topic`/`TopicId`/`PartitionCount`/`ReplicationFactor`/`Configs`，各分区行含 `Leader`/`Replicas`/`Isr` | `core/src/main/scala/kafka/admin/TopicCommand.scala`：`TopicDescription.printDescription`（114-123），topicId 非零时输出 `\tTopicId: $topicId`（117，KIP-516 引入，2.8+ 创建的 topic 都有） |
| `--bootstrap-server` 只需一个 Broker，客户端自动发现其他节点 | Producer：`KafkaProducer` 构造器（`clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java` 约 415 行）`ClientUtils.parseAndValidateAddresses` → `ProducerMetadata.bootstrap(addresses)`，`Sender` 线程维护元数据；Consumer：`KafkaConsumer`（KafkaConsumer.java 733 `metadata.bootstrap(addresses)`）+ `ConsumerNetworkClient`（762） |
| kafka-topics/kafka-configs 走 AdminClient，安全配置仅通过 `--command-config` 传入，不支持行内 `--producer-property`/`--consumer-property` | `TopicCommand.scala:617-618`：`commandConfigOpt` 定义 "Property file containing configs to be passed to Admin Client"；AdminClient 构造 `TopicCommand.scala:58`（`AdminClientTopicService(opts.commandConfig, opts.bootstrapServer)`）→ `createAdminClient`（224-225） |
| kafka-console-producer 通过 `--producer-property` 行内传安全配置 | `ConsoleProducer.scala:217`：`producerPropertyOpt = parser.accepts("producer-property", ...)`；`250`：`parseKeyValueArgs(options.valuesOf(producerPropertyOpt))` 解析为 Properties |
| kafka-console-consumer 通过 `--consumer-property` 行内传安全配置 | `ConsoleConsumer.scala:209`：`consumerPropertyOpt = parser.accepts("consumer-property", ...)`；`293`：`parseKeyValueArgs(options.valuesOf(consumerPropertyOpt))` 解析为 Properties |
| 支持 `--zookeeper` 的命令：`kafka-topics`、`kafka-configs`、`kafka-reassign-partitions` 等管理类命令，直连 ZooKeeper，由 ZK 自身的 SASL/JAAS 认证，不受 `security.protocol` 影响 | `TopicCommand.scala:378`：`KafkaZkClient(zkConnect.get, JaasUtils.isZkSaslEnabled, ...)` 建立 ZK 直连；`ConfigCommand.scala`、`ReassignPartitionsCommand.scala`、`PreferredReplicaLeaderElectionCommand.scala` 同样支持 `--zookeeper`；`kafka-console-producer`/`kafka-console-consumer` 不支持 `--zookeeper`（源码中未定义该选项） |
| `--zookeeper` 在 2.8.2 标记 DEPRECATED | `TopicCommand.scala:622`：选项描述以 "DEPRECATED," 开头；`742-743`：`Only one of --bootstrap-server or --zookeeper must be specified` |
| 控制台消费 `print.*` 展示参数 | `core/src/main/scala/kafka/tools/ConsoleConsumer.scala`（前次会话已核实） |

## 六、关键设计决策

| 文章观点 | 源码依据 |
| --- | --- |
| 追加式日志，顺序写 | `Log.scala`：`append` / `appendAsLeader` / `appendAsFollower`（1133/1148/1175） |
| OS 页缓存 | `Log.scala`：`read`（1617）→ `core/src/main/scala/kafka/log/LogSegment.scala`：`read`（290），读走 `FileChannel`（非 direct I/O），依赖 OS 页缓存 |
| 零拷贝 sendfile | `clients/src/main/java/org/apache/kafka/common/network/TransferableChannel.java`：`transferTo`（39-41，委托 `FileChannel.transferTo`，Linux 上即 sendfile）+ `PlaintextTransportLayer.java` |
| long polling 缓解轮询延迟 | 同第四节（`DelayedFetch`） |

| `--bootstrap-server` 与 `--broker-list` 效果相同（都传 `host:port` 列表），但支持范围和状态不同 | 见下方各命令定义行 |

`ConsoleProducer.scala:129`：`brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified")`；`133`：`bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified").requiredUnless("broker-list")`

`ConsoleConsumer.scala`：未定义 `--broker-list`，仅支持 `--bootstrap-server`

`TopicCommand.scala`：未定义 `--broker-list`，仅支持 `--bootstrap-server`（行 617）

`ConfigCommand.scala`：未定义 `--broker-list`，仅支持 `--bootstrap-server`（行 734 注解 "Replaced by --bootstrap-server"）

`ConsumerGroupCommand.scala`：未定义 `--broker-list`，仅支持 `--bootstrap-server`（行 989-1000 全部用 `--bootstrap-server`）

`DeleteRecordsCommand.scala:118`：仅定义 `--bootstrap-server`，未定义 `--broker-list`；走 AdminClient，安全配置同 `--command-config`（行 126-128）

`ReassignPartitionsCommand.scala:1823`：`brokerListOpt = parser.accepts("broker-list", "The list of brokers to which the partitions need to be reassigned")`，含义为 Broker ID 列表（数字 ID，如 `101,102,103`），不是连接地址

`LogDirsCommand.scala:110`：`brokerListOpt = parser.accepts("broker-list", "The list of brokers to be queried in the form \"0,1,2\"")`，含义同——Broker ID 列表

`ConsumerPerformance.scala:206`：`brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified")`；`210`：`bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified").requiredUnless("broker-list")`

`GetOffsetShell.scala:36`：仅支持 `--broker-list`（`REQUIRED: The list of hostname and port of the server to connect to`），未定义 `--bootstrap-server`

`ReplicaVerificationTool.scala:80`：仅支持 `--broker-list`（`REQUIRED: The list of hostname and port of the server to connect to`），未定义 `--bootstrap-server`

## 核实说明

- 以上全部基于 tag 2.8.2（commit 3146c6ff4a）逐条核实
- 前次会话已核实、本次引用的条目：`DefaultPartitioner`（murmur2 + sticky 分区）、`Partition.appendRecordsToLeader` 的 acks=all 检查、`broker.id` 自动生成、`ConsoleConsumer` 的 `print.*`、`--bootstrap-server` 与 `--broker-list` 支持范围对比
- 性能类描述（如"单机每秒数十万条消息"）与竞品对比（RabbitMQ）属通用描述，无特定源码依据
