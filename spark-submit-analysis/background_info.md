# Spark 提交流程总结 - 背景信息

## 源码项目

- **项目路径**：`D:\workspace\learning\spark`
- **分支**：`branch-3.2.3-comments`
- **版本**：Spark 3.2.3
- 以后所有源码分析都使用这个项目

## 背景

主要目的是学习总结 Spark 提交流程的源码，包括：
- Spark On Yarn
- Standalone 模式

之前已经大致了解这些流程，但并没有时间总结完对应的文档。

## 需求

1. 完成整个提交流程总结文档
2. 文档格式：markdown
3. 放在 `D:\hexo-blog\source\_posts\spark\` 目录下
4. 新创建的 md 文件时间设为当前时间
5. 命令行参数（如 `--proxy-user`、`--class`）用反引号包裹，避免被解析为链接
6. 源码中的重要注释需要同步更新到文档中，保持一致
7. 文档中的代码注释要和源码中的注释一致
8. **先修改源码注释，再同步文档**，避免不一致

## 已完成工作

### 第一篇总结
- 文件：`D:\hexo-blog\source\spark-submit-analysis\sparkSourceCode-script.md`
- 内容：Spark 脚本分析总结（spark-submit、spark-shell、spark-sql、start-master.sh、start-workers.sh、spark-daemon.sh、spark-class）
- 在源码中加了学习注释（提交记录：Spark 源码学习注释）

### PPT 内容（Spark关键组件L4定级.pptx）

共 9 张幻灯片：

1. **封面**：Spark关键组件L4定级
2. **Spark简介**
3. **组件架构图**（图片）
4. **相关概念**
5. **Yarn Client 提交流程**（图片）
6. **Yarn Cluster 提交流程**（图片）
7. **Standalone Client 提交流程**（图片）
8. **Standalone Cluster 提交流程**（图片）
9. **代码清单**

### PPT 文本内容

#### Slide 1: 封面
- 标题：Spark关键组件L4定级

#### Slide 2: Spark简介
- Spark 是一个分布式计算框架，用于大规模数据处理
- 提供了 Spark Core、Spark SQL、Spark Streaming、MLlib、GraphX 等组件

#### Slide 3: 组件架构图（图片）
- Driver
- SparkContext
- Cluster Manager
- Executor
- Task

#### Slide 4: 相关概念
- Driver：运行 Application 的 main() 函数，创建 SparkContext
- Executor：运行在 Worker 节点上的进程，负责执行 Task
- Cluster Manager：管理集群资源（Standalone、Yarn、Mesos、K8s）
- Task：执行器执行的工作单元
- Job：一个 Action 触发的一个或多个 Stage
- Stage：Shuffle 操作分隔的多个 TaskSet
- Application：用户编写的 Spark 程序

#### Slide 5-8: 提交流程图（图片）
由于模型不支持图片输入，无法直接解析图片内容。根据标题：
- Slide 5: Yarn Client 提交流程图
- Slide 6: Yarn Cluster 提交流程图
- Slide 7: Standalone Client 提交流程图
- Slide 8: Standalone Cluster 提交流程图

#### Slide 9: 代码清单
- 对应源码文件路径和关键类

### 核心源码文件（从PPT推断）
- SparkSubmit
- Client
- ApplicationMaster
- ExecutorLauncher
- DriverRunner
- Worker

## 已完成的第二篇总结

- 文件：`D:\hexo-blog\source\_posts\spark\sparkSourceCode-sparkSubmit.md`
- 内容：SparkSubmit 提交流程分析
- 主要内容：
  - SparkSubmit 入口和 doSubmit 方法
  - 参数解析 SparkSubmitArguments（详细解析过程见第三篇）
  - submit 和 prepareSubmitEnvironment 方法
  - 不同模式下的 childMainClass 对应关系
  - runMain 方法

## 已完成的第三篇总结

- 文件：`D:\hexo-blog\source\_posts\spark\sparkSourceCode-sparkSubmitArguments.md`
- 内容：SparkSubmitArguments 参数解析
- 源码提交：配合博客文章《Spark 源码 | SparkSubmitArguments 参数解析（三）》
- 主要内容：
  - SparkSubmitArguments 概述和字段定义
  - 解析流程：构造函数调用顺序
  - SparkSubmitOptionParser parse 方法解析逻辑（四步流程）
  - SparkSubmitArguments 重写的三个回调方法
  - 合并默认属性文件
  - 过滤非 Spark 属性
  - 加载环境变量参数
  - 参数验证
  - 参数优先级

## 已完成的第四篇总结

- 文件：`D:\hexo-blog\source\_posts\spark\sparkSourceCode-sparkContext.md`
- 内容：SparkContext 初始化和 TaskScheduler 创建流程
- 主要内容：
  - SparkContext 初始化流程
  - createTaskScheduler 方法
  - 不同模式的 SchedulerBackend 创建
  - YarnClientSchedulerBackend 创建

## 第五篇总结（待完成）

- Yarn Client 模式提交流程

## 参考资料：早期笔记整理

### spark-dkl-summary.md 分析总结

> **注意**：该笔记为早期学习总结，整理自 PDF 和 txt 文档，**不一定完全准确，也不一定全面**，仅作参考。后续文档总结需结合源码进行验证。

#### 1. 核心概念

- **核心角色**：client（客户端）、driver（运行 main 函数创建 SparkContext）、master（Standalone 集群管理器）、worker（Standalone 工作节点）
- **ApplicationMaster vs Driver**：
  - ApplicationMaster 是 YARN 框架概念，负责资源管理和任务协调
  - Driver 是 Spark 概念，负责应用程序逻辑控制和任务调度

#### 2. 核心类梳理

根据笔记整理的类（需结合源码验证）：

- 集群管理：ExternalClusterManager、YarnClusterManager
- YARN 相关：ApplicationMaster、YarnClusterApplication、ExecutorRunnable、YarnAllocator
- 任务调度：DAGScheduler、TaskScheduler
- Executor 相关：ExecutorBackend、CoarseGrainedExecutorBackend、YarnCoarseGrainedExecutorBackend
- RPC 通信：Endpoint、Dispatcher、Inbox、Outbox
- 其他：SparkSubmit、SparkContext

#### 3. 提交流程要点（待源码验证）

**Standalone Client 模式**：
1. SparkSubmit 提交，本地运行 Driver，初始化 SparkContext
2. Driver 向 Master 注册 Application
3. Master 分配资源，通知 Worker 启动 Executor
4. Executor 向 Driver 反注册
5. Driver 执行代码，提交任务

**Standalone Cluster 模式**：
1. SparkSubmit 启动 ClientApp
2. ClientApp 请求 Master 启动 Driver
3. Driver 向 Master 注册 Application
4. Master 分配资源，通知 Worker 启动 Executor
5. Executor 向 Driver 反注册
6. Driver 执行代码

**Yarn Client 模式**：
1. SparkSubmit 本地运行 Driver
2. Driver 向 RM 申请启动 ApplicationMaster
3. RM 在 NodeManager 上启动 Container 运行 ApplicationMaster
4. ApplicationMaster 向 RM 申请 Executor 资源
5. 在 NodeManager 上启动 Executor
6. Executor 向 Driver 反注册

**Yarn Cluster 模式**：与 Yarn Client 类似，区别在于 Driver 运行在集群中而非客户端

#### 4. 启动流程（待源码验证）

- **Master 启动**：spark-daemon.sh → spark-class → Master.main → NettyRpcEnvFactory → 注册 RpcEndpoint → 启动 WebUI → 定时检查 Worker 状态
- **Worker 启动**：spark-daemon.sh → spark-class → Worker.main → 注册 RpcEndpoint → 注册到 Master → 启动 WebUI
- **Driver 启动**：SparkSubmit → runMain → prepareSubmitEnvironment → 根据模式选择 childMainClass → app.start → ClientApp → 请求 Master 启动 Driver
- **Executor 启动**：Master schedule() → 通知 Worker → ExecutorRunner.start → CoarseGrainedExecutorBackend.main → 向 Driver 反注册

#### 5. 任务提交流程

```
DataFrame.count → DataSet.count → plan.executeCollect() → Rdd.collect()
→ sc.runJob → dagScheduler.runJob → submitJob → eventProcessLoop.post(JobSubmitted)
→ DAGSchedulerEventProcessLoop.onReceive → handleJobSubmitted
→ createResultStage → getOrCreateParentStages → createShuffleMapStage
→ submitStage → submitMissingTasks → taskScheduler.submitTasks
```

#### 6. 调试命令

```bash
# Yarn Client 模式
spark-submit --master yarn --deploy-mode client \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
  --class org.apache.spark.examples.SparkPi /path/to/spark-examples_2.12-3.2.3.jar

# Standalone 模式
spark-submit --master spark://host:7077 \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
  --class org.apache.spark.examples.SparkPi /path/to/spark-examples_2.12-3.2.3.jar
```

#### 7. 参考文章

- https://blog.csdn.net/weixin_45425054/article/details/114542750
- https://blog.csdn.net/luyllyl/category_7506344.html
- https://doc.hcs.huawei.com/zh-cn/productdesc/mrs/mrs_08_007101.html

## 当前进展（截至第五篇 Yarn Client）

### 第五篇 Yarn Client 已完成内容

**文件**：`D:\hexo-blog\source\_posts\spark\sparkSourceCode-yarn-client.md`

**已涵盖的源码分析**：
1. SparkSubmit runMain → JavaMainApplication 反射调用用户主类
2. SparkContext 初始化 → createTaskScheduler → YarnClusterManager → YarnClientSchedulerBackend（引用第四篇文章）
3. YarnClientSchedulerBackend.start() 完整源码注释（每个步骤编号说明）
4. Client.submitApplication() 详细源码分析：
   - yarnClient.createApplication() 获取 Application
   - createContainerLaunchContext() 创建 Container 启动上下文
   - createApplicationSubmissionContext() 创建提交上下文
   - yarnClient.submitApplication() 提交到 RM
5. Client 模式下 AM 启动的是 ExecutorLauncher（实际调用 ApplicationMaster.main）
6. ExecutorLauncher → ApplicationMaster.main → run() → runExecutorLauncher() 调用链
7. runExecutorLauncher() 与 Driver 通信、创建 YarnAllocator
8. YarnAllocator.allocateResources() → handleAllocatedContainers() → runAllocatedContainers()
9. ExecutorRunnable 通过 NMClient 启动 YarnCoarseGrainedExecutorBackend
10. YarnCoarseGrainedExecutorBackend 向 DriverEndpoint 注册（RegisterExecutor）
11. 注册成功后创建 Executor 计算引擎
12. Yarn Client vs Yarn Cluster 模式对比表（含 Driver 位置、AM 启动类、资源申请顺序等 10 个维度）
13. SchedulerBackend / ExecutorBackend 继承关系说明

**第五篇的覆盖范围说明**：
- YarnClientSchedulerBackend.start() 中只重点分析了 Client.submitApplication()（能体现 YARN 核心提交流程和 Client/Cluster 模式差异）
- 其他步骤（super.start()、配置获取、等待、监控等）在本文仅添加了完整注释说明其作用，未深入分析
- 后续如有必要，可能在 Yarn Cluster 或其他模式文章中单独补充分析这些步骤
- AM 启动后的流程（runExecutorLauncher → YarnAllocator → ExecutorRunnable → 注册 → 创建 Executor）已完整分析
- Job 提交和 Task 执行（DAGScheduler、TaskScheduler）标记为后续文章分析

**第五篇中定义的概念**：
- **Driver 定义**：用户主类的 main 方法执行的地方，Driver 是一个运行时角色，不是独立类
  - Yarn Client：childMainClass = args.mainClass，runMain 本地反射调用 → Driver 在客户端
  - Yarn Cluster：childMainClass = YarnClusterApplication，AM 内 runDriver() 调用 → Driver 在集群
- **isClusterMode 判断**：args.userClass != null（Cluster 传 --class，Client 不传）
- **ExecutorLauncher 命名**：仅用于运维区分 ps/jps 进程名，内部调用 ApplicationMaster.main

## 下一阶段工作

### 系列文章计划

1. **Yarn Cluster 模式提交流程**（第六篇）
   - ApplicationMaster.runDriver() 源码分析
   - AM 内部启动 Driver 的完整流程
2. **Standalone Client 模式提交流程**（第七篇）
3. **Standalone Cluster 模式提交流程**（第八篇）
4. **Promise + wait/notify 跨线程同步机制分析篇**（后续）
   - 深入分析 Yarn Cluster 模式中 Driver（用户线程）与 AM 主线程的跨线程同步机制
   - 覆盖：`sparkContextPromise`（`Promise[SparkContext]`）的创建与使用、`awaitResult` 阻塞等待、`sparkContextInitialized` 中的 `success` + `wait` 配对、`resumeDriver` 中的 `notify` 唤醒、异常时的 `tryFailure` / `trySuccess(null)` 兜底
   - 与 Java/Python 等语言中的 Promise/Future 模型对比，理解 Spark 选择此方式的考量
5. **RPC 通信源码分析篇**（待定）
   - 完整分析 Spark RPC 框架源码（NettyRpcEnv、Dispatcher、Inbox、Outbox 等）
   - 覆盖本文涉及的多处 RPC 通信：DriverEndpoint、YarnSchedulerEndpoint、Executor 反向注册等
   - 已有一篇 API 层面的总结文章 [Spark RPC 学习总结](../_posts/spark/sparkRpcLearning.md)，本篇将深入源码级别
6. **公共组件补充分析篇**（待定，位置选项见备注）
   - 补充分析 YarnClientSchedulerBackend.start() 中未深入的其他步骤：
     - super.start()：CoarseGrainedSchedulerBackend.start() 实现，创建 DriverEndpoint
     - waitForApplication()：轮询 Application 状态的实现
     - asyncMonitorApplication()：后台监控线程的实现
      - bindToYarn()、startBindings() 等
   - **位置选项**（根据实际写作情况确定）：
     - 方案一：放在 Yarn Client 和 Yarn Cluster 之间，作为"YARN 公共机制篇"
     - 方案二：放在所有模式之后、核心角色总结篇之前，作为"YARN 公共组件总结"
     - 方案三：先不单独成篇，根据后续实际需要决定
6. **Executor 启动与注册源码分析篇**（待定）
   - 深入分析 Executor 启动的完整流程，弥补第五篇（Yarn Client）的分析深度不足
   - 覆盖 Yarn 模式和 Standalone 模式的 Executor 启动差异对比：
     - Yarn 模式：ExecutorRunnable → YarnCoarseGrainedExecutorBackend → 反向注册
     - Standalone 模式：ExecutorRunner → CoarseGrainedExecutorBackend → 反向注册
   - 分析 Executor 启动后的关键初始化流程（SparkEnv 创建、BlockManager 注册等）
   - 分析 Executor 反向注册到 Driver 的完整 RPC 交互过程
8. **Job 提交与调度流程分析篇**（待定）
   - 分析各模式公共的 Job 提交流程：Action 算子触发 → DAGScheduler 划分 Stage → TaskScheduler 提交 Task → Executor 执行
   - 涉及的关键类：DAGScheduler、TaskSchedulerImpl、SchedulerBackend、Executor 等
   - 该流程与部署模式无关，是 Spark 核心调度逻辑
8. **核心角色与类总结篇**（待所有模式分析完成后）
    - 理清各模式下容易混淆的核心类和角色，聚焦继承关系和职责定位：
      - **SchedulerBackend 继承体系**：CoarseGrainedSchedulerBackend → YarnSchedulerBackend → YarnClientSchedulerBackend / StandaloneSchedulerBackend
      - **ExecutorBackend 继承体系**：CoarseGrainedExecutorBackend → YarnCoarseGrainedExecutorBackend
      - **启动入口类对比**：ExecutorLauncher（Yarn Client AM） vs ApplicationMaster（Yarn Cluster AM） vs ClientApp（Standalone Cluster）
      - **资源申请类对比**：YarnAllocator（Yarn） vs ExecutorRunner（Standalone）
      - **调度器对比**：YarnScheduler / StandaloneScheduler（TaskSchedulerImpl 的子类/实例）
      - **关键类与模式对应关系表**：不同模式下由哪个 ExternalClusterManager 创建什么 SchedulerBackend 和 TaskScheduler

> 后续文档总结时，需参考早期笔记的流程描述，但必须结合源码进行验证，确保准确性。