# Spark 提交流程笔记整理

## 1. Spark 简介

Apache Spark 是用于大规模数据（large-scala data）处理的统一（unified）分析引擎。简而言之，Spark 借鉴了 MapReduce 思想发展而来，保留了其分布式并行计算的优点并改进了其明显的缺陷。让中间数据存储在内存中提高了运行速度、并提供丰富的操作数据的 API 提高了开发速度。

Spark 是一款分布式内存计算的统一分析引擎。其特点就是对任意类型的数据进行自定义计算。

Spark 可以计算：结构化、半结构化、非结构化等各种类型的数据结构，同时也支持使用 Python、Java、Scala、R 以及 SQL 语言去开发应用程序计算数据。

## 2. 核心角色

| 角色 | 说明 |
|------|------|
| client | 客户端，提交应用的机器 |
| driver | 运行 Application 的 main() 函数，创建 SparkContext |
| master | Standalone 模式下的集群管理器 |
| worker | Standalone 模式下的工作节点 |

## 3. ApplicationMaster 与 Driver 的区别

| 区别点 | ApplicationMaster | Driver |
|--------|-------------------|--------|
| 框架差异 | 与 Hadoop YARN 等分布式计算框架相关的概念 | Spark 这类计算框架的概念 |
| 功能差异 | 负责资源的管理和任务的协调 | 负责 Spark 应用程序的逻辑控制和任务的调度 |
| 角色差异 | 运行在 YARN 集群中的一个独立组件 | Spark 应用程序内的组件，通常运行在集群中的某个节点上 |

## 4. 核心类

- ExternalClusterManager
- YarnClusterManager
- ApplicationMaster
- TaskSet
- DAGScheduler
- TaskScheduler
- 基于 RPC 的 Endpoint、Dispatcher、Inbox 和 Outbox 的交互机制
- YarnClusterApplication
- SparkSubmit
- ExecutorRunnable
- SparkContext
- ExecutorBackend
  - CoarseGrainedExecutorBackend
    - YarnCoarseGrainedExecutorBackend

## 5. Yarn 流程

```
YarnAllocator -> runAllocatedContainers -> launcherPool -> ExecutorRunnable -> NMClient -> startContainer
```

LauncherPool 为线程池，它会启动一个线程类 NMclient 用来和 NodeManager 通信用，然后会启动 NodeManager 中的一个进程 ExecutorBackend，然后启动两个通信模块（也就是 RPC 终端，用来通信）。通信注册完后，建立 Executor 计算对象（线程），也就是跑 RDD 的承担者。

## 6. On Yarn 优势

1. 运行于 Yarn 的框架都共享一个集中配置好的资源池
2. 可以很方便地利用 Yarn 的资源调度特性来做分类，隔离以及优先级控制负载，拥有更灵活的调度策略
3. Yarn 可以自由地选择 executor 数量
4. Yarn 是唯一支持 Spark 安全的集群管理器，使用 Yarn，Spark 可以运行于 Kerberized Hadoop 之上，在它们进程之间进行安全认证

## 7. Shell 脚本分析

### spark-submit

```
spark-class org.apache.spark.deploy.SparkSubmit "$@"
```

### spark-shell

```
spark-submit --class org.apache.spark.repl.Main --name "Spark shell"
```

实际执行：
```
/usr/local/jdk/bin/java -Xmx128m -cp /usr/hdp/current/spark-client/jars/* \
  org.apache.spark.launcher.Main org.apache.spark.deploy.SparkSubmit \
  --class org.apache.spark.repl.Main --name Spark shell --master local
```

### spark-sql

```
spark-submit --class org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver "$@"
```

### spark-daemon

```bash
mode = org.apache.spark.deploy.master.Master
case "$mode" in
  (class)
    execute_command nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class "$command" "$@"
    ;;
esac
```

### start-master

```bash
CLASS="org.apache.spark.deploy.master.Master"
"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
```

实际启动命令：
```
/usr/local/jdk/bin/java -Xmx128m -cp /usr/hdp/3.1.0.0-78/spark/jars/* \
  org.apache.spark.launcher.Main org.apache.spark.deploy.master.Master \
  --host indata-10-108-8-63.indata.com --port 7077 --webui-port 8080
```

### start-workers

```bash
CLASS="org.apache.spark.deploy.worker.Worker"
"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS $WORKER_NUM \
  --webui-port "$WEBUI_PORT" $PORT_FLAG $PORT_NUM $MASTER "$@"
```

实际启动命令：
```
/usr/local/jdk/bin/java -Xmx128m -cp /usr/hdp/3.1.0.0-78/spark/jars/* \
  org.apache.spark.launcher.Main org.apache.spark.deploy.worker.Worker \
  --webui-port 8081 spark://indata-10-108-8-63.indata.com:7077
```

## 8. Master 启动流程

1. `start-master` -> `spark-daemon.sh start org.apache.spark.deploy.master.Master`
2. `spark-class org.apache.spark.deploy.master.Master`
3. 实际启动 `Master.main`
4. `SparkConf, MasterArguments` -> `startRpcEnvAndEndpoint` -> `NettyRpcEnvFactory` 创建 rpcEnv 容器
5. 向 rpcEnv 容器注入 Master 的 rpcEndpoint
6. `Master.onStart` -> `MasterWebUI` 将每个页面的 html 以 scala.xml.Node 的形势封装放在 servlet
7. 创建 `ServletContextHandler`、创建 `createServlet`
8. 启动 `jettyServer` 监听 web 端口, jetty server 绑定 handler
9. 启动定时器，定时发送信息，检查 worker 的状态，移除超时的 worker
10. 指标系统，状态恢复等

## 9. Worker 启动流程

1. `start-worker` -> `spark-daemon.sh start org.apache.spark.deploy.worker.Worker`
2. `spark-class org.apache.spark.deploy.worker.Worker`
3. 实际启动 `Worker.main`
4. `SparkConf, WorkerArguments` 解析参数添加默认值 -> `startRpcEnvAndEndpoint` -> `NettyRpcEnvFactory` 创建 rpcEnv 容器
5. 向 rpcEnv 容器注入 Worker 的 rpcEndpoint
6. `Worker.onStart` -> 创建 Worker 文件夹 , 启动外部 Shuffle 服务 , 设置 worker 资源
7. `WorkerWebUI` 初始化将每个页面的 html 以 scala.xml.Node 的形势封装放在 servlet
8. 创建 `ServletContextHandler`、创建 `createServlet`
9. 启动 `jettyServer` 监听 web 端口, jetty server 绑定 handler
10. 将 worker 信息注册到 master
11. 指标系统，状态恢复等

## 10. Worker 与 Master 通信

1. `Worker.onStart` -> `tryRegisterAllMasters` -> `masterEndpoint.sendRegisterMessageToMaster`
2. Master 接收 -> `WorkerInfo` -> `registerWorker` -> 移除 dead worker，UNKNOWN worker -> 将 worker 放到 workers，idToWorker，addressToWorker
3. 发送给 worker `RegisteredWorker` -> 将 registered 属性设置为 true -> `changeMaster` 修改用于和 Master 通信的 masterRef、masterWebUiUrl、masterAddress，connected = true -> 取消所有未完成的重新注册尝试
4. 每15秒发一个心跳 `SendHeartbeat` -> `sendToMaster(Heartbeat)` -> `masterRef.send` -> 更新最后一次心跳时间，如果没有 worker 信息就重新注册
5. 不断尝试重新连接

## 11. 四种提交流程

### Standalone Client

1. 通过 SparkSubmit 提交程序，本地运行 Driver, 初始化 SparkContext（启动 TaskScheduler、CoraseGrainedScheduler 等）
2. Driver 向 master 注册 Application registerApplication，返回注册成功
3. Master 先分配资源，然后通知 Worker 启动 Executor，通过 ExecutorRunner.start 启动 CoarseGrainedExecutorBackend
4. Executor（CoarseGrainedExecutorBackend）向 Driver（CoraseGrainedSchedulerBackend）反注册
5. Driver 运行 Main 函数，创建 DataFrame，RDD，调用 action 算子，提交任务，划分 stage，提交 task

### Standalone Cluster

1. 通过 SparkSubmit 提交程序, 启动 ClientApp
2. ClientApp 向 master 发送启动 driver 的请求，master 通知 worker 启动 driver，driver 初始化 SparkContext（启动 TaskScheduler、CoraseGrainedScheduler 等）
3. driver 向 master 注册 Application，返回注册成功
4. Master 先分配资源，然后通知 Worker 启动 Executor，通过 ExecutorRunner.start 启动 CoarseGrainedExecutorBackend
5. Executor（CoarseGrainedExecutorBackend）向 Driver（CoraseGrainedSchedulerBackend）反注册
6. Driver 运行 Main 函数，创建 DataFrame，RDD，调用 action 算子，提交任务，划分 stage，提交 task

### Yarn Client

1. 通过 SparkSubmit 提交程序，本地运行 Driver, 初始化 SparkContext（启动 TaskScheduler、CoraseGrainedScheduler 等）
2. Driver 向 resourceManager 提交 application，申请启动 applicationMaster
3. ResourceManager 分配资源，在适当的 nodemanager 分配 container，在 container 中启动 applicationMaster
4. applicationMaster 相当于一个 executorLauncher, 向 resourceManager 发送申请启动 executor 的资源，resourceManager 返回资源列表
5. 根据返回的资源列表，在指定的 nodemanager 上启动 container 和 executor
6. executor 向 driver 反注册

### Yarn Cluster

（参考 Yarn Client，区别是 Driver 运行在集群中而非客户端）

## 12. Driver 启动

Driver 是指运行 main 函数，初始化 SparkContext 的地方。

1. `SparkSubmit` -> `runMain` -> `prepareSubmitEnvironment`
2. `childMainClass`:
   - Client : args.mainClass
   - StandaloneCluster: ClientApp
   - YarnCluster: YarnClusterApplication
3. `app.start`
4. `ClientApp.start` -> `ClientArguments` -> `ClientEndpoint.onStart` -> `masterEndpoint.ask(RequestSubmitDriver)` 请求 master 启动 driver
5. Master.receiveAndReply -> `createDriver` -> `waitingDrivers += driver` -> `drivers.add(driver)` -> `schedule()`
6. `worker.addDriver` -> master 通知 worker 启动 `worker.endpoint.send(LaunchDriver)` -> worker.receive -> LaunchDriver -> new DriverRunner -> driver.start()
7. `prepareAndRunDriver` -> 创建 worker dir -> `CommandUtils.buildProcessBuilder` .. 最终通过 `processBuilder.start()` 执行启动命令启动 DriverWrapper, 在 DriverWrapper 中启动 mainClass
8. `context.reply(SubmitDriverResponse)` -> 返回成功响应

## 13. 注册 Application

1. CoarseGrainedExecutorBackend 在 StandaloneSchedulerBackend start 方法中 command
2. SparkContext -> createTaskScheduler -> new StandaloneSchedulerBackend -> CoarseGrainedSchedulerBackend -> `rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint())` -> `DriverEndpoint.onStart` -> `_.send(ReviveOffers)`
3. `_taskScheduler.start()` -> `backend.start()` -> `StandaloneAppClient.start` -> `ClientEndpoint.onstart` -> `tryRegisterAllMasters` -> `RegisterApplication`
4. `Master.registerApplication` -> `waitingApps += app` -> 回复注册成功

## 14. Executor 启动

1. `schedule()` -> `startExecutorsOnWorkers` -> 遍历 waitingApps -> `scheduleExecutorsOnWorkers` -> `allocateWorkerResourceToExecutors` -> `app.addExecutor` -> `launchExecutor`
2. `worker.endpoint.send(LaunchExecutor)` -> worker.LaunchExecutor -> new ExecutorRunner.start -> `fetchAndRunExecutor` -> 启动 CoarseGrainedExecutorBackend
3. `CoarseGrainedExecutorBackend.main` ->
   - `onStart` -> 向 Driver 反注册 `ask RegisterExecutor` -> `executorDataMap.put(executorId, new ExecutorData)`
   - 反注册成功后 `self.send(RegisteredExecutor)` -> new Executor -> `driver.get.send(LaunchedExecutor)` -> `data.freeCores = data.totalCores` -> `makeOffers`
4. `application.driver.send(ExecutorAdded)` -> `StandaloneAppClient.ExecutorAdded` -> `listener.executorAdded` -> 打印日志
5. `app.state = ApplicationState.RUNNING`

## 15. SparkContext 与任务提交

### SparkContext 创建

`SparkSession.getOrCreate` -> SparkContext

### 任务提交 - Stage 划分

```
DataFrame.count
  -> DataSet.count
  -> plan.executeCollect()
  -> Rdd.collect()
  -> sc.runJob
  -> runJob
  -> dagScheduler.runJob
  -> submitJob
  -> eventProcessLoop.post(JobSubmitted)
  -> eventProcessLoop.start()
  -> DAGSchedulerEventProcessLoop.onReceive
  -> doOnReceive
  -> dagScheduler.handleJobSubmitted
  -> createResultStage
  -> getOrCreateParentStages
  -> getOrCreateShuffleMapStage
  -> createShuffleMapStage
  -> new ActiveJob(jobId, finalStage)
  -> submitStage
  -> submitMissingTasks
  -> taskScheduler.submitTasks
```

## 16. 调试命令

```bash
# spark-shell 远程调试
spark-shell --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

# spark-submit Yarn Client 模式远程调试
spark-submit --master yarn --deploy-mode client \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
  --queue default --class org.apache.spark.examples.SparkPi \
  /usr/hdp/3.1.0.0-78/spark/examples/jars/spark-examples_2.12-3.2.3.jar

# spark-submit Standalone 模式远程调试
spark-submit --master spark://indata-10-108-8-63.indata.com:7077 \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
  --class org.apache.spark.examples.SparkPi \
  /usr/hdp/3.1.0.0-78/spark/examples/jars/spark-examples_2.12-3.2.3.jar
```

## 17. 模块说明

| 模块 | 说明 |
|------|------|
| assembly | 依赖包 |
| benchmarks | 基准测试 |

## 18. 参考文章

- https://blog.csdn.net/weixin_45425054/article/details/114542750
- https://blog.csdn.net/luyllyl/category_7506344.html
- https://doc.hcs.huawei.com/zh-cn/productdesc/mrs/mrs_08_007101.html