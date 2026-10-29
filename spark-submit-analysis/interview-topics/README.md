# 面试考点 · 分篇详细总结目录

> 对应 `../interview-master-list.md` 中的各考点，按编号分篇整理。
> 有的需要深入（含源码分析），有的只需简要总结（概念清晰即可）。

---

## 进度

| # | 考点 | 文件 | 深度 | 状态 |
|---|------|------|------|------|
| 15 | Cache/Persist vs Checkpoint | 已有文章 `_posts/spark/sparkSourceCode-sparkContext.md` | 详 | ✅ 已完成 |
| 17 | Flink Checkpoint 原理 | 已有系列 `_posts/flink/flink-checkpoint-concept.md` | **详（源码）** | 🔄 进行中 |
| 27 | Kerberos / Ranger / Ranger-KMS / KDC / LDAP 安全体系 | `_posts/hadoop/hadoop-security-kerberos-ranger-kms.md`（公众号文章） | 详 | ✅ 已完成 |
| — | 更多待整理 | — | — | ⏳ |

---

## 深度分类说明

| 深度标识 | 说明 | 适用考点 |
|----------|------|----------|
| **详（源码）** | 源码级别的深入分析，配套文章 | Checkpoint、Shuffle、AQE、DAGScheduler 等 |
| 详 | 概念 + 流程 + 调优，有配套文章 | 数据倾斜、YARN 调度、Ranger 等 |
| 简 | 口述模板足够，不单独展开 | HDFS 读写、Kafka 分区副本等 |

---

## 如何添加新总结

1. 在 `interview-topics/` 下创建 `topic-NN-name.md` 文件
2. 在 `../interview-master-list.md` 进度表中更新状态
3. 在本 README 进度表中添加一行
4. 在 `.session-context.md` 中记录新增
