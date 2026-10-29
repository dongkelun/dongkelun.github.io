# 大数据安全体系：Kerberos / Ranger / Ranger-KMS / KDC / LDAP

> 核心链路：**Kerberos（身份认证）→ Ranger（权限授权）→ Ranger-KMS（数据加密）**

---

## 一、整体链路速览

```
  LDAP（账号通讯录，可选后端）
    ↓                            
  KDC（Kerberos发证中心）         ← OpenLDAP 可选做账号后端
    ↓ 发放加密票据
  Kerberos 身份认证（证明你是谁）    ← 门禁
    ↓ 可信身份
  Ranger 权限授权（你能干什么）     ← 各办公室门锁
    ↓
  Ranger-KMS 数据加密（数据安全）   ← 保险柜
```

**一句话：Kerberos 看门确认身份，Ranger 管权限，Ranger-KMS 管数据加密，各司其职、配套使用。**

---

## 二、Kerberos — 身份认证

### 2.1 用来干什么

Kerberos 是大数据集群的**身份认证协议**，解决 **"你是谁"** 的问题。

- 集群所有用户、服务都要有 Kerberos 票据
- 没有合法票据，不能访问 HDFS、Hive、Kafka 等组件
- **只负责身份认证，不负责权限控制**

### 2.2 怎么控制没有认证不能访问

关键原理：**每个 Hadoop 组件服务自身开启 Kerberos 安全模式，各自校验票据。**

```
① 用户 kinit → 向 KDC 申请 TGT（票据授予票据）
② 访问 HDFS/Hive → 客户端带 Service-Ticket
③ 服务端（NameNode/HiveServer2）校验票据：
   - ✅ 合法：解析出 principal 身份，放行 → 交给 Ranger 鉴权
   - ❌ 非法/过期：直接断开连接，抛出 GSSException
```

**核心要点：**
- 不是统一网关拦截，**是每个服务自己校验票据**
- 建立连接阶段就强制校验，不是先连上再验证
- 可信身份来自**服务端解密票据**，不是客户端传的字符串（防伪造）

### 2.3 生产常见报错

```
GSSException: No valid credentials provided
```
→ 客户端没有有效 Kerberos 票据，服务端直接拒绝。

### 2.4 对比：有 Kerberos vs 无 Kerberos

| 场景 | 身份来源 | 能否伪造 |
|------|----------|----------|
| 未开启 Kerberos | 客户端传 user 字符串 | 随便填，直接信任 |
| 开启 Kerberos | 加密票据解密结果 | 不能，票据加密不可伪造 |

---

## 三、Ranger — 权限授权

### 3.1 用来干什么

Ranger 是**细粒度权限管控**，解决 **"你能干什么"** 的问题。

- 用户身份已被 Kerberos 确认后，Ranger 做授权
- 控制：哪个用户能读/写哪张 Hive 表、HDFS 目录、Kafka 主题
- 支持行级、列级权限

### 3.2 关键机制：Ranger 插件

- Ranger 插件（Hive-plugin、HDFS-plugin）部署到各组件
- 插件拦截组件访问请求，拿 Kerberos 解析出的真实用户去查 Ranger 策略
- **不装插件，Ranger 权限不生效**

---

## 四、Ranger 和 Kerberos 的配合

### 4.1 关系图

```
用户访问 HDFS/Hive
        ↓
① Kerberos 校验票据 ← 没有票据，直接拒绝
        ↓ 通过
② 拿到可信 principal 身份
        ↓
③ Ranger 插件拦截请求，查权限策略
        ↓
④ 有权限 → 放行；无权限 → 拒绝
```

### 4.2 常见误区

**Q：开启 Ranger 是不是就不需要 Kerberos？**
❌ 错误。Ranger 不能替代 Kerberos。没有 Kerberos 身份认证，攻击者可伪造用户名绕过 Ranger。

**Q：Ranger 能不能脱离 Kerberos 运行？**
✅ 技术上可以，Ranger 组件能独立启动配置策略。但生产绝对不行——**Ranger 信任上层传过来的用户名，没有 Kerberos 强认证，用户名可被伪造**。

### 4.3 配套配置三件事

1. 集群各服务启用 Kerberos（HDFS/Hive/Kafka 配置 principal）
2. 各组件部署 Ranger 插件
3. Ranger 中导入 Kerberos principal 并配置权限策略

### 4.4 通俗类比

- **Kerberos = 大楼门禁卡**：没卡连大门都进不去
- **Ranger = 各办公室门锁**：刷卡进楼后，决定你能进哪个房间
- **只开 Kerberos 不开 Ranger**：只能控制"能不能进集群"，做不了表级/列级细粒度权限

---

## 五、KDC — 密钥分发中心

### 5.1 KDC 是什么

KDC 是 Kerberos 的**核心服务**，全称"密钥分发中心"，**整个 Kerberos 体系的发证机构**。

内部包含两部分：
| 组件 | 功能 | 类比 |
|------|------|------|
| AS（认证服务） | 客户端拿 keytab 申请 TGT | 办身份证 |
| TGS（票据授予服务） | 拿 TGT 申请具体 Service-Ticket | 凭身份证办具体通行证 |

**一句话流程：**
```
kinit → KDC 发 TGT → 访问 HDFS → 拿 TGT 找 KDC 换 Service-Ticket → 访问服务
```

**没有 KDC，Kerberos 就没法生成、发放票据。**

---

## 六、LDAP — 账号存储

### 6.1 LDAP 是什么

LDAP 是**轻量目录访问协议**，本质是**账号身份数据库**，存用户名、用户组。

它不是某个组件独有的，而是**集群或企业统一的账号中心**。多个地方可以用它：

- **KDC 的后端账号源**：KDC 可以把 principal 信息存在 LDAP 里，新增用户只需在 LDAP 创建，KDC 自动读取
- **Ranger 的管理员登录**：Ranger 网页后台支持用 LDAP 做管理员账号登录
- **Hadoop 服务的系统账号**：HDFS、Hive 等服务的操作系统用户也可以对接 LDAP 统一管理

但有一个关键点：**LDAP 只存账号，不做身份认证**。谁来查它都给，不校验请求方是不是合法。校验身份是 Kerberos/KDC 的事。

- 不是系统自带服务，需要手动安装（OpenLDAP）
- 安装命令：`yum install openldap-servers openldap-clients`

### 6.2 KDC 与 LDAP 的两种部署模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| A：KDC 本地存账号 | MIT-KDC 默认本地数据库，不用 LDAP | 简单常用 |
| B：KDC 对接 OpenLDAP | 账号存 LDAP，KDC 读 LDAP 生成票据 | 企业统一身份 |

### 6.3 LDAP 和 Kerberos 各自管什么

| | LDAP | Kerberos(KDC) |
|--|------|---------------|
| 职责 | 存账号、用户组 | 发票据、做身份认证 |
| 类比 | 户籍系统 | 办证大厅 |

### 6.4 怎么确认你们平台 LDAP 用在了哪里

如果你不确定平台的 LDAP 是只给 KDC 用的，还是 Ranger 和组件 Web 也在用，几个命令就能查：

**查 KDC 是否对接 LDAP：**

KDC 有两个配置文件，查的东西不一样：

- `/etc/krb5.conf` — Kerberos 通用配置（ realms、kdc 地址、domain_realm 等），**不包含 LDAP 配置**
- `/var/kerberos/krb5kdc/kdc.conf` — KDC 服务专有配置（数据库模块、加密类型等），**LDAP 对接信息在这里**

```bash
# 查 KDC 用的什么数据库后端
grep -A 10 'db_library' /var/kerberos/krb5kdc/kdc.conf
```

如果输出中有 `db_library = kldap` 和 `ldap_servers = ldap://...`，说明 KDC 对接了 LDAP。如果看不到这个文件或没有 `db_library` 项，说明 KDC 用的本地文件存储（不依赖 LDAP）。

**查 Ranger 是否用 LDAP：**
登录 Ranger 管理页面 → Audit → User Sync → Sync Source。
- 显示 **Unix**：Ranger 读系统账号，没有对接 LDAP
- 显示 **LDAP**/AD：Ranger 对接了 LDAP

**查 Hadoop 系统账号是否来自 LDAP：**

看各组件进程的运行用户：
```bash
# 各组件进程的运行用户（第一列）
ps -ef | grep -E 'NameNode|DataNode|ResourceManager|NodeManager|HiveServer2|Kafka' | grep -v grep | tr -s ' ' | cut -d ' ' -f 1 | sort -u
```

然后查这些账号的来源：
```bash
# 看 /etc/passwd 里有没有
grep -E '^hdfs:|^yarn:|^hive:|^kafka:' /etc/passwd

# 看系统是否配置了 LDAP 用户解析
grep 'files\|ldap' /etc/nsswitch.conf | grep -E 'passwd|shadow|group'
```
- `/etc/passwd` 能查到 → 系统账号在本地管理，不依赖 LDAP
- `/etc/passwd` 查不到，且 `nsswitch.conf` 中 `passwd: files ldap` → 系统账号来自 LDAP

**直接查 LDAP 里存了什么：**
```bash
ldapsearch -x -H ldap://<ldap_host> -b "cn=kerberos,dc=indata,dc=com"
```
- 只有 `krbPrincipalName` 类条目（ambari-qa、hdfs、hive 等组件 principal）→ **LDAP 只给 KDC 用**
- 同时有 `ranger_users` 等分组 → **多组件共用**

以实际环境为例：你的 KDC 对接了 LDAP（`database_module = LDAP`），但 Ranger User Sync Source 显示 **Unix**，且 LDAP 下只有 kerberos principal。结论：**LDAP 只给 KDC 做账号后端，Ranger 和组件 Web 没有对接 LDAP**。

### 6.5 OpenLDAP 常见误区

**Q：OpenLDAP 是 Linux 自带的吗？**
❌ 不是。CentOS yum 源收录了它的 rpm 包，所以可以 `yum install` 安装，但**系统装好默认不会部署 slapd 服务**。
- 客户端工具（ldapsearch 等）很多系统预装
- 服务端（slapd）全部需要手动安装
- Ubuntu 包名叫 `slapd`，不同发行版包名不同

---

## 七、Ranger-KMS — 数据加密

### 7.1 用来干什么

Ranger-KMS 是**密钥管理服务**，做 HDFS **透明静态加密（存储加密）**。

**不是传输加密（传输靠 SSL），是磁盘上文件本身加密。**

### 7.2 使用流程（4 步）

```
① 在 Ranger-KMS 页面创建 AES 加密密钥（配访问权限）
② HDFS 上创建空目录，设为加密 Zone：
   hdfs dfsadmin -createZone -keyName 密钥名 -path /xxx/sensitive_data
③ 业务往该目录写数据 → HDFS 底层自动加密（业务无感知）
④ 业务读取该目录 → HDFS 自动解密（有权限即可）
```

**注意：加密 Zone 必须是空目录，已有数据的目录不能直接改。**

### 7.3 加密了什么（容易混淆）

**关键细节：加密 Zone 加密的是 DataNode 磁盘上的底层块文件，不是 get 下载的文件。**

```
写入加密 Zone 目录：
         ↓
DataNode 向 KMS 获取密钥 → 磁盘上存的是密文块
         ↓
hdfs dfs -get 下载：
         ↓
HDFS 自动解密 → 本地拿到的是明文文件（有权限的用户）
         ↓
直接拷贝 DataNode 磁盘原始 block 文件：
         ↓
没有 KMS 密钥 → 乱码，无法解析
```

**Q：设置加密 Zone 后，hdfs get 下来的文件是乱码？**
❌ 错误。get 下载会自动解密，拿到的是明文。
✅ 正确：真正密文保存在 DataNode 磁盘底层。

### 7.4 三个组件区分

| 组件 | 职责 | 类比 |
|------|------|------|
| Kerberos | 传输访问身份认证 | 门禁 |
| Ranger | 访问授权（读/写/建表） | 办公室门锁 |
| **Ranger-KMS** | **存储静态加密（数据落盘加密）** | **保险柜** |

### 7.5 为什么平台装了但平时用不到（你的真实场景）

- 只有敏感数据（身份证、手机号）才会开加密 Zone
- 密钥创建、加密 Zone 设置是安全管理员操作
- 普通业务目录不启用加密，完全不走 KMS
- **不是整个 HDFS 都加密，必须手动指定目录**

---

## 八、完整链路复盘图

```
            OpenLDAP（可选后端，存账号）
                ↓
   KDC / Kerberos（发证中心）
   ① kinit → 拿 TGT
   ② 访问服务 → 拿 Service-Ticket
   ③ 服务端校验票据 → 解密出 principal
                ↓
   Ranger 插件拦截请求
   ④ 查 Ranger 策略 → 判断是否有权限
                ↓
   （可选）Ranger-KMS
   ⑤ 加密 Zone 目录 → 自动加密/解密
```

---

## 九、速记卡

```
┌─────────────────────────────────────────────────────┐
│  1. Kerberos：认证，证明你是谁（登录看门）           │
│  2. Ranger：授权，允许你干什么（权限管控）           │
│  3. 依赖：Kerberos 识别身份 → Ranger 基于身份做权限  │
│  4. Ranger-KMS：密钥管理，HDFS 透明数据加密          │
│  5. KDC：Kerberos 发证中心                          │
│  6. LDAP：统一账号存储，可被 KDC/Ranger/组件共用     │
└─────────────────────────────────────────────────────┘
```

---

## 十、补充说明

---



## 十一、常见误区总结

| 错误理解 | 正确理解 |
|----------|----------|
| Ranger 必须依赖 Kerberos 才能装 | 可以独立运行，但不安全 |
| 开启 Ranger 就不用 Kerberos | 两者配套，Kerberos 防身份伪造 |
| LDAP 可以替代 Kerberos | LDAP 只存账号，不会发加密票据 |
| Linux 自带 OpenLDAP 服务 | yum 源收录，默认不安装 |
| 加密 Zone 后 get 文件是乱码 | get 会自动解密，密文在 DataNode 磁盘 |
| 开启 Ranger-KMS = 整个 HDFS 加密 | 必须手动指定加密 Zone 目录才生效 |
| Kerberos 是一个统一网关拦截 | 每个 Hadoop 服务自己校验票据 |

---

> **一句话总结：Kerberos 锁身份，Ranger 锁权限，Ranger-KMS 锁数据，三层递进，缺一不可。**
