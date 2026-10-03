# 🛡️ FinGuard —— 银行实时风控数据平台

> 基于 IEEE-CIS Fraud Detection 数据集构建的银行实时交易风控平台
> **LightGBM + Kafka + Flink + Doris + Streamlit + AI Agent**

FinGuard 是一个面向银行交易场景的实时风控数据平台。

项目使用 IEEE-CIS Fraud Detection 历史交易数据训练 LightGBM 欺诈预测模型，并通过 Kafka 模拟实时交易流，经 Flink 完成实时数据处理、模型推理、用户行为风险计算与风险融合，最终将风控结果写入 Doris，并通过 Streamlit Dashboard 进行实时监控，同时提供 AI Agent 对风险数据进行查询和分析。

---

## 📌 项目概览

| 项目    | 内容                       |
| ----- | ------------------------ |
| 项目名称  | FinGuard                 |
| 项目类型  | 银行实时风控数据平台               |
| 数据集   | IEEE-CIS Fraud Detection |
| 核心场景  | 银行实时交易欺诈检测               |
| 实时消息  | Kafka                    |
| 实时计算  | Flink                    |
| 数据存储  | Doris                    |
| 机器学习  | LightGBM                 |
| 数据展示  | Streamlit                |
| AI 能力 | ARK + AI Agent           |
| 部署环境  | VMware + CentOS 7        |
| 开发语言  | Python / SQL             |
| 集群规模  | 3 节点                     |

---

# 1. 🎯 项目背景

银行交易数据具有数据量大、实时性高以及风险行为复杂等特点。

传统离线分析通常只能对历史交易进行批量处理，而实时风控需要在交易产生后尽快完成：

```
交易产生
   ↓
实时数据接入
   ↓
实时计算
   ↓
风险模型预测
   ↓
用户行为分析
   ↓
风险融合
   ↓
风险结果存储
   ↓
实时监控 / AI 分析
```

FinGuard 通过构建一套完整的实时数据链路，对 IEEE-CIS 交易数据进行实时回放，模拟银行交易进入风控系统后的处理过程。

---

# 2. 🏗️ 系统整体架构

## 2.1 架构图

<img src="docs/images/architecture.png" width="1000">

整个系统主要分为 **离线模型训练、实时数据处理、风险分析、数据存储、可视化与 AI Agent** 六个部分。

---

## 2.2 数据流

```
                 IEEE-CIS Fraud Detection
                          │
             ┌────────────┴────────────┐
             │                         │
        Train Data                 Test Data
             │                         │
             ↓                         ↓
       特征工程 / 训练              实时数据回放
             │                         │
             ↓                         ↓
         LightGBM                  Kafka
             │                         │
             │                         ↓
             │                      Flink
             │                         │
             │          ┌──────────────┼──────────────┐
             │          │              │              │
             │          ↓              ↓              ↓
             │      模型推理       行为风险计算     实时指标
             │          │              │
             │          └──────┬───────┘
             │                 ↓
             │              风险融合
             │                 │
             │                 ↓
             │              Doris
             │                 │
             │        ┌────────┴────────┐
             │        ↓                 ↓
             │   Dashboard          AI Agent
             │        │                 │
             │        ↓                 ↓
             │   实时风控监控       风险查询 / 分析
```

---

# 3. 🧰 技术栈

| 层级          | 技术                 |
| ----------- | ------------------ |
| 开发语言        | Python / SQL       |
| 数据处理        | Pandas / PySpark   |
| 机器学习        | LightGBM           |
| 消息队列        | Apache Kafka       |
| 实时计算        | Apache Flink       |
| 数据仓库 / OLAP | Apache Doris       |
| 数据可视化       | Streamlit          |
| AI Agent    | Python + ARK       |
| 大模型         | DeepSeek           |
| 分布式环境       | Hadoop / YARN      |
| 协调服务        | ZooKeeper          |
| 虚拟化         | VMware Workstation |
| 操作系统        | CentOS 7.9         |

---

# 4. 📊 数据与模型

## 4.1 数据集

项目使用 Kaggle IEEE-CIS Fraud Detection 数据集。

详细数据集数据字典请跳转👉 [data_dictionary.md](data_dictionary.md)

主要数据包括：

```
train_transaction
train_identity

test_transaction
test_identity
```

训练数据约：

```
590,540 条交易记录
394 个原始字段
```

欺诈交易约：

```
20,663 条
```

欺诈比例约：

```
3.5%
```

由于原始交易数据存在明显的类别不平衡，因此项目在模型评估时同时关注 ROC-AUC、PR-AUC、Precision 和 Recall。

---

# 5. 🤖 欺诈检测模型

## 5.1 模型训练流程

```
原始交易数据
      ↓
数据分析
      ↓
缺失值 / 类型处理
      ↓
Transaction + Identity 数据关联
      ↓
特征工程
      ↓
时间顺序划分训练集 / 验证集
      ↓
LightGBM
      ↓
模型评估
      ↓
model.pkl
```

---

## 5.2 特征工程

项目对原始交易数据进行特征处理，包括：

* TransactionDT
* TransactionAmt
* ProductCD
* card1 ~ card6
* addr1 / addr2
* dist1
* P_emaildomain
* identity 特征
* DeviceType
* DeviceInfo
* transaction_hour
* transaction_day
* transaction_weekday

最终构建约 **46 个模型特征**。

其中 `id_32` 等类别字段根据实际数据类型进行处理，避免直接进行错误的数值化。

---

# 6. 📈 模型效果

项目采用时间顺序进行训练集 / 验证集划分，避免随机划分带来的时间信息泄漏。

模型主要指标：

| 指标              |       结果 |
| --------------- | -------: |
| Best Iteration  |      281 |
| ROC-AUC         | 0.828154 |
| PR-AUC          | 0.331562 |
| Precision @ 0.5 | 0.702155 |
| Recall @ 0.5    | 0.136319 |

其中 ROC-AUC 为 **0.828154**。

由于欺诈交易属于少数类，因此项目同时使用 PR-AUC、Precision 和 Recall 对模型进行补充评价。

---

# 7. ⚡ 实时风控链路

## 7.1 Kafka —— 实时交易接入

模型训练完成后，项目不会直接使用测试数据中的 `isFraud` 字段。

而是通过交易回放程序模拟银行实时交易：

```
IEEE-CIS Test Data
        ↓
Python Replay
        ↓
Kafka Producer
        ↓
transaction_topic
        ↓
Flink
```

Kafka 负责实时交易消息接入和缓冲。

<img src="docs/images/kafka.png" width="900">

---

# 8. 🔥 Flink —— 实时计算

Flink 是整个实时风控链路的核心计算引擎。

主要负责：

```
Kafka
  ↓
交易解析
  ↓
模型风险
  ↓
行为风险
  ↓
风险融合
  ↓
risk_output
  ↓
Doris
```

项目使用 Flink SQL / DataStream 完成实时数据处理。

<img src="docs/images/flink.png" width="900">

---

# 9. 🧠 实时模型推理

每笔实时交易进入 Flink 后，通过模型推理得到：

```
fraud_probability
```

该结果代表模型对当前交易属于欺诈交易的概率判断。

实时交易数据本身不包含 `isFraud` 标签，因此实时风控过程不依赖测试集真实标签。

---

# 10. 👤 用户行为风险

单笔交易模型只能判断当前交易本身的风险。

因此 FinGuard 增加了基于时间窗口的行为风险分析。

项目使用：

```
subject_key
```

作为用户行为聚合代理键。

该字段由多个交易属性组合生成：

```
card1
card2
card3
card5
card6
addr1
addr2
```

用于模拟同一用户 / 同一交易主体的行为聚合。

> `subject_key` 是项目构造的行为分析代理键，并不代表真实用户 ID。

---

## 10.1 行为指标

系统维护最近 10 分钟窗口内的行为统计：

```
transaction_count
total_amount
max_amount
avg_amount
```

例如：

* 最近 10 分钟交易次数
* 最近 10 分钟累计金额
* 最近 10 分钟最大交易金额
* 最近 10 分钟平均交易金额

---

# 11. 🚨 风险评分

行为风险根据交易频率、交易金额等指标进行评分。

主要规则：

```
交易次数：
>= 7      +40
>= 4      +25
>= 3      +10

累计交易金额：
>= 6000   +40
>= 3000   +25
>= 1500   +10

最大交易金额：
>= 3000   +20
>= 1500   +10
```

最终：

```
behavior_score <= 100
```

风险等级：

```
score >= 70  → HIGH
score >= 40  → MEDIUM
否则         → LOW
```

---

# 12. 🔀 风险融合

FinGuard 将模型风险与行为风险进行融合。

最终风险评分：

```
risk_score =
    fraud_probability × 70%
    +
    behavior_score × 30%
```

最终生成：

```
risk_score
risk_level
risk_reason
```

形成完整的交易风险结果。

---

# 13. 🗄️ Doris —— 风控结果存储

Flink 将最终风险结果写入 Doris。

数据链路：

```
Kafka
  ↓
Flink
  ↓
Risk Fusion
  ↓
Doris
```

Doris 负责保存实时风控结果，并为 Dashboard 和 AI Agent 提供查询服务。


---

# 14. 📊 实时风控 Dashboard

项目使用 Streamlit 构建实时风控监控页面，实时展示交易数据与风险分析结果。

主要展示：

* 实时交易数量
* 风险交易数量
* HIGH / MEDIUM / LOW 风险分布
* 风险交易列表
* 风险评分
* 欺诈概率
* 行为风险
* 交易金额
* 最近交易趋势

整体数据来源于 Doris。

```
Flink
  ↓
Doris
  ↓
Streamlit
  ↓
实时风控 Dashboard
```

<img src="docs/images/1.png" width="1000">

<img src="docs/images/2.png" width="1000">

<img src="docs/images/3.png" width="1000">

---

# 15. 🤖 AI Agent

FinGuard 在实时风控平台上进一步增加 AI Agent。

Agent 不负责重新计算风险，而是通过工具查询已经进入 Doris 的风控数据。

整体流程：

```
用户问题
   ↓
AI Agent
   ↓
Tool
   ↓
Doris
   ↓
查询结果
   ↓
LLM
   ↓
自然语言分析
```

---

## 15.1 Agent Tools

目前 Agent 提供的主要工具包括：

```
get_risk_summary
get_recent_high_risk
get_transaction
```

分别用于：

### 风险概览

查询当前风险交易总体情况。

### 高风险交易

查询近期高风险交易。

### 单笔交易

根据 TransactionID 查询具体交易的风控信息。

---

## 15.2 Agent 示例

例如用户可以询问：

> 最近有哪些高风险交易？

Agent 会：

```
问题
 ↓
调用 get_recent_high_risk
 ↓
查询 Doris
 ↓
获取风险交易
 ↓
交给大模型分析
 ↓
生成自然语言结果
```

---

# 16. 🖥️ 三节点集群

项目使用 VMware 搭建三节点 Linux 集群。

| 节点       | IP             | 主要服务                                                     |
| -------- | -------------- | -------------------------------------------------------- |
| master   | 192.168.56.101 | NameNode / ResourceManager / Kafka / Flink JM / Doris FE |
| worker01 | 192.168.56.102 | DataNode / NodeManager / Kafka / Flink TM / Doris BE     |
| worker02 | 192.168.56.103 | DataNode / NodeManager / Kafka / Flink TM / Doris BE     |

基础环境：

```
VMware Workstation
CentOS 7.9
JDK 17
Hadoop 3.4.0
ZooKeeper 3.7.2
Kafka 3.6.1
Flink 2.3.0
Doris 3.0.8
```

---

# 17. 📁 项目目录

```
IEEE-CIS/
│
├── agent/
│   ├── agent.py
│   ├── tools.py
│   └── __init__.py
│
├── api/
│   ├── main.py
│   └── __init__.py
│
├── dashboard/
│   └── app.py
│
├── model/
│   ├── feature_config.py
│   ├── preprocess.py
│   ├── profile_data.py
│   ├── analyze_features.py
│   ├── analyze_fraud_features.py
│   ├── analyze_identity.py
│   ├── analyze_subject_key.py
│   ├── find_behavior_cases.py
│   ├── behavior_risk.py
│   ├── prepare_realtime.py
│   ├── predict_test.py
│   ├── train.py
│   └── model.pkl
│
├── realtime/
│   ├── kafka_producer.py
│   ├── model_inference.py
│   ├── replay_transaction.py
│   ├── replay_behavior_test.py
│   └── risk_fusion.py
│
├── flink/
│   └── sql/
│       ├── realtime_risk.sql
│       ├── risk_output_source.sql
│       └── doris_sink.sql
│
├── docs/
│   └── images/
│       ├── architecture.png
│       ├── kafka.png
│       ├── flink.png
│       ├── doris.png
│       ├── 1.png
│       ├── 2.png
│       └── 3.png
│
├── PROJECT_HANDOVER.md
├── README.md
└── .gitignore
```

---

# 18. 🔧 核心代码说明

## model/

负责离线数据分析、特征工程、模型训练和行为风险逻辑。

```
preprocess.py
    ↓
数据预处理 / 特征构建

train.py
    ↓
LightGBM 模型训练

predict_test.py
    ↓
测试集预测

behavior_risk.py
    ↓
行为风险计算
```

---

## realtime/

负责实时交易数据链路。

```
replay_transaction.py
        ↓
模拟交易产生

kafka_producer.py
        ↓
发送 Kafka

model_inference.py
        ↓
实时模型推理

risk_fusion.py
        ↓
模型风险 + 行为风险
```

---

## flink/sql/

负责实时计算和 Doris 数据写入。

```
realtime_risk.sql
        ↓
实时风控处理

risk_output_source.sql
        ↓
风险结果数据源

doris_sink.sql
        ↓
写入 Doris
```

---

## dashboard/

`app.py` 负责实时风控 Dashboard。

---

## agent/

`agent.py` 负责 AI Agent。

`tools.py` 负责 Doris 查询工具。

---

# 19. 🚀 项目运行流程

完整运行链路：

```
① 启动 VMware 三节点
        ↓
② 启动 Hadoop / YARN
        ↓
③ 启动 ZooKeeper
        ↓
④ 启动 Kafka
        ↓
⑤ 启动 Flink
        ↓
⑥ 启动 Doris
        ↓
⑦ 启动 Flink SQL Job
        ↓
⑧ 启动 Kafka Producer
        ↓
⑨ 实时交易进入 Kafka
        ↓
⑩ Flink 实时计算
        ↓
⑪ 风险结果写入 Doris
        ↓
⑫ 启动 Dashboard
        ↓
⑬ 启动 AI Agent
```

---

# 20. 🔍 系统验证

可以通过以下组件分别验证系统运行状态。

### Flink

`http://192.168.56.101:8081`

查看：

* JobManager
* TaskManagers
* Running Jobs
* Slots

### Doris

`http://192.168.56.101:8050`

查看 FE 状态。

### Kafka

通过命令行检查 Topic：

```bash
kafka-topics.sh \
  --bootstrap-server master:9092 \
  --list
```

检查实时消息：

```bash
kafka-console-consumer.sh \
  --bootstrap-server master:9092 \
  --topic transaction_topic
```

---

# 21. 📌 项目核心特点

### ① 完整实时数据链路

```
数据
 ↓
Kafka
 ↓
Flink
 ↓
风险计算
 ↓
Doris
 ↓
Dashboard
```

实现从实时数据接入到最终展示的完整链路。

### ② 机器学习 + 实时计算结合

LightGBM 负责单笔交易风险预测，Flink 负责实时计算和数据流转。

### ③ 增加行为风险

不仅考虑单笔交易的模型概率，同时结合时间窗口内的交易行为进行风险判断。

### ④ AI Agent

通过 Agent 查询 Doris 中的风险数据，让用户可以使用自然语言进行风险分析。

---

# 22. 📈 项目核心指标

| 模块                      | 指标 / 结果   |
| ----------------------- | --------- |
| 训练数据                    | 590,540 条 |
| 原始字段                    | 394       |
| 模型特征                    | 46        |
| LightGBM Best Iteration | 281       |
| ROC-AUC                 | 0.828154  |
| PR-AUC                  | 0.331562  |
| Precision @ 0.5         | 0.702155  |
| Recall @ 0.5            | 0.136319  |
| 实时消息系统                  | Kafka     |
| 实时计算                    | Flink     |
| OLAP                    | Doris     |
| 可视化                     | Streamlit |
| AI Agent                | ARK       |


---

# 23. 🎯 项目总结

FinGuard 从 IEEE-CIS 历史交易数据出发，构建了一个完整的银行实时风控数据处理流程。

项目将：

```
机器学习
+
消息队列
+
实时计算
+
OLAP 数据库
+
数据可视化
+
AI Agent
```

进行结合，实现了从交易数据接入、实时风险计算、风险结果存储，到可视化监控和自然语言分析的完整数据链路。

项目重点实践了：

* Kafka 实时数据接入
* Flink 实时计算
* 实时特征与风险计算
* LightGBM 模型推理
* Doris OLAP 存储
* Streamlit 数据可视化
* AI Agent 数据查询

