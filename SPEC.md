# SimpleMessageBroker 技术规格文档

| 字段   | 值                              |
| ------ | ------------------------------- |
| 版本   | v1.0.0                          |
| 日期   | 2026-09-05                      |
| 状态   | Draft                           |
| 作者   | Architecture Team               |
| 项目   | MinGo.SimpleMessageBroker       |

---

## 1. 概述与目标

### 1.1 项目定位

SimpleMessageBroker 是一个轻量级单节点消息队列中间件，为分布式系统提供异步解耦能力。适用于不需要 Kafka/RabbitMQ 级别复杂度、但需要消息持久化和分区路由的业务场景。

### 1.2 设计目标（In-Goals）

| 目标           | 说明                                                     |
| -------------- | -------------------------------------------------------- |
| 消息持久化     | 关系型数据库存储（SQLite / PostgreSQL），消息不丢失       |
| 分区路由       | 基于 Key 的哈希分区，保证同 Key 消息分区内有序           |
| 多消费模式     | 队列模式（竞争消费）+ 广播模式（独立消费全量）           |
| 过期清理       | 基于 TTL 的自动过期删除，后台定时清理                    |
| byte[] Payload | 服务端仅存储原始字节，序列化/反序列化由客户端自行约定    |
| 可插拔序列化   | SDK 提供 `IPayloadSerializer` 切面接口，用户自定义实现   |
| JSON over HTTP | RESTful API，标准 HTTP 协议通信，易于集成和调试          |
| Client SDK     | C# 客户端 SDK，支持 DI 集成、连接池、自动重试            |

### 1.3 明确排除（Non-Goals）

| 排除项                 | 说明                                           |
| ---------------------- | ---------------------------------------------- |
| 多节点集群部署         | 不支持 Broker 集群，仅单节点运行               |
| 节点协调               | 无 Gossip、节点发现、一致性哈希环              |
| 副本同步               | 无 ISR、主从复制、跨节点数据冗余               |
| 元数据协调             | 不依赖 ZooKeeper / etcd / Raft                 |
| 跨节点路由             | 无消息转发、无跨节点消费                       |
| 分区 Rebalance         | 分区数量在 Topic 创建时固定，不动态调整        |
| 事务消息               | 不支持跨 Topic 事务                            |
| 死信队列               | 不支持自动转入死信队列（可手动扩展）           |

### 1.4 核心特性一览

| 特性         | 实现方式                                        |
| ------------ | ----------------------------------------------- |
| 存储引擎     | EF Core + SQLite / PostgreSQL                   |
| 通信协议     | JSON over HTTP/REST                             |
| 分区策略     | SHA256 哈希取模 / 自定义 `IPartitionStrategy`   |
| 消费模式     | ConsumerGroup 竞争 + 跨 Group 广播             |
| 过期机制     | 写入时计算 ExpiresAt，后台定时清理               |
| Payload      | `byte[]` 透传，ContentType 标记类型             |
| 序列化       | SDK 不内置，用户通过 `IPayloadSerializer` 注入  |
| 认证         | API Key + 可选 HMAC 签名                       |

### 1.5 技术栈

| 组件           | 技术选型                           |
| -------------- | ---------------------------------- |
| 运行时         | .NET 9+                            |
| Web 框架       | ASP.NET Core Web API               |
| ORM            | Entity Framework Core              |
| 数据库（开发） | SQLite                             |
| 数据库（生产） | PostgreSQL 14+                     |
| 序列化         | System.Text.Json（协议层）         |
| 客户端         | HttpClient + 连接池                |

---

## 2. 整体架构

### 2.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SimpleMessageBroker.Server                     │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                  ASP.NET Core Web API 层                      │  │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌────────────────┐  │  │
│  │  │ ProducerEndpoint│ │ ConsumerEndpoint│ │ AdminEndpoint  │  │  │
│  │  └────────┬────────┘ └────────┬────────┘ └───────┬────────┘  │  │
│  └───────────┼──────────────────┼──────────────────┼────────────┘  │
│              │                  │                  │                │
│  ┌───────────▼──────────────────▼──────────────────▼────────────┐  │
│  │                    Service 层                                 │  │
│  │  ┌──────────────────┐  ┌──────────────────────────────────┐  │  │
│  │  │ MessageService   │  │ PartitionRouter                  │  │  │
│  │  │ (核心业务逻辑)    │  │ (SHA256 哈希 / 自定义策略)       │  │  │
│  │  └────────┬─────────┘  └──────────────────────────────────┘  │  │
│  │           │                                                   │  │
│  │  ┌────────▼─────────┐  ┌──────────────────────────────────┐  │  │
│  │  │ ConsumerGroup    │  │ CleanupService (BackgroundService)│  │  │
│  │  │ Manager          │  │ 定时清理过期/已消费消息           │  │  │
│  │  └──────────────────┘  └──────────────────────────────────┘  │  │
│  └───────────────────────────┬────────────────────────────────────┘  │
│                              │                                       │
│  ┌───────────────────────────▼────────────────────────────────────┐  │
│  │               Database Layer (EF Core)                         │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │  │
│  │  │ Messages     │  │ Topics       │  │ ConsumerOffsets    │   │  │
│  │  └──────────────┘  └──────────────┘  └────────────────────┘   │  │
│  └────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                    SimpleMessageBroker.Client (SDK)                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ IMessageQueue    │  │ IPayloadSerializer│  │ IMessageHandler  │  │
│  │ Client           │  │ (用户自定义实现)  │  │ <T> (泛型消费者) │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘  │
│  ┌──────────────────┐  ┌──────────────────────────────────────────┐ │
│  │ HttpClient Pool  │  │ Retry Policy (指数退避)                  │ │
│  └──────────────────┘  └──────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 项目结构

```
SimpleMessageBroker/
├── SimpleMessageBroker.Server/           # 服务端
│   ├── Controllers/
│   │   ├── ProducerController.cs         # 生产消息 API
│   │   ├── ConsumerController.cs         # 消费消息 API
│   │   └── AdminController.cs            # 管理 API
│   ├── Models/
│   │   ├── Message.cs                    # 消息实体
│   │   ├── Topic.cs                      # 主题实体
│   │   └── ConsumerOffset.cs             # 消费偏移实体
│   ├── Data/
│   │   └── MessageQueueContext.cs        # EF Core DbContext
│   ├── Services/
│   │   ├── IMessageService.cs            # 核心业务接口
│   │   ├── MessageService.cs             # 核心业务实现
│   │   ├── IPartitionRouter.cs           # 分区路由接口
│   │   ├── PartitionRouter.cs            # 分区路由实现
│   │   ├── IConsumerGroupManager.cs      # 消费组管理接口
│   │   ├── ConsumerGroupManager.cs       # 消费组管理实现
│   │   └── CleanupService.cs             # 后台清理服务
│   ├── DTOs/
│   │   ├── ProduceRequest.cs             # 生产请求
│   │   ├── ConsumeRequest.cs             # 消费请求
│   │   ├── ApiResponse.cs                # 统一响应
│   │   └── ErrorCodes.cs                 # 错误码定义
│   ├── Middleware/
│   │   └── GlobalExceptionMiddleware.cs  # 全局异常处理
│   ├── Program.cs                        # 应用入口
│   └── appsettings.json                  # 配置文件
│
├── SimpleMessageBroker.Client/           # Client SDK
│   ├── IMessageQueueClient.cs            # 客户端核心接口
│   ├── MessageQueueClient.cs             # 客户端实现
│   ├── IPayloadSerializer.cs             # 序列化切面接口
│   ├── IMessageHandler.cs                # 泛型消息处理器
│   ├── Models/
│   │   ├── MqMessage.cs                  # SDK 消息模型
│   │   └── ClientConfig.cs               # 客户端配置
│   ├── Retry/
│   │   └── ExponentialBackoffPolicy.cs   # 指数退避重试
│   └── Extensions/
│       └── ServiceCollectionExtensions.cs # DI 注册扩展
│
├── SPEC.md                               # 本文档
└── SimpleMessageBroker.sln               # 解决方案文件
```

### 2.3 请求处理流程

**生产流程：**

```
Producer → POST /api/v1/producer/messages
         → ProducerController 验证请求
         → MessageService.ProduceAsync()
           → 获取/自动创建 Topic
           → PartitionRouter 计算分区
           → 写入 Messages 表（同步）
         → 返回 { messageId, partition, createdAt }
```

**消费流程：**

```
Consumer → POST /api/v1/consumer/pull
         → ConsumerController 验证请求
         → MessageService.ConsumeAsync()
           → 获取/初始化 ConsumerOffset
           → 查询未消费且未过期消息
           → 标记消息为已消费，更新 Offset
         → 返回消息列表（byte[] Payload）
```

**确认流程：**

```
Consumer → POST /api/v1/consumer/ack/{messageId}
         → ConsumerController 验证请求
         → MessageService.AckMessageAsync()
           → 验证消息归属（ConsumerGroup 匹配）
         → 返回确认结果
```

---

## 3. 数据模型

### 3.1 Message 表

| 字段          | 类型             | 约束          | 说明                         |
| ------------- | ---------------- | ------------- | ---------------------------- |
| Id            | VARCHAR(36)      | PRIMARY KEY   | GUID，非自增                 |
| Topic         | VARCHAR(100)     | NOT NULL      | 所属主题                     |
| Key           | VARCHAR(200)     | NULLABLE      | 路由键，用于分区计算         |
| Partition     | INT              | NOT NULL      | 分区编号（0-based）          |
| Payload       | BLOB / BYTEA     | NOT NULL      | 原始字节载荷                 |
| ContentType   | VARCHAR(100)     | DEFAULT ''    | MIME 类型标记                |
| Headers       | TEXT / JSONB     | NULLABLE      | 消息头，JSON 字典序列化存储  |
| CreatedAt     | DATETIME/TIMESTAMP | NOT NULL    | 创建时间（UTC）              |
| ExpiresAt     | DATETIME/TIMESTAMP | NULLABLE    | 过期时间（UTC），NULL 表示不过期 |
| RetryCount    | INT              | DEFAULT 0     | 重试次数                     |
| IsConsumed    | BOOLEAN          | DEFAULT FALSE | 是否已被消费                 |
| ConsumerGroup | VARCHAR(100)     | NULLABLE      | 消费该消息的消费者组         |
| ConsumerId    | VARCHAR(200)     | NULLABLE      | 消费该消息的消费者实例       |
| ConsumedAt    | DATETIME/TIMESTAMP | NULLABLE    | 消费时间（UTC）              |

**索引：**

| 索引名                            | 字段                              | 类型   |
| --------------------------------- | --------------------------------- | ------ |
| IX_Messages_Topic_Partition       | (Topic, Partition, CreatedAt)     | 复合   |
| IX_Messages_ExpiresAt             | (ExpiresAt)                       | 单列   |
| IX_Messages_Consumer              | (Topic, ConsumerGroup, IsConsumed)| 复合   |

### 3.2 Topic 表

| 字段              | 类型         | 约束          | 说明                        |
| ----------------- | ------------ | ------------- | --------------------------- |
| Name              | VARCHAR(100) | PRIMARY KEY   | 主题名称，唯一标识          |
| PartitionCount    | INT          | DEFAULT 10    | 分区数量，创建后不可变      |
| DefaultTtlSeconds | INT          | DEFAULT 86400 | 默认 TTL（秒），24 小时     |
| CreatedAt         | DATETIME     | NOT NULL      | 创建时间（UTC）             |
| IsActive          | BOOLEAN      | DEFAULT TRUE  | 是否活跃                    |

### 3.3 ConsumerOffset 表

| 字段          | 类型         | 约束          | 说明                        |
| ------------- | ------------ | ------------- | --------------------------- |
| Id            | INT/BIGSERIAL| PRIMARY KEY   | 自增主键                    |
| Topic         | VARCHAR(100) | NOT NULL      | 所属主题                    |
| ConsumerGroup | VARCHAR(100) | NOT NULL      | 消费者组                    |
| Partition     | INT          | NOT NULL      | 分区编号                    |
| LastOffset    | BIGINT       | DEFAULT 0     | 最后消费偏移（消息 Id 序号）|
| UpdatedAt     | DATETIME     | NOT NULL      | 最后更新时间（UTC）         |

**唯一约束：** `(Topic, ConsumerGroup, Partition)`

### 3.4 SQL DDL — SQLite

```sql
CREATE TABLE IF NOT EXISTS Topics (
    Name              TEXT PRIMARY KEY,
    PartitionCount    INTEGER NOT NULL DEFAULT 10,
    DefaultTtlSeconds INTEGER NOT NULL DEFAULT 86400,
    CreatedAt         TEXT NOT NULL,
    IsActive          INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS Messages (
    Id            TEXT PRIMARY KEY,
    Topic         TEXT NOT NULL,
    Key           TEXT,
    Partition     INTEGER NOT NULL,
    Payload       BLOB NOT NULL,
    ContentType   TEXT DEFAULT '',
    Headers       TEXT,
    CreatedAt     TEXT NOT NULL,
    ExpiresAt     TEXT,
    RetryCount    INTEGER NOT NULL DEFAULT 0,
    IsConsumed    INTEGER NOT NULL DEFAULT 0,
    ConsumerGroup TEXT,
    ConsumerId    TEXT,
    ConsumedAt    TEXT,
    FOREIGN KEY (Topic) REFERENCES Topics(Name)
);

CREATE INDEX IX_Messages_Topic_Partition ON Messages (Topic, Partition, CreatedAt);
CREATE INDEX IX_Messages_ExpiresAt ON Messages (ExpiresAt);
CREATE INDEX IX_Messages_Consumer ON Messages (Topic, ConsumerGroup, IsConsumed);

CREATE TABLE IF NOT EXISTS ConsumerOffsets (
    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
    Topic         TEXT NOT NULL,
    ConsumerGroup TEXT NOT NULL,
    Partition     INTEGER NOT NULL,
    LastOffset    INTEGER NOT NULL DEFAULT 0,
    UpdatedAt     TEXT NOT NULL,
    FOREIGN KEY (Topic) REFERENCES Topics(Name)
);

CREATE UNIQUE INDEX IX_ConsumerOffsets_Unique
    ON ConsumerOffsets (Topic, ConsumerGroup, Partition);
```

### 3.5 SQL DDL — PostgreSQL

```sql
CREATE TABLE IF NOT EXISTS Topics (
    Name              VARCHAR(100) PRIMARY KEY,
    PartitionCount    INTEGER NOT NULL DEFAULT 10,
    DefaultTtlSeconds INTEGER NOT NULL DEFAULT 86400,
    CreatedAt         TIMESTAMP WITH TIME ZONE NOT NULL,
    IsActive          BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS Messages (
    Id            VARCHAR(36) PRIMARY KEY,
    Topic         VARCHAR(100) NOT NULL REFERENCES Topics(Name),
    Key           VARCHAR(200),
    Partition     INTEGER NOT NULL,
    Payload       BYTEA NOT NULL,
    ContentType   VARCHAR(100) DEFAULT '',
    Headers       JSONB,
    CreatedAt     TIMESTAMP WITH TIME ZONE NOT NULL,
    ExpiresAt     TIMESTAMP WITH TIME ZONE,
    RetryCount    INTEGER NOT NULL DEFAULT 0,
    IsConsumed    BOOLEAN NOT NULL DEFAULT FALSE,
    ConsumerGroup VARCHAR(100),
    ConsumerId    VARCHAR(200),
    ConsumedAt    TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IX_Messages_Topic_Partition ON Messages (Topic, Partition, CreatedAt);
CREATE INDEX IX_Messages_ExpiresAt ON Messages (ExpiresAt);
CREATE INDEX IX_Messages_Consumer ON Messages (Topic, ConsumerGroup, IsConsumed);

CREATE TABLE IF NOT EXISTS ConsumerOffsets (
    Id            BIGSERIAL PRIMARY KEY,
    Topic         VARCHAR(100) NOT NULL REFERENCES Topics(Name),
    ConsumerGroup VARCHAR(100) NOT NULL,
    Partition     INTEGER NOT NULL,
    LastOffset    BIGINT NOT NULL DEFAULT 0,
    UpdatedAt     TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE UNIQUE INDEX IX_ConsumerOffsets_Unique
    ON ConsumerOffsets (Topic, ConsumerGroup, Partition);
```

---

## 4. API 协议定义

### 4.1 通用规范

- **基础路径：** `/api/v1/`
- **Content-Type：** `application/json`
- **请求头（可选）：**

| Header       | 说明                          |
| ------------ | ----------------------------- |
| X-Client-Id  | 客户端标识，用于追踪          |
| X-Request-Id | 请求唯一标识，用于链路追踪    |
| X-Api-Key    | API Key 认证（见第 10 章）    |

- **统一响应格式：**

```json
{
  "success": true,
  "message": "Success",
  "data": { },
  "errorCode": null
}
```

错误响应：

```json
{
  "success": false,
  "message": "Topic not found",
  "data": null,
  "errorCode": "TOPIC_NOT_FOUND"
}
```

### 4.2 错误码

| 错误码              | HTTP 状态码 | 说明               |
| ------------------- | ----------- | ------------------ |
| SUCCESS             | 200         | 成功               |
| VALIDATION_ERROR    | 400         | 参数验证失败       |
| TOPIC_NOT_FOUND     | 404         | 主题不存在         |
| MESSAGE_NOT_FOUND   | 404         | 消息不存在         |
| MESSAGE_EXPIRED     | 410         | 消息已过期         |
| MESSAGE_ALREADY_CONSUMED | 409    | 消息已被消费       |
| CONSUMER_GROUP_NOT_FOUND | 404    | 消费者组不存在     |
| INTERNAL_ERROR      | 500         | 内部服务器错误     |
| DATABASE_ERROR      | 500         | 数据库操作错误     |
| RATE_LIMITED        | 429         | 请求频率超限       |

### 4.3 生产消息

#### `POST /api/v1/producer/messages`

生产单条消息。

**请求体：**

```json
{
  "topic": "order.created",
  "key": "user-123",
  "payload": "base64EncodedBytes...",
  "contentType": "application/json",
  "headers": {
    "source": "order-service",
    "trace-id": "abc-123"
  },
  "ttlSeconds": 3600
}
```

| 字段         | 类型     | 必填 | 说明                                  |
| ------------ | -------- | ---- | ------------------------------------- |
| topic        | string   | 是   | 主题名称，最长 100 字符               |
| key          | string   | 否   | 路由键，用于分区计算                  |
| payload      | string   | 是   | Base64 编码的 byte[] 载荷             |
| contentType  | string   | 否   | MIME 类型，默认 `application/octet-stream` |
| headers      | object   | 否   | 消息头键值对                          |
| ttlSeconds   | int      | 否   | 消息存活时间（秒），不传则使用 Topic 默认值 |

**响应（200）：**

```json
{
  "success": true,
  "message": "Message produced successfully",
  "data": {
    "messageId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
    "partition": 3,
    "createdAt": "2026-09-05T10:30:00Z"
  },
  "errorCode": null
}
```

#### `POST /api/v1/producer/messages/batch`

批量生产消息。

**请求体：**

```json
{
  "messages": [
    {
      "topic": "order.created",
      "key": "user-123",
      "payload": "base64EncodedBytes1...",
      "contentType": "application/json",
      "headers": { "source": "order-service" }
    },
    {
      "topic": "order.created",
      "key": "user-456",
      "payload": "base64EncodedBytes2..."
    }
  ]
}
```

**响应（200）：**

```json
{
  "success": true,
  "message": "Batch messages produced successfully",
  "data": {
    "results": [
      { "messageId": "msg-001", "partition": 3, "createdAt": "2026-09-05T10:30:00Z" },
      { "messageId": "msg-002", "partition": 7, "createdAt": "2026-09-05T10:30:00Z" }
    ],
    "totalCount": 2
  },
  "errorCode": null
}
```

### 4.4 消费消息

#### `POST /api/v1/consumer/pull`

拉取消息。

**请求体：**

```json
{
  "topic": "order.created",
  "consumerGroup": "payment-service",
  "consumerId": "payment-instance-1",
  "batchSize": 10,
  "timeoutSeconds": 30
}
```

| 字段          | 类型   | 必填 | 说明                              |
| ------------- | ------ | ---- | --------------------------------- |
| topic         | string | 是   | 主题名称                          |
| consumerGroup | string | 是   | 消费者组名称                      |
| consumerId    | string | 否   | 消费者实例标识                    |
| batchSize     | int    | 否   | 批量拉取数量，默认 10，最大 100   |
| timeoutSeconds| int    | 否   | 长轮询超时（秒），默认 30         |

**响应（200）：**

```json
{
  "success": true,
  "message": "Messages consumed successfully",
  "data": {
    "messages": [
      {
        "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "topic": "order.created",
        "key": "user-123",
        "partition": 3,
        "payload": "base64EncodedBytes...",
        "contentType": "application/json",
        "headers": { "source": "order-service" },
        "createdAt": "2026-09-05T10:30:00Z"
      }
    ],
    "count": 1,
    "hasMore": false
  },
  "errorCode": null
}
```

#### `POST /api/v1/consumer/ack/{messageId}`

确认单条消息。

**请求参数：**

| 字段          | 位置   | 类型   | 必填 | 说明           |
| ------------- | ------ | ------ | ---- | -------------- |
| messageId     | path   | string | 是   | 消息 ID        |
| consumerGroup | query  | string | 是   | 消费者组名称   |
| consumerId    | query  | string | 否   | 消费者实例标识 |

**响应（200）：**

```json
{
  "success": true,
  "message": "Message acknowledged",
  "data": true,
  "errorCode": null
}
```

#### `POST /api/v1/consumer/ack/batch`

批量确认消息。

**请求体：**

```json
{
  "messageIds": ["msg-001", "msg-002", "msg-003"],
  "consumerGroup": "payment-service",
  "consumerId": "payment-instance-1"
}
```

**响应（200）：**

```json
{
  "success": true,
  "message": "Messages acknowledged",
  "data": {
    "acknowledged": 3,
    "failed": 0
  },
  "errorCode": null
}
```

### 4.5 管理接口

#### `POST /api/v1/admin/topics`

创建主题。

**请求体：**

```json
{
  "name": "order.created",
  "partitionCount": 10,
  "defaultTtlSeconds": 86400
}
```

**响应（201）：**

```json
{
  "success": true,
  "message": "Topic created successfully",
  "data": {
    "name": "order.created",
    "partitionCount": 10,
    "defaultTtlSeconds": 86400,
    "createdAt": "2026-09-05T10:00:00Z"
  },
  "errorCode": null
}
```

#### `GET /api/v1/admin/topics/{topic}/depth`

查询队列深度。

**请求参数：**

| 字段          | 位置   | 类型   | 必填 | 说明           |
| ------------- | ------ | ------ | ---- | -------------- |
| topic         | path   | string | 是   | 主题名称       |
| consumerGroup | query  | string | 否   | 消费者组名称   |

**响应（200）：**

```json
{
  "success": true,
  "message": "Queue depth retrieved",
  "data": {
    "topic": "order.created",
    "consumerGroup": "payment-service",
    "depth": 1523,
    "partitions": [
      { "partition": 0, "depth": 152 },
      { "partition": 1, "depth": 148 }
    ]
  },
  "errorCode": null
}
```

#### `POST /api/v1/admin/cleanup`

手动触发清理过期消息。

**响应（200）：**

```json
{
  "success": true,
  "message": "Cleanup completed",
  "data": {
    "deletedExpired": 42,
    "deletedConsumed": 108,
    "totalDeleted": 150
  },
  "errorCode": null
}
```

### 4.6 健康检查

#### `GET /health`

```json
{
  "status": "Healthy",
  "checks": {
    "database": "Healthy",
    "storage": "Healthy"
  },
  "totalDuration": "12ms"
}
```

---

## 5. 路由与分区策略

### 5.1 分区目的

分区在 SimpleMessageBroker 中用于**单节点内的逻辑隔离与并行度提升**，而非分布式分片。所有分区均运行在同一 Broker 节点内，不涉及跨节点分配。

分区的主要作用：
- **消息分组有序**：相同 Key 的消息进入同一分区，分区内按 CreatedAt 有序
- **消费并行度**：不同分区可被并行处理，提升消费吞吐
- **逻辑隔离**：不同业务维度的消息通过分区键自然分组

### 5.2 分区算法

默认使用 **SHA256 哈希取模**：

```
partition = |SHA256(key).ToInt64()| % partitionCount
```

- 输入：消息的 `Key` 字段 + Topic 的 `PartitionCount`
- 输出：`0` 到 `PartitionCount - 1` 的整数
- 特性：相同 Key 始终映射到相同分区，分布均匀

### 5.3 路由策略

| 策略           | 条件                         | 行为                                |
| -------------- | ---------------------------- | ----------------------------------- |
| Key-based      | 消息提供了 Key               | SHA256(Key) % PartitionCount        |
| 轮询（Round-Robin） | 消息未提供 Key          | 在分区间轮询分配                    |
| 自定义         | 实现 `IPartitionStrategy`    | 用户自定义分区计算逻辑              |

**`IPartitionStrategy` 接口：**

```csharp
public interface IPartitionStrategy
{
    int GetPartition(string topic, string? key, int partitionCount);
}
```

### 5.4 顺序性保证

- **分区内有序**：同一分区内的消息按 `CreatedAt` 升序排列
- **同 Key 同分区**：相同 Key 的消息保证进入同一分区，因此同 Key 消息严格有序
- **跨分区无序**：不同分区的消息不保证全局顺序
- **单节点保证**：所有分区在同一节点，无网络延迟导致的乱序问题

---

## 6. 消费模型

### 6.1 队列模式（Queue）

同一 `ConsumerGroup` 内的消费者**竞争消费**消息。每条消息只被组内一个消费者处理。

```
Topic: order.created
ConsumerGroup: payment-service
  ├── Consumer: payment-instance-1  ← 消费 partition 0-4 的消息
  └── Consumer: payment-instance-2  ← 消费 partition 5-9 的消息
```

- 消息被拉取后标记 `IsConsumed = true`，不会被同组其他消费者重复拉取
- 如果消费者拉取后未 ACK（处理崩溃），消息不会被重新投递（at-most-once 语义）
- 如需 at-least-once，可扩展：拉取时不标记 IsConsumed，ACK 时才标记

### 6.2 广播模式（Broadcast）

不同的 `ConsumerGroup` 各自独立消费全量消息。每个 Group 都能收到所有消息。

```
Topic: order.created
  ├── ConsumerGroup: payment-service    ← 独立消费全量
  ├── ConsumerGroup: notification-service ← 独立消费全量
  └── ConsumerGroup: analytics-service  ← 独立消费全量
```

- 每个 ConsumerGroup 维护独立的 ConsumerOffset
- 消息在不同 Group 间不被互斥

### 6.3 Offset 管理

- 每个 `(Topic, ConsumerGroup, Partition)` 组合维护一个 `LastOffset`
- `LastOffset` 基于消息的自增序号（或使用 CreatedAt Ticks）追踪消费进度
- 首次消费时自动初始化所有分区的 Offset 为 0
- Offset 更新与消息消费在同一数据库事务中完成

### 6.4 消费确认（ACK）流程

```
1. Consumer 调用 POST /consumer/pull 拉取消息
2. 服务端返回消息列表，消息状态暂不变更（或标记为"消费中"）
3. Consumer 处理消息
4. Consumer 调用 POST /consumer/ack/{messageId} 确认
5. 服务端标记消息 IsConsumed = true，更新 Offset
```

> **设计决策**：默认采用 pull-then-ack 模式。拉取时消息仍可见，ACK 后才标记已消费。
> 如果 Consumer 拉取后未 ACK，消息可在下次 pull 时被重新拉取（at-least-once）。

---

## 7. 持久化与清理策略

### 7.1 数据库适配

通过 EF Core Provider 机制实现数据库无关：

| 环境   | 数据库     | NuGet 包                                |
| ------ | ---------- | --------------------------------------- |
| 开发   | SQLite     | `Microsoft.EntityFrameworkCore.Sqlite`  |
| 生产   | PostgreSQL | `Npgsql.EntityFrameworkCore.PostgreSQL` |

切换方式：修改 `appsettings.json` 中的 `ConnectionStrings` 和 `DatabaseProvider` 配置项。

### 7.2 消息写入流程

```
1. 接收 ProduceRequest
2. 获取或自动创建 Topic（首次写入自动创建）
3. PartitionRouter 计算分区号
4. 构造 Message 实体，设置 ExpiresAt = UtcNow + TTL
5. 同步写入 Messages 表
6. SaveChanges 成功后返回 ACK（含 messageId, partition）
```

- 写入为同步操作，确保消息落库后才返回成功
- 写入失败时返回错误，客户端可重试

### 7.3 TTL 过期机制

- 消息写入时计算 `ExpiresAt = CreatedAt + TTL`
- TTL 优先级：请求级 > Topic 默认值 > 系统默认值（86400 秒 = 24 小时）
- `ttlSeconds = 0` 或 `ttlSeconds = -1` 表示永不过期
- 过期消息在 pull 时被过滤（查询条件 `ExpiresAt > UtcNow OR ExpiresAt IS NULL`）
- 过期消息由后台清理服务物理删除

### 7.4 后台清理服务

基于 `BackgroundService` 实现的定时清理任务：

```
CleanupService (BackgroundService)
  └── 每隔 CleanupIntervalMinutes 执行一次
      ├── 删除 ExpiresAt < UtcNow 的过期消息
      ├── 删除 IsConsumed = true 且 ConsumedAt < UtcNow - RetainConsumedMinutes 的已消费消息
      └── 记录清理日志（删除数量、耗时）
```

### 7.5 清理策略配置

| 配置项                    | 默认值  | 说明                              |
| ------------------------- | ------- | --------------------------------- |
| CleanupIntervalMinutes    | 30      | 清理执行间隔（分钟）              |
| DefaultTtlSeconds         | 86400   | 消息默认存活时间（秒）            |
| RetainConsumedMinutes     | 60      | 已消费消息保留时间（分钟）        |
| CleanupBatchSize          | 1000    | 单次清理最大删除条数              |

---

## 8. Client SDK 设计

### 8.1 核心接口 `IMessageQueueClient`

SDK 的核心客户端接口，所有操作基于 `byte[]` Payload：

```csharp
public interface IMessageQueueClient
{
    /// <summary>
    /// 生产单条消息
    /// </summary>
    Task<ProduceResult> ProduceAsync(
        string topic,
        byte[] payload,
        string? key = null,
        string? contentType = null,
        Dictionary<string, string>? headers = null,
        int? ttlSeconds = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量生产消息
    /// </summary>
    Task<IReadOnlyList<ProduceResult>> ProduceBatchAsync(
        IReadOnlyList<ProduceMessage> messages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 拉取消息（返回原始 byte[]）
    /// </summary>
    Task<ConsumeResult> ConsumeAsync(
        string topic,
        string consumerGroup,
        string? consumerId = null,
        int batchSize = 10,
        int timeoutSeconds = 30,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 确认单条消息
    /// </summary>
    Task<bool> AcknowledgeAsync(
        string messageId,
        string consumerGroup,
        string? consumerId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量确认消息
    /// </summary>
    Task<BatchAckResult> AcknowledgeBatchAsync(
        IReadOnlyList<string> messageIds,
        string consumerGroup,
        string? consumerId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 查询队列深度
    /// </summary>
    Task<long> GetQueueDepthAsync(
        string topic,
        string? consumerGroup = null,
        CancellationToken cancellationToken = default);
}
```

### 8.2 序列化切面 `IPayloadSerializer`

SDK **不内置任何序列化实现**。用户通过实现 `IPayloadSerializer` 接口自定义序列化/反序列化逻辑：

```csharp
/// <summary>
/// 序列化切面接口 — 用户自定义实现
/// SDK 不内置任何序列化器
/// </summary>
public interface IPayloadSerializer
{
    /// <summary>
    /// 将对象序列化为 byte[]
    /// </summary>
    byte[] Serialize<T>(T obj);

    /// <summary>
    /// 将 byte[] 反序列化为对象
    /// </summary>
    T Deserialize<T>(byte[] data);
}
```

**示例实现 — JSON 序列化器（仅示例，不内置）：**

```csharp
public class JsonPayloadSerializer : IPayloadSerializer
{
    private readonly JsonSerializerOptions _options;

    public JsonPayloadSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    public byte[] Serialize<T>(T obj)
        => JsonSerializer.SerializeToUtf8Bytes(obj, _options);

    public T Deserialize<T>(byte[] data)
        => JsonSerializer.Deserialize<T>(data, _options)!;
}
```

**示例实现 — Protobuf 序列化器（仅示例，不内置）：**

```csharp
public class ProtobufPayloadSerializer : IPayloadSerializer
{
    public byte[] Serialize<T>(T obj)
    {
        using var ms = new MemoryStream();
        ProtoBuf.Serializer.Serialize(ms, obj);
        return ms.ToArray();
    }

    public T Deserialize<T>(byte[] data)
    {
        using var ms = new MemoryStream(data);
        return ProtoBuf.Serializer.Deserialize<T>(ms);
    }
}
```

### 8.3 高级消费者 `IMessageHandler<T>`

泛型消息处理器，结合 `IPayloadSerializer` 实现类型安全的消费：

```csharp
public interface IMessageHandler<T>
{
    Task HandleAsync(T message, MessageContext context, CancellationToken cancellationToken = default);
}

public class MessageContext
{
    public string MessageId { get; init; }
    public string Topic { get; init; }
    public string? Key { get; init; }
    public int Partition { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
    public DateTime CreatedAt { get; init; }
}
```

**使用示例：**

```csharp
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    private readonly ILogger<OrderCreatedHandler> _logger;

    public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger)
    {
        _logger = logger;
    }

    public async Task HandleAsync(OrderCreatedEvent message, MessageContext context, CancellationToken ct)
    {
        _logger.LogInformation("Processing order {OrderId} from partition {Partition}",
            message.OrderId, context.Partition);
        // 业务处理逻辑...
        await Task.CompletedTask;
    }
}
```

### 8.4 DI 集成

```csharp
// Program.cs 或 Startup.cs
builder.Services.AddMessageQueueClient(options =>
{
    options.BaseAddress = "http://localhost:5000";
    options.ApiKey = "your-api-key";
    options.DefaultTimeout = TimeSpan.FromSeconds(30);
    options.MaxRetries = 3;
    options.InitialBackoffMs = 100;
});

// 注册自定义序列化器
builder.Services.AddSingleton<IPayloadSerializer, JsonPayloadSerializer>();

// 注册消息处理器
builder.Services.AddTransient<IMessageHandler<OrderCreatedEvent>, OrderCreatedHandler>();
```

**扩展方法签名：**

```csharp
public static IServiceCollection AddMessageQueueClient(
    this IServiceCollection services,
    Action<MessageQueueClientOptions> configure)
{
    // 注册 IMessageQueueClient
    // 注册 HttpClient 连接池
    // 注册重试策略
    // 注册配置
}
```

### 8.5 连接管理

- 使用 `IHttpClientFactory` 管理 HttpClient 实例
- 默认连接池大小：`MaxConnectionsPerServer = 50`
- 支持 Keep-Alive 长连接
- 请求超时默认 30 秒，可通过配置调整

### 8.6 错误重试

内置指数退避重试策略：

| 参数              | 默认值 | 说明                    |
| ----------------- | ------ | ----------------------- |
| MaxRetries        | 3      | 最大重试次数            |
| InitialBackoffMs  | 100    | 初始退避时间（毫秒）    |
| MaxBackoffMs      | 5000   | 最大退避时间（毫秒）    |
| BackoffMultiplier | 2.0    | 退避倍数                |

**重试条件：** HTTP 5xx、网络超时、连接失败。不重试 4xx 错误。

---

## 9. 错误处理与重试

### 9.1 错误码定义表

| 错误码                     | HTTP 状态码 | 说明                     | 客户端行为       |
| -------------------------- | ----------- | ------------------------ | ---------------- |
| SUCCESS                    | 200         | 成功                     | 正常处理         |
| VALIDATION_ERROR           | 400         | 请求参数验证失败         | 检查参数，不重试 |
| TOPIC_NOT_FOUND            | 404         | 主题不存在               | 先创建 Topic     |
| MESSAGE_NOT_FOUND          | 404         | 消息不存在               | 跳过或告警       |
| MESSAGE_EXPIRED            | 410         | 消息已过期               | 跳过             |
| MESSAGE_ALREADY_CONSUMED   | 409         | 消息已被其他消费者消费   | 跳过             |
| CONSUMER_GROUP_NOT_FOUND   | 404         | 消费者组不存在           | 自动创建         |
| INTERNAL_ERROR             | 500         | 服务端内部错误           | 重试             |
| DATABASE_ERROR             | 500         | 数据库操作失败           | 重试             |
| RATE_LIMITED               | 429         | 请求频率超限             | 退避重试         |

### 9.2 服务端错误处理

全局异常中间件 `GlobalExceptionMiddleware`：

- 捕获所有未处理异常
- 记录错误日志（包含 RequestId、异常堆栈）
- 返回统一 `ApiResponse` 格式
- 数据库异常返回 `DATABASE_ERROR`，其他返回 `INTERNAL_ERROR`
- 生产环境不暴露异常堆栈详情

### 9.3 客户端重试策略

```
请求失败
  ├── HTTP 4xx → 不重试，直接抛出异常
  ├── HTTP 5xx → 指数退避重试
  │     ├── 第 1 次：等待 100ms
  │     ├── 第 2 次：等待 200ms
  │     └── 第 3 次：等待 400ms，仍失败则抛出异常
  └── 网络异常 → 指数退避重试（同上）
```

---

## 10. 安全与认证

### 10.1 API Key 认证

- 客户端在请求头中携带 `X-Api-Key`
- 服务端通过中间件验证 API Key 有效性
- API Key 配置在服务端 `appsettings.json` 中
- 支持多个 API Key（多客户端场景）

```json
{
  "Authentication": {
    "ApiKeys": [
      { "Key": "key-001", "Name": "order-service", "IsActive": true },
      { "Key": "key-002", "Name": "payment-service", "IsActive": true }
    ]
  }
}
```

### 10.2 HMAC 请求签名（可选）

对于高安全要求场景，支持可选的 HMAC 请求签名：

- 客户端使用共享密钥对请求体计算 HMAC-SHA256 签名
- 签名放入请求头 `X-Signature`
- 服务端验证签名一致性
- 启用方式：配置 `Authentication:HmacEnabled = true`

### 10.3 CORS 配置

```json
{
  "Cors": {
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "POST"],
    "AllowedHeaders": ["Content-Type", "X-Api-Key", "X-Request-Id", "X-Client-Id"]
  }
}
```

---

## 11. 监控与指标

### 11.1 计数器指标

| 指标名                        | 类型      | 标签                    | 说明           |
| ----------------------------- | --------- | ----------------------- | -------------- |
| mq_messages_produced_total    | Counter   | topic, partition        | 生产消息总数   |
| mq_messages_consumed_total    | Counter   | topic, consumer_group   | 消费消息总数   |
| mq_messages_expired_total     | Counter   | topic                   | 过期消息总数   |
| mq_messages_cleaned_total     | Counter   | reason(expired/consumed)| 清理消息总数   |
| mq_api_requests_total         | Counter   | endpoint, status_code   | API 请求总数   |

### 11.2 仪表盘指标

| 指标名                        | 类型      | 标签                    | 说明               |
| ----------------------------- | --------- | ----------------------- | ------------------ |
| mq_queue_depth                | Gauge     | topic, consumer_group   | 当前队列深度       |
| mq_produce_latency_ms         | Histogram | topic                   | 生产延迟（毫秒）   |
| mq_consume_latency_ms         | Histogram | topic, consumer_group   | 消费延迟（毫秒）   |
| mq_partition_message_count    | Gauge     | topic, partition        | 各分区消息数量     |

### 11.3 健康检查

`GET /health` 端点使用 ASP.NET Core Health Check 中间件：

- **database**：验证数据库连接可用性
- **storage**：验证磁盘空间充足

### 11.4 日志规范

| 日志级别   | 场景                                   |
| ---------- | -------------------------------------- |
| Trace      | 请求进入/退出、分区计算详情            |
| Debug      | SQL 查询、Offset 更新                  |
| Information| 消息生产/消费/确认、Topic 创建、清理完成|
| Warning    | 重试、慢查询、接近容量上限             |
| Error      | 数据库异常、未处理异常                 |
| Critical   | 数据库连接失败、服务启动失败           |

---

## 12. 配置参考

### 12.1 appsettings.json 完整配置

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning",
      "Microsoft.EntityFrameworkCore": "Warning"
    }
  },
  "ConnectionStrings": {
    "DefaultConnection": "Data Source=messagequeue.db"
  },
  "DatabaseProvider": "Sqlite",
  "MessageQueue": {
    "CleanupIntervalMinutes": 30,
    "DefaultTtlSeconds": 86400,
    "RetainConsumedMinutes": 60,
    "CleanupBatchSize": 1000,
    "MaxBatchSize": 100,
    "DefaultPartitionCount": 10
  },
  "Authentication": {
    "Enabled": false,
    "ApiKeys": [],
    "HmacEnabled": false,
    "HmacSecret": ""
  },
  "Cors": {
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "POST"],
    "AllowedHeaders": ["Content-Type", "X-Api-Key", "X-Request-Id", "X-Client-Id"]
  }
}
```

### 12.2 配置项说明

| 配置路径                              | 类型     | 默认值                    | 说明                       |
| ------------------------------------- | -------- | ------------------------- | -------------------------- |
| ConnectionStrings:DefaultConnection   | string   | `Data Source=messagequeue.db` | 数据库连接字符串           |
| DatabaseProvider                      | string   | `Sqlite`                  | 数据库提供程序（Sqlite/Postgres） |
| MessageQueue:CleanupIntervalMinutes   | int      | 30                        | 清理执行间隔（分钟）       |
| MessageQueue:DefaultTtlSeconds        | int      | 86400                     | 消息默认 TTL（秒）         |
| MessageQueue:RetainConsumedMinutes    | int      | 60                        | 已消费消息保留时间（分钟） |
| MessageQueue:CleanupBatchSize         | int      | 1000                      | 单次清理最大删除条数       |
| MessageQueue:MaxBatchSize             | int      | 100                       | 单次批量生产/消费最大条数  |
| MessageQueue:DefaultPartitionCount    | int      | 10                        | 自动创建 Topic 时的默认分区数 |
| Authentication:Enabled                | bool     | false                     | 是否启用 API Key 认证      |
| Authentication:HmacEnabled            | bool     | false                     | 是否启用 HMAC 签名         |

### 12.3 环境变量覆盖

所有配置项均可通过环境变量覆盖，格式为 `Section__Key`：

```bash
# 示例
ConnectionStrings__DefaultConnection="Host=localhost;Database=mq;Username=mq;Password=secret"
DatabaseProvider="Postgres"
MessageQueue__DefaultTtlSeconds=172800
```

### 12.4 Docker 部署配置

```yaml
# docker-compose.yml
version: '3.8'

services:
  message-broker:
    image: simple-message-broker:latest
    ports:
      - "5000:8080"
    environment:
      - DatabaseProvider=Postgres
      - ConnectionStrings__DefaultConnection=Host=postgres;Database=messagequeue;Username=mq;Password=secret
      - MessageQueue__DefaultTtlSeconds=86400
      - MessageQueue__CleanupIntervalMinutes=30
      - Authentication__Enabled=false
    depends_on:
      - postgres
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  postgres:
    image: postgres:16-alpine
    environment:
      - POSTGRES_DB=messagequeue
      - POSTGRES_USER=mq
      - POSTGRES_PASSWORD=secret
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  pgdata:
```

---

## 13. 性能基准（预期）

> 以下为预期性能指标，基于单节点、中等配置硬件（4C8G）的估算。实际性能取决于硬件、网络、数据量等因素。

### 13.1 单条操作

| 操作             | 数据库     | 吞吐量 (msg/s) | 延迟 p50 (ms) | 延迟 p99 (ms) |
| ---------------- | ---------- | -------------- | ------------- | ------------- |
| 生产（单条）     | SQLite     | ~1,000         | 2             | 10            |
| 生产（单条）     | PostgreSQL | ~2,000         | 1             | 5             |
| 消费（单条拉取） | SQLite     | ~800           | 3             | 15            |
| 消费（单条拉取） | PostgreSQL | ~1,500         | 1             | 8             |

### 13.2 批量操作

| 操作              | 数据库     | 吞吐量 (msg/s) | 延迟 p50 (ms) | 延迟 p99 (ms) |
| ----------------- | ---------- | -------------- | ------------- | ------------- |
| 批量生产 (100条)  | SQLite     | ~5,000         | 15            | 50            |
| 批量生产 (100条)  | PostgreSQL | ~8,000         | 10            | 30            |
| 批量确认 (100条)  | SQLite     | ~3,000         | 20            | 60            |
| 批量确认 (100条)  | PostgreSQL | ~6,000         | 12            | 40            |

### 13.3 SQLite vs PostgreSQL 差异说明

| 维度           | SQLite                              | PostgreSQL                          |
| -------------- | ----------------------------------- | ----------------------------------- |
| 适用场景       | 开发、测试、轻量级单机部署          | 生产环境、高并发场景                |
| 并发写入       | 单写者锁，并发写入受限              | MVCC，支持高并发写入                |
| 查询性能       | 小数据量下性能良好                  | 大数据量下查询优化器更优            |
| 运维成本       | 零运维，单文件                      | 需要独立部署和维护                  |
| 数据量上限     | 理论 281TB，实际建议 < 10GB         | 无实际限制                          |

---

## 附录 A：完整 curl API 调用示例

### A.1 创建主题

```bash
curl -X POST http://localhost:5000/api/v1/admin/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "order.created",
    "partitionCount": 10,
    "defaultTtlSeconds": 86400
  }'
```

### A.2 生产单条消息

```bash
curl -X POST http://localhost:5000/api/v1/producer/messages \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "order.created",
    "key": "user-123",
    "payload": "eyJvcmRlcklkIjogMTIzLCAiYW1vdW50IjogOTkuOTl9",
    "contentType": "application/json",
    "headers": {
      "source": "order-service",
      "trace-id": "abc-123"
    },
    "ttlSeconds": 3600
  }'
```

### A.3 批量生产消息

```bash
curl -X POST http://localhost:5000/api/v1/producer/messages/batch \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "topic": "order.created",
        "key": "user-123",
        "payload": "eyJvcmRlcklkIjogMX0=",
        "contentType": "application/json"
      },
      {
        "topic": "order.created",
        "key": "user-456",
        "payload": "eyJvcmRlcklkIjogMn0="
      }
    ]
  }'
```

### A.4 拉取消息

```bash
curl -X POST http://localhost:5000/api/v1/consumer/pull \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "order.created",
    "consumerGroup": "payment-service",
    "consumerId": "payment-instance-1",
    "batchSize": 10,
    "timeoutSeconds": 30
  }'
```

### A.5 确认单条消息

```bash
curl -X POST "http://localhost:5000/api/v1/consumer/ack/d290f1ee-6c54-4b01-90e6-d701748f0851?consumerGroup=payment-service&consumerId=payment-instance-1"
```

### A.6 批量确认消息

```bash
curl -X POST http://localhost:5000/api/v1/consumer/ack/batch \
  -H "Content-Type: application/json" \
  -d '{
    "messageIds": ["msg-001", "msg-002", "msg-003"],
    "consumerGroup": "payment-service",
    "consumerId": "payment-instance-1"
  }'
```

### A.7 查询队列深度

```bash
curl "http://localhost:5000/api/v1/admin/topics/order.created/depth?consumerGroup=payment-service"
```

### A.8 手动清理

```bash
curl -X POST http://localhost:5000/api/v1/admin/cleanup
```

### A.9 健康检查

```bash
curl http://localhost:5000/health
```

---

## 附录 B：数据库迁移指南（SQLite → PostgreSQL）

### B.1 迁移步骤

1. **更新 NuGet 包**：移除 `Microsoft.EntityFrameworkCore.Sqlite`，添加 `Npgsql.EntityFrameworkCore.PostgreSQL`
2. **更新连接字符串**：修改为 PostgreSQL 格式 `Host=xxx;Database=xxx;Username=xxx;Password=xxx`
3. **更新 DatabaseProvider 配置**：设置为 `Postgres`
4. **数据类型映射**：
   - SQLite `TEXT` → PostgreSQL `VARCHAR(n)` / `TIMESTAMP WITH TIME ZONE`
   - SQLite `BLOB` → PostgreSQL `BYTEA`
   - SQLite `INTEGER` (boolean) → PostgreSQL `BOOLEAN`
   - SQLite `TEXT` (JSON) → PostgreSQL `JSONB`
5. **重建索引**：使用 PostgreSQL DDL 重新创建索引
6. **数据迁移**：如需迁移已有数据，使用 ETL 工具或自定义脚本

### B.2 注意事项

- SQLite 的 `DATETIME` 存储为文本，PostgreSQL 使用原生时间类型
- PostgreSQL 的 `JSONB` 支持索引和查询，比 SQLite 的 JSON 文本更高效
- PostgreSQL 支持并发写入（MVCC），适合生产环境
- 迁移后需重新执行 `EnsureCreated()` 或 EF Migrations

---

## 附录 C：变更日志

| 版本   | 日期       | 变更内容               |
| ------ | ---------- | ---------------------- |
| v1.0.0 | 2026-09-05 | 初始版本，包含完整技术规格 |
