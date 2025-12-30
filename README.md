# MySQL → MongoDB 数据同步服务

一个面向**生产环境**的 MySQL 到 MongoDB 数据同步服务，支持：

- **全量快照同步（FullSync）**
- **增量 Binlog 同步（CDC / Incremental Sync）**

---

## 1. 背景与目标

适用于以下场景：

- 冷数据存储
- 账务 / 资产快照
- 查询分析（OLAP / 风控）
- 历史留存 / 审计

设计目标：

- 可长期稳定运行
- 对大表友好
- 金融级数值精度
- 具备可观测性
- 可直接用于生产环境

---

## 2. 核心能力

### 2.1 同步模式

#### 全量同步（FullSync）

- 主键递增分段扫描（无 OFFSET）
- MongoDB 批量写入（insert_many）
- 百万 / 千万级数据
- 实时进度输出
- 可关闭 journaling 提升性能

#### 增量同步（CDC）

- MySQL Binlog（ROW）
- Insert / Update / Delete
- 位点持久化
- 自动衔接 FullSync

---

## 3. 系统架构

~~~text
+----------------+        +------------------------+
|     MySQL      | -----> |   FullSync 全量同步     |
|   (InnoDB)     |        | 主键分段 + 批量写入     |
+----------------+        +------------------------+
        |
        v
+------------------------+
|  Binlog CDC 增量同步   |
|  ROW + 位点持久化      |
+------------------------+
        |
        v
+----------------------+
|       MongoDB        |
|     (ReplicaSet)     |
+----------------------+
~~~

---

## 4. 接口说明

### 4.1 启动同步任务

**POST** `/tasks/start`

~~~json
{
  "task_id": "job_cold_account",
  "mysql_conf": {
    "host": "mysql.example.com",
    "port": 3306,
    "user": "sync_user",
    "password": "******",
    "database": "mysql_database"
  },
  "mongo_conf": {
    "user": "mongo_admin",
    "password": "******",
    "database": "mongo_databasse",
    "replica_set": "rs",
    "hosts": [
      "xxx:27017",
      "xxx:27017",
      "xxx:27017"
    ]
  },
  "table_map": {},
  "pk_field": "id",
  "progress_interval": 10
}
~~~

---

### 4.2 停止同步任务

**POST** `/tasks/stop/{task_id}`

---

### 4.3 服务状态

**GET** `/`

---

## 5. 同步日志示例

~~~text
[job_cold_account] Table start: table, total=2111477
[job_cold_account] Prog: done=300000/2111477 14.2% sp=12800 row/s
[job_cold_account] Prog: done=1200000/2111477 56.8% sp=14500 row/s
[job_cold_account] Table done: inserted=2111477
~~~

---

## 6. 已知限制

- Insert-only（不支持 upsert）
- 依赖递增主键
- Binlog 必须为 ROW
- 不保证跨表事务一致性
- 追加模式，不删除，update 也是添加新行
