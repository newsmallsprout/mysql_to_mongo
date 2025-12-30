MySQL → MongoDB 数据同步服务
（Full Sync + Binlog CDC + 数据版本化）




项目简介
这是一个面向生产环境的 MySQL → MongoDB 数据同步服务，
支持：
全量同步（Full Sync）
Binlog 增量同步（ROW 模式 CDC）
UPDATE 版本化（历史数据可追溯）
DELETE 软删除（可恢复、可审计）
重点解决 DECIMAL 精度、安全性、可审计性 三大生产级难题。
核心特性
✅ MySQL 全量数据同步
✅ MySQL Binlog（ROW）增量同步
✅ UPDATE 数据版本化（历史保留）
✅ DELETE 软删除（审计 / 回滚友好）
✅ Decimal 防崩溃 & 防精度丢失
✅ 同步任务 & Binlog 位点持久化
✅ FastAPI 管理接口
✅ Docker 生产部署友好
整体架构
MySQL (ROW Binlog)
        │
        ▼
   Sync Engine
 (Full + CDC)
        │
        ▼
MongoDB
 ├── Base Collection    (当前态)
 └── Version Collection (历史版本)
快速开始（Quick Start）
1️⃣ 克隆项目
git clone https://github.com/your-org/mysql-to-mongo.git
cd mysql-to-mongo
2️⃣ 安装依赖
pip install -r requirements.txt
3️⃣ 启动服务
uvicorn app.main:app --host 0.0.0.0 --port 8000
访问 Swagger：
http://localhost:8000/docs
Docker 部署（推荐）
构建镜像
docker build -t mysql-to-mongo .
运行容器
docker run -d \
  --name mysql-to-mongo \
  -p 8000:8000 \
  -e PYTHONUNBUFFERED=1 \
  -v /opt/mysql-to-mongo/configs:/app/configs \
  -v /opt/mysql-to-mongo/state:/app/state \
  --restart unless-stopped \
  mysql-to-mongo
目录结构
mysql_to_mongo/
├── app/
│   ├── main.py                  # FastAPI 入口
│   ├── api/                     # 同步任务 API
│   ├── core/
│   │   ├── config_store.py      # 任务配置持久化
│   │   ├── state_store.py       # Binlog 位点持久化
│   │   └── logging.py
│   └── sync/
│       ├── worker.py            # 同步主流程
│       ├── task_manager.py      # 任务生命周期管理
│       ├── mysql_introspector.py
│       ├── mongo_writer.py
│       ├── flush_buffer.py
│       └── convert.py           # 数据转换核心
├── Dockerfile
├── requirements.txt
└── README.md
数据同步规则
INSERT
写入 Base Collection
主键字段可配置是否作为 MongoDB _id
支持 upsert，避免重复写入
UPDATE（版本化）
Base Collection：
镜像模式（覆盖）
审计模式（保留原值）
Version Collection：
每次 UPDATE 都新增一条历史版本
{
  "_id": "1001_20250101123000",
  "ref_id": 1001,
  "version": 3,
  "data": {
    "balance": "1200.00"
  },
  "updated_at": "2025-01-01T12:30:00"
}
DELETE（软删除）
❌ 不物理删除
✅ 标记 deleted / deleted_at
✅ 历史数据完整保留
✅ 支持审计 / 回滚 / 对账
Decimal 设计说明（架构级）
背景问题
同步链路：
MySQL DECIMAL
   ↓
Python Decimal
   ↓
MongoDB Decimal128
Python Decimal 在以下情况极易抛异常并导致同步任务崩溃：
精度不一致
scale 不固定
quantize 失败
科学计数法参与运算
MongoDB 若不使用 Decimal128，会直接产生精度丢失。
统一处理策略（核心）
所有 DECIMAL 统一规则：
MySQL DECIMAL → Python Decimal
强制 quantize 到统一 scale
使用 ROUND_DOWN 截断
转换为 MongoDB Decimal128
同时保留字符串字段 xxx_str 用于对账
关键实现逻辑（convert.py）
dq = obj.quantize(self.DEC_Q, rounding=ROUND_DOWN)
return Decimal128(dq)
DEC_Q：统一精度模板
所有 Decimal 先规整、再入库
从根源避免 decimal.InvalidOperation
设计收益
✅ 同步任务永不因 Decimal CRASH
✅ MongoDB 精度稳定
✅ 可人工 / 程序化对账
✅ 金融 / 账务 / 审计级安全
任务持久化与恢复
配置持久化
/app/configs
Binlog 位点持久化
/app/state
保存内容：
binlog file
binlog position
服务重启行为
自动加载所有任务配置
自动恢复同步任务
从上次 Binlog 位点继续
不重复消费历史数据
适用场景
审计系统
风控系统
历史回溯
MongoDB 查询加速
金融 / 账务系统
License
MIT License