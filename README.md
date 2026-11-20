# database-logical-replication-ddl-router
协调逻辑复制中的 DDL 顺序、双版本和失败回放

## 背景

逻辑复制同时连接报表、搜索和归档等多个消费者。一次字段类型修改
如果在 DML 追平前广播,旧消费者会按错误格式解析数据。本项目实现
一个 DDL 路由与阶段规划器(Python 3.8.2),根据订阅端 LSN、schema
版本、表重写、双写状态和回填进度,安排以下阶段的顺序:

```
扩展结构(EXPAND) -> [表重写(REWRITE)] -> 双写(DUAL_WRITE)
  -> 回填(BACKFILL) -> 追平屏障(WAIT_CATCHUP) -> [校验(VALIDATE)]
  -> 切换读取(SWITCH_READ) / 约束生效(ENFORCE) -> 清理旧结构(CLEANUP)
```

核心不变式:**每个订阅端都必须先具备兼容结构,再接收对应 schema
版本的 DML**;不可逆变更失败时只回滚受影响的订阅端链路。

## 重要说明

**输出仅为路由计划,不直接执行 SQL。** 计划中的 `sql_hint` 字段只是
人类可读的说明性提示,执行与否由外部运维系统决定。

## 模块

- `ddl_router/models.py` — 订阅端状态、变更请求、计划步骤等数据模型
- `ddl_router/planner.py` — `PhasePlanner`:生成有序阶段计划;
  处理删除列(先停写再清理)、唯一约束(先校验再生效)、
  多表原子变更(原子屏障)、长期落后消费者(重快照 + 清理延期)
- `ddl_router/router.py` — `DDLSafetyRouter`:按订阅端 schema 版本
  与落后状态门控 DML 广播
- `ddl_router/rollback.py` — `RollbackPlanner`:不可逆失败只对
  受影响链路生成重快照;可逆失败按相反顺序撤销已完成步骤

## 使用

```bash
python -m ddl_router        # 打印一份演示路由计划(JSON)
python -m unittest discover -s tests   # 运行全部测试
```

## 示例

```python
from ddl_router import (
    ChangeKind, ChangeRequest, PhasePlanner, PublisherState,
    SubscriberState, TableChange,
)

publisher = PublisherState(current_lsn=1000, schema_version=3)
subscribers = [
    SubscriberState(name="report", role="report", lsn=1000, schema_version=3),
    SubscriberState(name="search", role="search", lsn=990, schema_version=3),
    SubscriberState(name="archive", role="archive", lsn=800, schema_version=3),
]
request = ChangeRequest(
    change_id="chg-1",
    changes=[TableChange(table="orders",
                         kind=ChangeKind.ALTER_COLUMN_TYPE,
                         column="amount", new_type="numeric(14,2)")],
)
plan = PhasePlanner(publisher, subscribers).plan(request)
for step in plan.steps:
    print(step.step_id, step.stage.value, step.description)
```
