"""逻辑复制 DDL 路由与阶段规划器。

根据订阅端 LSN、schema 版本、表重写、双写状态与回填进度,
为 DDL 变更生成有序的阶段计划(扩展结构 -> 双写 -> 回填 ->
追平 -> 切换读取 -> 清理旧结构),并保证每个订阅端在接收对应
DML 之前已具备兼容结构。输出仅为路由计划,不直接执行 SQL。
"""

from .models import (
    ChangeKind,
    ChangeRequest,
    DMLBatch,
    PlanStep,
    PublisherState,
    RoutePlan,
    Stage,
    SubscriberState,
    TableChange,
)
from .planner import PhasePlanner
from .router import DDLSafetyRouter
from .rollback import RollbackPlanner

__all__ = [
    "ChangeKind",
    "ChangeRequest",
    "DMLBatch",
    "PlanStep",
    "PublisherState",
    "RoutePlan",
    "Stage",
    "SubscriberState",
    "TableChange",
    "PhasePlanner",
    "DDLSafetyRouter",
    "RollbackPlanner",
]

__version__ = "0.1.0"
