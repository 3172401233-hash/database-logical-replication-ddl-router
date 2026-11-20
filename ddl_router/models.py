"""数据模型:订阅端状态、DDL 变更请求与路由计划。

兼容 Python 3.8.2。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class ChangeKind(enum.Enum):
    """受支持的 DDL 变更类型。"""

    ALTER_COLUMN_TYPE = "alter_column_type"
    DROP_COLUMN = "drop_column"
    ADD_UNIQUE_CONSTRAINT = "add_unique_constraint"
    ADD_COLUMN = "add_column"


class Stage(enum.Enum):
    """阶段规划器可生成的阶段。"""

    EXPAND = "expand"                # 扩展结构:新增兼容列/索引(NOT VALID)
    REWRITE = "rewrite"              # 表重写:需要重建堆/索引的变更
    DUAL_WRITE = "dual_write"        # 双写:同时写旧结构与新结构
    BACKFILL = "backfill"            # 回填:历史数据迁移到新结构
    WAIT_CATCHUP = "wait_catchup"    # 屏障:等待订阅端 LSN 追平
    VALIDATE = "validate"            # 校验:去重/抽样比对回填结果
    SWITCH_READ = "switch_read"      # 切换读取:读流量切到新结构
    ENFORCE = "enforce"              # 生效:约束置为 VALID
    CLEANUP = "cleanup"              # 清理:删除旧结构(通常不可逆)
    DEFER = "defer"                  # 延期:因落后消费者推迟后续阶段
    RESNAPSHOT = "resnapshot"        # 重快照:为长期落后的订阅端重建链路
    ROLLBACK = "rollback"            # 回滚:仅作用于受影响链路


# 失败后无法简单逆向执行的阶段。
IRREVERSIBLE_STAGES = frozenset({Stage.CLEANUP})


@dataclass
class SubscriberState:
    """单个订阅端(报表/搜索/归档等消费者)的复制状态。"""

    name: str
    role: str
    lsn: int
    schema_version: int
    lag_bytes: int = 0
    max_lag_bytes: int = 64 * 1024 * 1024
    dual_write_active: bool = False
    backfill_progress: float = 1.0  # 0.0 ~ 1.0

    @property
    def is_lagging(self) -> bool:
        """落后超过容忍阈值,视为长期落后消费者。"""
        return self.lag_bytes > self.max_lag_bytes

    def has_compatible_structure(self, required_version: int) -> bool:
        return self.schema_version >= required_version


@dataclass
class PublisherState:
    """发布端当前状态。"""

    current_lsn: int
    schema_version: int


@dataclass
class TableChange:
    """单表上的一次 DDL 变更描述。"""

    table: str
    kind: ChangeKind
    column: Optional[str] = None
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    constraint_name: Optional[str] = None
    requires_rewrite: bool = False

    @property
    def irreversible(self) -> bool:
        # 删除列后旧数据无法从流中恢复,属于不可逆变更。
        return self.kind is ChangeKind.DROP_COLUMN


@dataclass
class ChangeRequest:
    """一次 DDL 变更请求;changes 多于一条且 atomic 时为多表原子变更。"""

    change_id: str
    changes: List[TableChange]
    atomic: bool = False

    @property
    def tables(self) -> List[str]:
        seen = []
        for change in self.changes:
            if change.table not in seen:
                seen.append(change.table)
        return seen


@dataclass
class DMLBatch:
    """一批待广播的 DML,携带其 schema 版本与 LSN 位置。"""

    schema_version: int
    lsn: int
    tables: List[str] = field(default_factory=list)


@dataclass
class PlanStep:
    """路由计划中的一个有序步骤。

    sql_hint 仅为人类可读的说明,规划器不执行任何 SQL。
    """

    step_id: str
    stage: Stage
    description: str
    tables: List[str] = field(default_factory=list)
    subscribers: List[str] = field(default_factory=list)  # 空表示发布端/全局
    preconditions: List[str] = field(default_factory=list)
    reversible: bool = True
    sql_hint: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "step_id": self.step_id,
            "stage": self.stage.value,
            "description": self.description,
            "tables": list(self.tables),
            "subscribers": list(self.subscribers),
            "preconditions": list(self.preconditions),
            "reversible": self.reversible,
            "sql_hint": self.sql_hint,
        }


@dataclass
class RoutePlan:
    """一次变更的完整路由计划(仅计划,不执行)。"""

    change_id: str
    schema_version_from: int
    schema_version_to: int
    steps: List[PlanStep] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def stages(self) -> List[Stage]:
        return [step.stage for step in self.steps]

    def find_step(self, step_id: str) -> Optional[PlanStep]:
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def to_dict(self) -> Dict[str, object]:
        return {
            "change_id": self.change_id,
            "schema_version_from": self.schema_version_from,
            "schema_version_to": self.schema_version_to,
            "steps": [step.to_dict() for step in self.steps],
            "notes": list(self.notes),
        }
