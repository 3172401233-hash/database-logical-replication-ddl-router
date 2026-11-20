"""阶段规划器:把一次 DDL 变更编排为有序、带门控条件的步骤序列。

核心不变式:任何订阅端必须先具备兼容结构(EXPAND 已应用),
对应 schema 版本的 DML 才允许广播给它。
"""

from __future__ import annotations

from typing import List

from .models import (
    ChangeKind,
    ChangeRequest,
    PlanStep,
    PublisherState,
    RoutePlan,
    Stage,
    SubscriberState,
    TableChange,
)


class PhasePlanner:
    """根据发布端与订阅端状态生成 DDL 路由计划。"""

    def __init__(self, publisher, subscribers):
        # type: (PublisherState, List[SubscriberState]) -> None
        self.publisher = publisher
        self.subscribers = list(subscribers)
        self._seq = 0

    def plan(self, request):
        # type: (ChangeRequest) -> RoutePlan
        self._seq = 0
        plan = RoutePlan(
            change_id=request.change_id,
            schema_version_from=self.publisher.schema_version,
            schema_version_to=self.publisher.schema_version + 1,
        )
        dual_write_start_lsn = self.publisher.current_lsn

        phases = [
            self._expand_phase(request),
            self._dual_write_phase(request, dual_write_start_lsn),
            self._backfill_phase(request),
            self._catchup_phase(request, dual_write_start_lsn),
            self._switch_phase(request),
            self._cleanup_phase(request),
        ]
        for steps in phases:
            plan.steps.extend(steps)

        self._handle_lagging_subscribers(plan)
        return plan

    def _expand_phase(self, request):
        # type: (ChangeRequest) -> List[PlanStep]
        steps = []
        for change in request.changes:
            steps.append(self._expand_step(request, change))
            if change.requires_rewrite:
                steps.append(self._step(
                    request, Stage.REWRITE,
                    "表 %s 需要重写(类型转换无法就地完成),安排在维护窗口内重建"
                    % change.table,
                    tables=[change.table],
                    preconditions=["维护窗口已确认", "双写触发器已就位"],
                    sql_hint="-- 仅计划:重建表 %s 并保留触发器双写" % change.table,
                ))
        if request.atomic and len(request.tables) > 1:
            steps.append(self._barrier(request, Stage.EXPAND, request.tables))
        return steps

    def _dual_write_phase(self, request, start_lsn):
        # type: (ChangeRequest, int) -> List[PlanStep]
        steps = []
        for change in request.changes:
            if change.kind is ChangeKind.DROP_COLUMN:
                continue  # 删除列走"先停写"路径,无双写阶段
            preconditions = [
                "订阅端 %s schema_version >= %d(兼容结构已就位)"
                % (sub.name, self.publisher.schema_version + 1)
                for sub in self.subscribers
            ]
            steps.append(self._step(
                request, Stage.DUAL_WRITE,
                "表 %s 开启双写:旧结构与新结构同时写入" % change.table,
                tables=[change.table],
                preconditions=preconditions,
                sql_hint="-- 仅计划:为 %s 安装双写触发器" % change.table,
            ))
        if steps and request.atomic and len(request.tables) > 1:
            steps.append(self._barrier(request, Stage.DUAL_WRITE,
                                       request.tables))
        return steps

    def _backfill_phase(self, request):
        # type: (ChangeRequest) -> List[PlanStep]
        steps = []
        for change in request.changes:
            if change.kind is ChangeKind.DROP_COLUMN:
                continue
            steps.append(self._step(
                request, Stage.BACKFILL,
                "回填表 %s 历史数据到新结构" % change.table,
                tables=[change.table],
                preconditions=["双写已开启"],
                sql_hint="-- 仅计划:分批回填 %s,记录进度" % change.table,
            ))
        return steps

    def _catchup_phase(self, request, start_lsn):
        # type: (ChangeRequest, int) -> List[PlanStep]
        preconditions = [
            "订阅端 %s lsn >= %d 且 backfill_progress == 1.0"
            % (sub.name, start_lsn)
            for sub in self.subscribers
        ]
        return [self._step(
            request, Stage.WAIT_CATCHUP,
            "屏障:等待全部订阅端追平双写起点 LSN %d" % start_lsn,
            tables=request.tables,
            subscribers=[sub.name for sub in self.subscribers],
            preconditions=preconditions,
        )]

    def _switch_phase(self, request):
        # type: (ChangeRequest) -> List[PlanStep]
        steps = []
        for change in request.changes:
            if change.kind is ChangeKind.ADD_UNIQUE_CONSTRAINT:
                steps.append(self._step(
                    request, Stage.VALIDATE,
                    "校验表 %s 回填结果无重复键" % change.table,
                    tables=[change.table],
                    preconditions=["回填完成", "双写期间无新增冲突"],
                ))
                steps.append(self._step(
                    request, Stage.ENFORCE,
                    "约束 %s 置为 VALID 并切换读取"
                    % (change.constraint_name or "unique_constraint"),
                    tables=[change.table],
                    preconditions=["校验通过"],
                    sql_hint="-- 仅计划:VALIDATE CONSTRAINT",
                ))
            elif change.kind is ChangeKind.DROP_COLUMN:
                steps.append(self._step(
                    request, Stage.SWITCH_READ,
                    "停止读写 %s.%s,读取切换到不含该列的版本"
                    % (change.table, change.column),
                    tables=[change.table],
                    preconditions=["全部订阅端已应用弃用该列的 schema 版本"],
                ))
            else:
                steps.append(self._step(
                    request, Stage.SWITCH_READ,
                    "读取切换到表 %s 的新结构" % change.table,
                    tables=[change.table],
                    preconditions=["追平屏障已通过", "回填校验通过"],
                ))
        if request.atomic and len(request.tables) > 1:
            steps.append(self._barrier(request, Stage.SWITCH_READ,
                                       request.tables))
        return steps

    def _cleanup_phase(self, request):
        # type: (ChangeRequest) -> List[PlanStep]
        steps = []
        for change in request.changes:
            if change.kind is ChangeKind.DROP_COLUMN:
                description = "删除列 %s.%s(不可逆)" % (change.table,
                                                       change.column)
                hint = "-- 仅计划:DROP COLUMN %s" % change.column
            elif change.kind is ChangeKind.ALTER_COLUMN_TYPE:
                description = "删除表 %s 的旧类型列(不可逆)" % change.table
                hint = "-- 仅计划:DROP 旧列"
            elif change.kind is ChangeKind.ADD_UNIQUE_CONSTRAINT:
                description = "清理表 %s 的临时去重辅助结构" % change.table
                hint = None
            else:
                continue  # ADD_COLUMN 无旧结构可清理
            steps.append(self._step(
                request, Stage.CLEANUP, description,
                tables=[change.table],
                preconditions=[
                    "全部订阅端已切换读取",
                    "全部订阅端 lsn 超过切换点",
                ],
                reversible=False,
                sql_hint=hint,
            ))
        return steps

    def _expand_step(self, request, change):
        # type: (ChangeRequest, TableChange) -> PlanStep
        if change.kind is ChangeKind.ALTER_COLUMN_TYPE:
            description = ("扩展结构:为表 %s 新增 %s 类型的影子列"
                           % (change.table, change.new_type))
            hint = "-- 仅计划:ADD COLUMN %s__new %s" % (change.column,
                                                        change.new_type)
        elif change.kind is ChangeKind.ADD_UNIQUE_CONSTRAINT:
            description = ("扩展结构:为表 %s 创建唯一索引(NOT VALID)"
                           % change.table)
            hint = "-- 仅计划:CREATE UNIQUE INDEX ... NOT VALID"
        elif change.kind is ChangeKind.DROP_COLUMN:
            description = ("扩展结构:广播弃用 %s.%s 的 schema 版本"
                           % (change.table, change.column))
            hint = None
        else:
            description = ("扩展结构:为表 %s 新增列 %s"
                           % (change.table, change.column))
            hint = "-- 仅计划:ADD COLUMN %s" % change.column
        return self._step(
            request, Stage.EXPAND, description,
            tables=[change.table],
            preconditions=["发布端与全部订阅端均可应用该扩展(向后兼容)"],
            sql_hint=hint,
        )

    def _barrier(self, request, stage, tables):
        # type: (ChangeRequest, Stage, List[str]) -> PlanStep
        return self._step(
            request, Stage.WAIT_CATCHUP,
            "原子屏障:%s 阶段必须在全部表 %s 上同时完成"
            % (stage.value, ",".join(tables)),
            tables=list(tables),
            preconditions=["多表原子变更:任一表未完成则整体等待"],
        )

    def _handle_lagging_subscribers(self, plan):
        # type: (RoutePlan) -> None
        """长期落后的消费者:重快照并推迟清理阶段。"""
        lagging = [sub for sub in self.subscribers if sub.is_lagging]
        if not lagging:
            return
        cleanup_steps = [s for s in plan.steps if s.stage is Stage.CLEANUP]
        lagging_names = ",".join(sub.name for sub in lagging)
        for sub in lagging:
            plan.steps.append(PlanStep(
                step_id=self._next_id(plan.change_id, Stage.RESNAPSHOT),
                stage=Stage.RESNAPSHOT,
                description=("订阅端 %s 落后 %d 字节超过阈值,执行分离并重快照"
                             % (sub.name, sub.lag_bytes)),
                subscribers=[sub.name],
                preconditions=["确认该订阅端允许重建复制链路"],
                reversible=False,
            ))
            plan.notes.append(
                "订阅端 %s 长期落后:清理阶段延期至其重快照完成" % sub.name)
        for step in cleanup_steps:
            step.preconditions.append(
                "落后订阅端 %s 已完成重快照" % lagging_names)
            plan.steps.append(PlanStep(
                step_id=self._next_id(plan.change_id, Stage.DEFER),
                stage=Stage.DEFER,
                description="清理 %s 延期:等待落后订阅端重快照"
                            % ",".join(step.tables),
                tables=list(step.tables),
                subscribers=[sub.name for sub in lagging],
            ))

    def _step(self, request, stage, description, **kwargs):
        # type: (ChangeRequest, Stage, str, ...) -> PlanStep
        return PlanStep(
            step_id=self._next_id(request.change_id, stage),
            stage=stage,
            description=description,
            **kwargs
        )

    def _next_id(self, change_id, stage):
        # type: (str, Stage) -> str
        self._seq += 1
        return "%s-%02d-%s" % (change_id, self._seq, stage.value)
