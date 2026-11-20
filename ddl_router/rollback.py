"""失败回滚规划器。

不可逆变更失败时,只对受影响的订阅端链路生成回滚/修复步骤,
健康链路保持不变。
"""

from __future__ import annotations

from typing import List, Optional

from .models import (
    IRREVERSIBLE_STAGES,
    PlanStep,
    RoutePlan,
    Stage,
)


# 各可逆阶段的逆向操作描述。
_INVERSE = {
    Stage.EXPAND: "撤销扩展结构:删除影子列/未生效索引",
    Stage.REWRITE: "终止表重写任务并恢复原表",
    Stage.DUAL_WRITE: "关闭双写触发器,恢复单写旧结构",
    Stage.BACKFILL: "停止回填任务并清理部分回填数据",
    Stage.VALIDATE: "取消校验任务",
    Stage.SWITCH_READ: "读取切回旧结构",
    Stage.ENFORCE: "约束回退为 NOT VALID",
}


class RollbackPlanner:
    """基于已生成的路由计划,为失败步骤构建受限范围的回滚计划。"""

    def build(self, plan, failed_step_id, affected_subscribers=None):
        # type: (RoutePlan, str, Optional[List[str]]) -> RoutePlan
        """为失败步骤生成回滚计划,只覆盖受影响链路。

        - 失败步骤不可逆:不尝试逆向,只对受影响订阅端生成重快照修复。
        - 失败步骤可逆:按相反顺序撤销已完成的可逆步骤,
          范围限定在受影响订阅端与失败步骤涉及的表。
        """
        failed = plan.find_step(failed_step_id)
        if failed is None:
            raise ValueError("未知步骤: %s" % failed_step_id)

        if affected_subscribers is None:
            affected_subscribers = list(failed.subscribers)
        scope = list(affected_subscribers)

        rollback = RoutePlan(
            change_id="%s-rollback" % plan.change_id,
            schema_version_from=plan.schema_version_to,
            schema_version_to=plan.schema_version_from,
        )
        rollback.notes.append(
            "回滚范围限定为受影响链路: %s"
            % (",".join(scope) if scope else "发布端"))

        failed_index = plan.steps.index(failed)
        irreversible_failure = (
            not failed.reversible) or failed.stage in IRREVERSIBLE_STAGES

        if irreversible_failure:
            rollback.notes.append(
                "步骤 %s 不可逆,不逆向执行,仅修复受影响链路"
                % failed.step_id)
            for name in scope:
                rollback.steps.append(PlanStep(
                    step_id="%s-rb-%02d-resnapshot"
                            % (plan.change_id, len(rollback.steps) + 1),
                    stage=Stage.RESNAPSHOT,
                    description=("订阅端 %s 链路受损:分离并重快照"
                                 % name),
                    subscribers=[name],
                    reversible=False,
                ))
            return rollback

        # 可逆失败:逆序撤销失败步骤之前已完成的可逆步骤。
        completed = plan.steps[:failed_index]
        seq = 0
        for step in reversed(completed):
            inverse = _INVERSE.get(step.stage)
            if inverse is None or not step.reversible:
                continue
            seq += 1
            rollback.steps.append(PlanStep(
                step_id="%s-rb-%02d-%s" % (plan.change_id, seq,
                                           step.stage.value),
                stage=Stage.ROLLBACK,
                description="%s(表 %s)" % (inverse,
                                           ",".join(step.tables) or "-"),
                tables=list(step.tables),
                subscribers=scope,
                preconditions=["仅作用于受影响链路,健康链路保持不变"],
            ))
        return rollback
