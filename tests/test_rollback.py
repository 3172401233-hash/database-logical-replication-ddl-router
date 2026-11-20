"""回滚规划器测试:不可逆失败只回滚受影响链路。"""

import unittest

from ddl_router.models import (
    ChangeKind,
    ChangeRequest,
    PublisherState,
    Stage,
    SubscriberState,
    TableChange,
)
from ddl_router.planner import PhasePlanner
from ddl_router.rollback import RollbackPlanner


def make_plan():
    publisher = PublisherState(current_lsn=1000, schema_version=3)
    subscribers = [
        SubscriberState(name="report", role="report",
                        lsn=1000, schema_version=3),
        SubscriberState(name="search", role="search",
                        lsn=990, schema_version=3),
        SubscriberState(name="archive", role="archive",
                        lsn=800, schema_version=3),
    ]
    request = ChangeRequest(
        change_id="chg-rb",
        changes=[TableChange(
            table="orders",
            kind=ChangeKind.ALTER_COLUMN_TYPE,
            column="amount",
            new_type="numeric(14,2)",
        )],
    )
    return PhasePlanner(publisher, subscribers).plan(request)


class IrreversibleFailureTest(unittest.TestCase):
    def test_only_affected_chain_rolled_back(self):
        plan = make_plan()
        cleanup = next(s for s in plan.steps if s.stage is Stage.CLEANUP)

        rollback = RollbackPlanner().build(
            plan, cleanup.step_id, affected_subscribers=["search"])

        # 不可逆步骤不逆向,只对 search 链路生成重快照。
        self.assertEqual([s.stage for s in rollback.steps],
                         [Stage.RESNAPSHOT])
        self.assertEqual(rollback.steps[0].subscribers, ["search"])
        # 健康链路(report/archive)不出现在任何回滚步骤中。
        for step in rollback.steps:
            self.assertNotIn("report", step.subscribers)
            self.assertNotIn("archive", step.subscribers)

    def test_unknown_step_raises(self):
        plan = make_plan()
        with self.assertRaises(ValueError):
            RollbackPlanner().build(plan, "no-such-step")


class ReversibleFailureTest(unittest.TestCase):
    def test_inverse_steps_in_reverse_order(self):
        plan = make_plan()
        backfill = next(s for s in plan.steps if s.stage is Stage.BACKFILL)

        rollback = RollbackPlanner().build(
            plan, backfill.step_id, affected_subscribers=["archive"])

        # 已完成的双写与扩展应按相反顺序撤销。
        descriptions = [s.description for s in rollback.steps]
        self.assertEqual(len(rollback.steps), 2)
        self.assertIn("关闭双写", descriptions[0])
        self.assertIn("撤销扩展结构", descriptions[1])

        # 回滚范围限定在受影响链路。
        for step in rollback.steps:
            self.assertEqual(step.subscribers, ["archive"])
            self.assertEqual(step.stage, Stage.ROLLBACK)


if __name__ == "__main__":
    unittest.main()
