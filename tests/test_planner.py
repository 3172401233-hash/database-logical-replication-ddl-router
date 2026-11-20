"""阶段规划器测试:阶段顺序、门控条件与特殊变更类型。"""

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


def make_subscribers(lagging=()):
    subs = [
        SubscriberState(name="report", role="report",
                        lsn=1000, schema_version=3),
        SubscriberState(name="search", role="search",
                        lsn=990, schema_version=3),
        SubscriberState(name="archive", role="archive",
                        lsn=800, schema_version=3),
    ]
    for sub in subs:
        if sub.name in lagging:
            sub.lag_bytes = sub.max_lag_bytes + 1
    return subs


def make_planner(lagging=()):
    publisher = PublisherState(current_lsn=1000, schema_version=3)
    return PhasePlanner(publisher, make_subscribers(lagging))


def alter_type_request():
    return ChangeRequest(
        change_id="chg-1",
        changes=[TableChange(
            table="orders",
            kind=ChangeKind.ALTER_COLUMN_TYPE,
            column="amount",
            old_type="int",
            new_type="numeric(14,2)",
        )],
    )


class AlterTypePhaseOrderTest(unittest.TestCase):
    def test_phase_order(self):
        plan = make_planner().plan(alter_type_request())
        self.assertEqual(plan.stages(), [
            Stage.EXPAND,
            Stage.DUAL_WRITE,
            Stage.BACKFILL,
            Stage.WAIT_CATCHUP,
            Stage.SWITCH_READ,
            Stage.CLEANUP,
        ])

    def test_schema_version_bump(self):
        plan = make_planner().plan(alter_type_request())
        self.assertEqual(plan.schema_version_from, 3)
        self.assertEqual(plan.schema_version_to, 4)

    def test_dual_write_gated_on_compatible_structure(self):
        plan = make_planner().plan(alter_type_request())
        dual = next(s for s in plan.steps if s.stage is Stage.DUAL_WRITE)
        for name in ("report", "search", "archive"):
            self.assertTrue(
                any(name in pre and "schema_version >= 4" in pre
                    for pre in dual.preconditions),
                "双写必须门控在订阅端兼容结构就位之后: %s" % name)

    def test_catchup_barrier_covers_all_subscribers(self):
        plan = make_planner().plan(alter_type_request())
        barrier = next(s for s in plan.steps
                       if s.stage is Stage.WAIT_CATCHUP)
        self.assertEqual(set(barrier.subscribers),
                         {"report", "search", "archive"})
        for name in ("report", "search", "archive"):
            self.assertTrue(any(name in pre and "lsn >= 1000" in pre
                                for pre in barrier.preconditions))

    def test_cleanup_is_irreversible(self):
        plan = make_planner().plan(alter_type_request())
        cleanup = next(s for s in plan.steps if s.stage is Stage.CLEANUP)
        self.assertFalse(cleanup.reversible)

    def test_rewrite_inserted_when_required(self):
        request = alter_type_request()
        request.changes[0].requires_rewrite = True
        plan = make_planner().plan(request)
        stages = plan.stages()
        self.assertIn(Stage.REWRITE, stages)
        self.assertLess(stages.index(Stage.REWRITE),
                        stages.index(Stage.DUAL_WRITE))


class DropColumnTest(unittest.TestCase):
    def make_request(self):
        return ChangeRequest(
            change_id="chg-drop",
            changes=[TableChange(
                table="users",
                kind=ChangeKind.DROP_COLUMN,
                column="legacy_flag",
            )],
        )

    def test_switch_before_cleanup_and_no_dual_write(self):
        plan = make_planner().plan(self.make_request())
        stages = plan.stages()
        self.assertNotIn(Stage.DUAL_WRITE, stages)
        self.assertNotIn(Stage.BACKFILL, stages)
        self.assertLess(stages.index(Stage.SWITCH_READ),
                        stages.index(Stage.CLEANUP))

    def test_cleanup_waits_for_all_consumers(self):
        plan = make_planner().plan(self.make_request())
        cleanup = next(s for s in plan.steps if s.stage is Stage.CLEANUP)
        self.assertFalse(cleanup.reversible)
        self.assertTrue(any("全部订阅端" in pre
                            for pre in cleanup.preconditions))


class UniqueConstraintTest(unittest.TestCase):
    def test_validate_before_enforce(self):
        request = ChangeRequest(
            change_id="chg-uniq",
            changes=[TableChange(
                table="accounts",
                kind=ChangeKind.ADD_UNIQUE_CONSTRAINT,
                column="email",
                constraint_name="uq_accounts_email",
            )],
        )
        plan = make_planner().plan(request)
        stages = plan.stages()
        self.assertLess(stages.index(Stage.EXPAND),
                        stages.index(Stage.BACKFILL))
        self.assertLess(stages.index(Stage.BACKFILL),
                        stages.index(Stage.VALIDATE))
        self.assertLess(stages.index(Stage.VALIDATE),
                        stages.index(Stage.ENFORCE))


class MultiTableAtomicTest(unittest.TestCase):
    def test_atomic_barriers_and_grouped_phases(self):
        request = ChangeRequest(
            change_id="chg-atomic",
            atomic=True,
            changes=[
                TableChange(table="orders",
                            kind=ChangeKind.ALTER_COLUMN_TYPE,
                            column="amount", new_type="numeric(14,2)"),
                TableChange(table="payments",
                            kind=ChangeKind.ALTER_COLUMN_TYPE,
                            column="amount", new_type="numeric(14,2)"),
            ],
        )
        plan = make_planner().plan(request)
        steps = plan.steps

        expand_idx = [i for i, s in enumerate(steps)
                      if s.stage is Stage.EXPAND]
        dual_idx = [i for i, s in enumerate(steps)
                    if s.stage is Stage.DUAL_WRITE]
        self.assertEqual(len(expand_idx), 2)
        self.assertEqual(len(dual_idx), 2)
        # 所有扩展结构必须先于任何双写。
        self.assertLess(max(expand_idx), min(dual_idx))

        # 原子屏障存在且覆盖两张表。
        barriers = [s for s in steps
                    if s.stage is Stage.WAIT_CATCHUP
                    and "原子屏障" in s.description]
        self.assertTrue(barriers)
        for barrier in barriers:
            self.assertEqual(set(barrier.tables), {"orders", "payments"})


class LaggingConsumerTest(unittest.TestCase):
    def test_lagging_consumer_defers_cleanup(self):
        plan = make_planner(lagging={"archive"}).plan(alter_type_request())
        stages = plan.stages()
        self.assertIn(Stage.RESNAPSHOT, stages)
        self.assertIn(Stage.DEFER, stages)

        resnapshot = next(s for s in plan.steps
                          if s.stage is Stage.RESNAPSHOT)
        self.assertEqual(resnapshot.subscribers, ["archive"])

        cleanup = next(s for s in plan.steps if s.stage is Stage.CLEANUP)
        self.assertTrue(any("archive" in pre and "重快照" in pre
                            for pre in cleanup.preconditions))
        self.assertTrue(any("archive" in note for note in plan.notes))

    def test_no_lagging_consumer_no_defer(self):
        plan = make_planner().plan(alter_type_request())
        self.assertNotIn(Stage.RESNAPSHOT, plan.stages())
        self.assertNotIn(Stage.DEFER, plan.stages())


if __name__ == "__main__":
    unittest.main()
