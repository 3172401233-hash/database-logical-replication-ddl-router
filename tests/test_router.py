"""DML 广播安全路由器测试:先具备兼容结构再接收 DML。"""

import unittest

from ddl_router.models import (
    ChangeKind,
    ChangeRequest,
    DMLBatch,
    PublisherState,
    SubscriberState,
    TableChange,
)
from ddl_router.planner import PhasePlanner
from ddl_router.router import DDLSafetyRouter


def make_plan():
    publisher = PublisherState(current_lsn=1000, schema_version=3)
    subscribers = [
        SubscriberState(name="report", role="report",
                        lsn=1000, schema_version=3),
        SubscriberState(name="search", role="search",
                        lsn=990, schema_version=3),
    ]
    request = ChangeRequest(
        change_id="chg-rt",
        changes=[TableChange(
            table="orders",
            kind=ChangeKind.ALTER_COLUMN_TYPE,
            column="amount",
            new_type="numeric(14,2)",
        )],
    )
    plan = PhasePlanner(publisher, subscribers).plan(request)
    return plan, subscribers


class BroadcastGatingTest(unittest.TestCase):
    def test_old_consumer_rejected_before_expand(self):
        plan, subscribers = make_plan()
        router = DDLSafetyRouter(plan)
        dml = DMLBatch(schema_version=4, lsn=1001, tables=["orders"])

        # 订阅端仍是 schema_version=3,未具备兼容结构。
        for sub in subscribers:
            allowed, reason = router.can_broadcast(sub, dml)
            self.assertFalse(allowed)
            self.assertIn("兼容结构未就位", reason)

    def test_consumer_allowed_after_expand_applied(self):
        plan, subscribers = make_plan()
        router = DDLSafetyRouter(plan)
        dml = DMLBatch(schema_version=4, lsn=1001, tables=["orders"])

        for sub in subscribers:
            sub.schema_version = 4  # 模拟 EXPAND 已在订阅端应用
        self.assertEqual(
            set(router.allowed_subscribers(dml, subscribers)),
            {"report", "search"})

    def test_old_version_dml_still_flows_during_migration(self):
        plan, subscribers = make_plan()
        router = DDLSafetyRouter(plan)
        old_dml = DMLBatch(schema_version=3, lsn=999, tables=["orders"])
        self.assertEqual(
            set(router.allowed_subscribers(old_dml, subscribers)),
            {"report", "search"})

    def test_lagging_consumer_blocked_from_new_format(self):
        plan, subscribers = make_plan()
        router = DDLSafetyRouter(plan)
        dml = DMLBatch(schema_version=4, lsn=1001, tables=["orders"])

        for sub in subscribers:
            sub.schema_version = 4
        lagging = subscribers[1]
        lagging.lag_bytes = lagging.max_lag_bytes + 1

        allowed, reason = router.can_broadcast(lagging, dml)
        self.assertFalse(allowed)
        self.assertIn("重快照", reason)

        decisions = router.route(dml, subscribers)
        self.assertTrue(decisions["report"][0])
        self.assertFalse(decisions["search"][0])


if __name__ == "__main__":
    unittest.main()
