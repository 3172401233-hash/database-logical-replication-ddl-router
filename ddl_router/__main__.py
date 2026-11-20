"""演示入口:生成一份 DDL 路由计划并以 JSON 打印。

注意:输出仅为路由计划,不直接执行任何 SQL。

用法: python -m ddl_router
"""

from __future__ import annotations

import json

from .models import (
    ChangeKind,
    ChangeRequest,
    PublisherState,
    SubscriberState,
    TableChange,
)
from .planner import PhasePlanner


def build_demo_plan():
    publisher = PublisherState(current_lsn=982_000_000, schema_version=7)
    subscribers = [
        SubscriberState(name="report", role="report",
                        lsn=982_000_000, schema_version=7),
        SubscriberState(name="search", role="search",
                        lsn=981_999_500, schema_version=7),
        SubscriberState(name="archive", role="archive",
                        lsn=981_900_000, schema_version=7,
                        lag_bytes=128 * 1024 * 1024),
    ]
    request = ChangeRequest(
        change_id="chg-2026-0001",
        changes=[TableChange(
            table="orders",
            kind=ChangeKind.ALTER_COLUMN_TYPE,
            column="amount",
            old_type="integer",
            new_type="numeric(14,2)",
        )],
    )
    return PhasePlanner(publisher, subscribers).plan(request)


def main():
    plan = build_demo_plan()
    print(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
