"""DML 广播安全路由器。

在 DDL 追平完成之前,按订阅端的 schema 版本与 LSN 决定一批 DML
允许发往哪些订阅端,防止旧消费者按错误格式解析数据。
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from .models import DMLBatch, RoutePlan, SubscriberState


class DDLSafetyRouter:
    """按订阅端兼容结构门控 DML 广播。"""

    def __init__(self, plan):
        # type: (RoutePlan) -> None
        self.plan = plan

    def can_broadcast(self, subscriber, dml):
        # type: (SubscriberState, DMLBatch) -> Tuple[bool, str]
        """判断一批 DML 是否允许发送给指定订阅端。"""
        if not subscriber.has_compatible_structure(dml.schema_version):
            return False, (
                "订阅端 %s schema_version=%d 低于 DML 所需版本 %d:"
                "兼容结构未就位,禁止广播"
                % (subscriber.name, subscriber.schema_version,
                   dml.schema_version))
        if subscriber.is_lagging and \
                dml.schema_version >= self.plan.schema_version_to:
            return False, (
                "订阅端 %s 长期落后且 DML 属于新结构:需先重快照"
                % subscriber.name)
        return True, "允许广播"

    def route(self, dml, subscribers):
        # type: (DMLBatch, List[SubscriberState]) -> Dict[str, Tuple[bool, str]]
        """对全部订阅端给出广播裁决。"""
        return {
            sub.name: self.can_broadcast(sub, dml)
            for sub in subscribers
        }

    def allowed_subscribers(self, dml, subscribers):
        # type: (DMLBatch, List[SubscriberState]) -> List[str]
        """返回当前允许接收该批 DML 的订阅端名称。"""
        return [
            sub.name for sub in subscribers
            if self.can_broadcast(sub, dml)[0]
        ]
