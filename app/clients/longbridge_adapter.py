# app/clients/longbridge_adapter.py
"""LongbridgeAdapter – placeholder / stub implementation.

目的：
- 固定接口形状（__init__ / get_agg_bars / get_quotes）
- 调用时如果真的需要 Longbridge 数据，会抛出清晰的 NotImplementedError，
  而不是静默返回空数据。

后续你确认 Longbridge 官方 SDK 或 REST API 后，可以在这些方法内补充真实实现。
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Dict, Any


@dataclass
class LBBar:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class LongbridgeAdapter:
    def __init__(self, api_key: str | None = None, **kwargs: Any) -> None:
        self.api_key = api_key
        self.extra = kwargs

    def ping(self) -> bool:
        """健康检查占位实现，目前返回 False。"""
        return False

    def get_agg_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1min",
    ) -> List[LBBar]:
        """        获取聚合 K 线数据（用于和 Futu 做横向校验）。

        当前为占位实现：直接抛出 NotImplementedError，
        以免误用「空数据」导致你误以为校验通过。
        """
        raise NotImplementedError(
            "LongbridgeAdapter.get_agg_bars is not implemented yet. "            "Please wire it to the real Longbridge API before use."
        )

    def get_quotes(self, symbols: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取当前报价，占位实现。"""
        raise NotImplementedError(
            "LongbridgeAdapter.get_quotes is not implemented yet."
        )
