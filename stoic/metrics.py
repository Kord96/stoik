"""Injectable metrics hook for the consume loop.

MetricsHook defines the Tier 1 loop metrics interface.
NoopMetrics provides a silent default when no metrics server is running.

lib.metrics.Metrics structurally satisfies MetricsHook — zero changes needed.
"""

from typing import Protocol


class MetricsHook(Protocol):
    """Tier 1 loop metrics only. Higher tiers stay in flush callbacks."""

    def inc_consumed(self, topic: str, n: int = 1) -> None: ...
    def inc_failed(self, topic: str, n: int = 1) -> None: ...
    def observe_flush(self, duration: float) -> None: ...
    def inc_flush_entities(self, n: int) -> None: ...
    def set_buffer_size(self, n: int) -> None: ...
    def set_entity_count(self, n: int) -> None: ...
    def set_latest_event_ts(self, ts: float) -> None: ...


class NoopMetrics:
    """Silent metrics implementation — all methods are no-ops."""

    def inc_consumed(self, topic, n=1): pass
    def inc_failed(self, topic, n=1): pass
    def observe_flush(self, duration): pass
    def inc_flush_entities(self, n): pass
    def set_buffer_size(self, n): pass
    def set_entity_count(self, n): pass
    def set_latest_event_ts(self, ts): pass
