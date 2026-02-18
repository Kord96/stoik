"""
Generic buffer for accumulating records before flushing to storage.

Provides time/size-based batching with configurable thresholds.
"""

import time


class Buffer:
    """Generic buffer for accumulating records.

    Subclasses add an ``add()`` method that populates internal data structures.
    Call ``should_flush()`` to check if it's time to flush, then ``drain()``
    to retrieve and reset the buffer.
    """

    def __init__(self):
        self._last_flush = time.time()
        self._count = 0

    def should_flush(
        self,
        interval: float = 30.0,
        max_size: int = 10000,
        min_size: int = 0,
    ) -> bool:
        """Check if buffer should be flushed based on count or elapsed time.

        Args:
            interval: Seconds between time-based flushes.
            max_size: Flush immediately when count reaches this threshold.
            min_size: Minimum items before a time-based flush triggers.
                If set and the buffer is smaller, the flush is deferred
                unless 3x the interval has passed (hard deadline).

        Returns:
            True if buffer should be flushed.
        """
        if self._count >= max_size:
            return True
        elapsed = time.time() - self._last_flush
        if elapsed >= interval:
            if min_size and self._count < min_size:
                return elapsed >= interval * 3
            return True
        return False

    def mark_flushed(self) -> None:
        """Reset flush timer after a successful flush."""
        self._last_flush = time.time()

    @property
    def count(self) -> int:
        """Number of records in the buffer."""
        return self._count
