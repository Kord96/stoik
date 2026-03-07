"""
Generic consume loop — decoupled from Kafka and Prometheus.

Accepts a Stream (message source), Buffer, optional Storage,
and injectable MetricsHook. Signal handling stays here (generic).
"""

import signal
import threading
import time
from typing import Callable, Optional

import structlog

from stoic.buffer import Buffer
from stoic.metrics import MetricsHook, NoopMetrics

logger = structlog.get_logger()


def _flush_with_heartbeat(consumer, on_flush):
    """Run flush in a background thread while keeping Kafka heartbeat alive.

    Pauses all assigned partitions so poll() returns no messages (no
    auto-commit advancement, no data loss).  The main thread keeps calling
    consumer.poll() which triggers librdkafka's internal heartbeat and
    rebalance processing — preventing session timeouts regardless of how
    long the flush takes.
    """
    assignment = consumer.assignment()
    if assignment:
        consumer.pause(assignment)

    exc_holder = [None]

    def _run():
        try:
            on_flush()
        except Exception as e:
            exc_holder[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    while t.is_alive():
        consumer.poll(timeout=1.0)
        t.join(timeout=0)

    if assignment:
        consumer.resume(assignment)

    if exc_holder[0] is not None:
        raise exc_holder[0]


def _do_flush(m, buffer, store, on_flush, stream=None):
    """Execute a flush with metrics instrumentation."""
    count = buffer.count
    m.set_buffer_size(count)
    if store:
        store.reconnect()
    t0 = time.monotonic()

    consumer = getattr(stream, '_consumer', None) if stream else None
    if consumer is not None:
        _flush_with_heartbeat(consumer, on_flush)
    else:
        on_flush()

    elapsed = time.monotonic() - t0
    m.observe_flush(elapsed)
    m.inc_flush_entities(count)
    buffer.mark_flushed()
    m.set_buffer_size(0)

    if elapsed > 600:
        logger.warning("flush_slow", duration_s=round(elapsed, 1),
                       entities=count, threshold_s=600)
    elif elapsed > 300:
        logger.warning("flush_slow", duration_s=round(elapsed, 1),
                       entities=count, threshold_s=300)
    if store and hasattr(store, 'get_row_count'):
        n = store.get_row_count()
        if n >= 0:
            m.set_entity_count(n)
    if store:
        store.release()


def consume(
    stream,
    buffer: Buffer,
    store,
    parse_message: Callable[[dict], None],
    on_flush: Callable[[], None],
    *,
    flush_interval: float = 30.0,
    max_batch: int = 10000,
    min_batch: int = 100,
    metrics: Optional[MetricsHook] = None,
    compact_callback: Optional[Callable[[], None]] = None,
    compact_interval: int = 3600,
) -> None:
    """Generic consume loop.

    Args:
        stream: Stream instance (subscribe must already be called).
        buffer: Buffer instance for batching.
        store: Storage instance for persistence (or None).
        parse_message: Callable(value: dict) — process deserialized message.
        on_flush: Callable() — drain buffer, persist to store.
        flush_interval: Seconds between time-based flushes.
        max_batch: Flush when buffer reaches this size.
        min_batch: Minimum items before time-based flush.
        metrics: Optional MetricsHook. Defaults to NoopMetrics.
        compact_callback: Optional callback for periodic compaction.
        compact_interval: Seconds between compaction runs (default 3600).
    """
    m = metrics or NoopMetrics()

    shutdown = threading.Event()

    if threading.current_thread() is threading.main_thread():
        def _handle_signal(sig, frame):
            logger.info("shutdown_signal", signal=sig)
            shutdown.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    # Tag store with consumer group for metrics labeling
    if store:
        store._metrics_consumer = getattr(m, 'consumer', '')

    # Release DB connection while buffering
    if store:
        store.release()

    last_compact = time.time()
    _consume_batch = min(5000, max_batch)
    _consecutive_errors = 0
    _last_error_report = 0.0

    try:
        while not shutdown.is_set():
            msgs = stream.poll(_consume_batch, timeout=1.0)

            if not msgs:
                if buffer.count > 0 and buffer.should_flush(
                    interval=flush_interval,
                    max_size=max_batch,
                    min_size=min_batch,
                ):
                    _do_flush(m, buffer, store, on_flush, stream)
                continue

            latest_ts = 0
            batch_errors = 0
            for msg in msgs:
                if msg.error():
                    m.inc_failed(msg.topic() or 'unknown')
                    continue

                try:
                    value = msg.value()
                    if value:
                        parse_message(value)
                        m.inc_consumed(msg.topic())
                        _consecutive_errors = 0
                        # Track latest Kafka message timestamp
                        ts_type, ts_ms = msg.timestamp()
                        if ts_ms and ts_ms > latest_ts:
                            latest_ts = ts_ms
                except Exception:
                    logger.exception("message_processing_error",
                                     topic=msg.topic())
                    m.inc_failed(msg.topic())
                    batch_errors += 1
                    _consecutive_errors += 1
                    continue

            if batch_errors > 0:
                now = time.monotonic()
                if now - _last_error_report > 60:
                    logger.warning("message_errors_in_batch",
                                   batch_errors=batch_errors,
                                   consecutive_errors=_consecutive_errors)
                    _last_error_report = now

            if latest_ts > 0:
                m.set_latest_event_ts(latest_ts / 1000.0)

            if buffer.should_flush(interval=flush_interval, max_size=max_batch):
                _do_flush(m, buffer, store, on_flush, stream)

            # Periodic compaction
            if compact_callback and time.time() - last_compact >= compact_interval:
                if store:
                    store.reconnect()
                compact_callback()
                last_compact = time.time()
                if store:
                    store.release()

    finally:
        if buffer.count > 0:
            if store:
                store.reconnect()
            t0 = time.monotonic()
            on_flush()
            m.observe_flush(time.monotonic() - t0)

        stream.close()
        if store:
            store.close()
        logger.info("consumer_stopped")
