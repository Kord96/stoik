# Stoic — Pluggable Consumer Framework

Stoic is a minimal framework for building stream-to-storage consumer pipelines.
It provides three abstract axes — **storage**, **stream**, and **server** — each
with a thin protocol and one concrete plug. Future backends slot in by
implementing the same protocols.

## Architecture

```
stoic/
    __init__.py           # Re-exports: Buffer, Storage, Stream, Server, ReadPool, consume
    buffer.py             # Buffer base class (time/size-based batching)
    loop.py               # Generic consume() — decoupled from Kafka
    metrics.py            # MetricsHook protocol + NoopMetrics

    storage/
        _protocol.py      # Storage Protocol (release/reconnect/write/close)
        duckdb.py         # DuckDBStore — connection lifecycle + PyArrow bulk insert
        snapshot.py       # XFS reflink snapshot helpers

    stream/
        _protocol.py      # Stream + Message Protocols
        kafka.py          # KafkaStream — confluent_kafka wrapper with built-in deserialization

    server/
        _protocol.py      # Server + ReadPool Protocols (for query servers)
```

## Protocols

### Storage — persistence lifecycle

```python
class Storage(Protocol):
    db_path: str
    def release(self) -> None: ...       # Close connection between flushes
    def reconnect(self) -> None: ...     # Reopen before next flush
    def write(self, fn, description): ...  # Retry-wrapped write
    def merge_staging(self, table, sql) -> int: ...  # Merge staging table
    def staging_count(self, table) -> int: ...       # Staging row count
    def close(self) -> None: ...         # Final shutdown
```

Concrete: `DuckDBStore` — DuckDB with lock retry, PyArrow bulk insert, staging
tables, reflink snapshots.

#### Staging tables

`insert_arrow(..., staging=True)` appends to `{table}_staging` — an un-indexed
copy of the main table schema. This is orders of magnitude faster than
`INSERT...ON CONFLICT` against large indexed tables.

When the consumer detects it has caught up (buffer no longer filling to
capacity), it calls `merge_staging(table, merge_sql)` which runs the
caller-provided aggregation query and drops the staging table. The merge SQL
is domain-specific (the caller knows the GROUP BY aggregation and ON CONFLICT
expressions).

```python
# Fast path (catching up): bulk append, no index probes
store.insert_arrow('emailed_with', pa_table, staging=True)

# Transition (caught up): merge + resume normal upserts
store.merge_staging('emailed_with', MERGE_SQL)
store.insert_arrow('emailed_with', pa_table, on_conflict=ON_CONFLICT)
```

### Stream — message source

```python
class Message(Protocol):
    def topic(self) -> str: ...
    def value(self) -> Any: ...     # Pre-deserialized
    def error(self) -> Any: ...

class Stream(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, batch_size: int, timeout: float) -> list: ...
    def close(self) -> None: ...
```

Concrete: `KafkaStream` — confluent_kafka with JSON/Avro/custom deserialization
pushed into the stream plug. The generic loop receives `msg.value()` already decoded.

### Server — query server

```python
class Server(Protocol):
    def serve(self) -> None: ...
    def shutdown(self) -> None: ...

class ReadPool(Protocol):
    def get_connection(self, scope: str) -> Any: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
```

No concrete plug in stoic — project-specific servers (Flight SQL, FastAPI)
structurally satisfy these protocols.

### MetricsHook — injectable metrics

```python
class MetricsHook(Protocol):
    def inc_consumed(self, topic, n=1): ...
    def inc_failed(self, topic, n=1): ...
    def observe_flush(self, duration): ...
    def inc_flush_entities(self, n): ...
    def set_buffer_size(self, n): ...
    def set_entity_count(self, n): ...
```

`NoopMetrics` is the silent default. Pass any Prometheus-backed implementation
(e.g. `lib.metrics.Metrics`) to enable real metrics.

## Usage

### Direct (new code)

```python
from stoic import Buffer, consume
from stoic.storage import DuckDBStore
from stoic.stream import KafkaStream

stream = KafkaStream('localhost:9092', 'my-group', deserializer='json')
stream.subscribe(['my.topic'])

store = DuckDBStore('/opt/data/my.duckdb', init_fn=init_schema)
buffer = MyBuffer()

consume(
    stream=stream,
    buffer=buffer,
    store=store,
    parse_message=buffer.add,
    on_flush=lambda: flush(buffer, store),
    metrics=my_metrics,
)
```

### Via backward-compat shim (existing code)

All existing imports continue to work:

```python
from lib.consumer import Buffer, Store, consume    # unchanged
from lib.dbpool import refresh_snapshot             # unchanged
from stoic import Store                             # unchanged
from stoic.snapshot import refresh_snapshot          # unchanged
```

## Adding a new backend

1. Create `stoic/storage/postgres.py` (or `stoic/stream/rabbitmq.py`, etc.)
2. Implement the relevant protocol
3. Re-export from `stoic/storage/__init__.py`
4. Existing code is unaffected — new consumers import the new plug directly
