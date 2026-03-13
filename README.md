# Stoik — Stream-to-Store Pipeline Framework

Data flow framework: Kafka → buffer → batch flush → DuckDB.

## Architecture

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Kafka   │───→│ consume()│───→│  Buffer  │───→│  Store   │
│  topic   │    │  parse() │    │ add/drain│    │  DuckDB  │
└──────────┘    └──────────┘    └──────────┘    └────┬─────┘
                                                     │
                                              ┌──────┴─────┐
                                              │  Snapshot   │
                                              │(XFS reflink)│
                                              └──────┬─────┘
                                                     │
                                              ┌──────┴─────┐
                                              │   Server   │
                                              │REST+Flight │
                                              └────────────┘
```

Each consumer is a single instance of this pipeline, one per DuckDB file (single-writer constraint).

## Installation

```bash
pip install stoik-pipeline
```

With FlightSQL support:
```bash
pip install stoik-pipeline[flight]
```

## Core Abstractions (Protocol-based)

- `Buffer` — Time/size-based batching. Subclass: implement `add()` + `drain()`
- `Store` (DuckDB) — Connection lifecycle with lock retry: `reconnect()`, `insert_arrow()`, `merge_staging()`, `release()`, `close()`
- `consume()` — Generic loop: poll → parse → buffer → flush. Handles signals, heartbeat-during-flush, compaction
- `MetricsHook` — Injectable Prometheus metrics
- `Server` — REST API + FlightSQL query interface over DuckDB snapshots

## Quick Start

```python
from stoik import Buffer, Store, consume

class MyBuffer(Buffer):
    def add(self, record):
        self._batch.append(record)

    def drain(self):
        batch = self._batch
        self._batch = []
        return batch

def on_flush(store, records):
    table = pa.Table.from_pylist(records)
    store.reconnect()
    store.insert_arrow(table)
    store.release()

consume(
    topic="my-topic",
    buffer=MyBuffer(max_size=1000, max_seconds=30),
    store=Store("/data/my.duckdb"),
    on_flush=on_flush,
)
```

## Flush Cycle

```
consume() loop:
  │
  ├─→ poll Kafka
  ├─→ parse_message(msg)
  ├─→ Buffer.add(parsed)
  │
  └─→ [timer/size trigger]
        ├─→ on_flush()
        ├─→ Store.reconnect()
        ├─→ Store.insert_arrow(pa_table)
        ├─→ Store.release()
        └─→ snapshot (XFS reflink)
```

## Anti-patterns

- Don't hold DuckDB connection open between flushes — `release()` after each flush
- Don't write per-message — buffer and batch via PyArrow tables
- Don't skip staging tables for large merges — use `insert_arrow(staging=True)` + `merge_staging()`

## Development

Branches follow a promotion chain: `dev` → `test` → `main` (prod).

Merging to `main` auto-publishes to PyPI.

## License

MIT
