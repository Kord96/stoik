# Stoic — Stream-to-Store Pipeline Framework

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

Each consumer is a single instance of this pipeline, one per DuckDB file (single-writer constraint). graphdb runs 27 of these in parallel.

## Core Abstractions (Protocol-based)

- `Buffer` — Time/size-based batching. Subclass: implement `add()` + `drain()`
- `Store` (DuckDB) — Connection lifecycle with lock retry: `reconnect()`, `insert_arrow()`, `merge_staging()`, `release()`, `close()`
- `consume()` — Generic loop: poll → parse → buffer → flush. Handles signals, heartbeat-during-flush, compaction
- `MetricsHook` — Injectable Prometheus metrics
- `Server` — REST API + FlightSQL query interface over DuckDB snapshots

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

## Installation

```bash
pip install -e frameworks/stoic
```

## Used by

- **graphdb** (office/prod) — 27 consumers, each a stoic consume loop writing to DuckDB
