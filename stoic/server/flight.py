"""Generic Arrow Flight SQL server over DuckDB.

Provides a gRPC-based SQL interface using the Flight SQL protocol.
Clients (ADBC, JDBC, ODBC) connect natively; DuckDB returns Arrow
record batches with zero-copy; large results stream without buffering.

The server maintains a cached DuckDB connection, rebuilding it only
when snapshot file mtimes change.  This avoids per-query ATTACH
overhead while keeping data fresh.
"""

import os
import threading
import time
import uuid
from typing import Callable

import pyarrow as pa
import pyarrow.flight as flight
import structlog
from google.protobuf import any_pb2
from flightsql import flightsql_pb2 as pb2

logger = structlog.get_logger()

# Stash entry TTL in seconds.
_STASH_TTL = 60

# Default query cache TTL in seconds.
_QUERY_CACHE_TTL = 300  # 5 min


class _FlightSQLBase(flight.FlightServerBase):
    """Base class providing stash management and Flight SQL request routing.

    Subclasses must implement:
        _handle_statement_query(any_msg) -> FlightInfo
        _handle_get_tables(any_msg) -> FlightInfo
        _handle_get_db_schemas() -> FlightInfo
        _handle_get_catalogs() -> FlightInfo
    """

    def __init__(self, location: str, **kwargs):
        super().__init__(location, **kwargs)
        self._stash: dict[bytes, tuple[pa.Schema, pa.Table, float]] = {}
        self._stash_lock = threading.Lock()
        self._prepared_stmts: dict[bytes, str] = {}
        self._prepared_lock = threading.Lock()

    # ── Flight SQL: get_flight_info ───────────────────────────────

    def get_flight_info(self, context, descriptor):
        any_msg = any_pb2.Any()
        any_msg.ParseFromString(descriptor.command)

        if any_msg.Is(pb2.CommandStatementQuery.DESCRIPTOR):
            return self._handle_statement_query(any_msg)

        if any_msg.Is(pb2.CommandPreparedStatementQuery.DESCRIPTOR):
            return self._handle_prepared_statement_query(any_msg)

        if any_msg.Is(pb2.CommandGetTables.DESCRIPTOR):
            return self._handle_get_tables(any_msg)

        if any_msg.Is(pb2.CommandGetDbSchemas.DESCRIPTOR):
            return self._handle_get_db_schemas()

        if any_msg.Is(pb2.CommandGetCatalogs.DESCRIPTOR):
            return self._handle_get_catalogs()

        if any_msg.Is(pb2.CommandGetSqlInfo.DESCRIPTOR):
            return self._handle_get_sql_info(any_msg)

        raise flight.FlightUnauthenticatedError(
            f"Unsupported command: {any_msg.type_url}"
        )

    # ── Flight SQL: do_action (prepared statements) ────────────────

    def do_action(self, context, action):
        if action.type == "CreatePreparedStatement":
            body = action.body.to_pybytes()
            # Some clients (e.g. ADBC, Grafana FlightSQL plugin) wrap the
            # body in google.protobuf.Any; unwrap if needed and remember
            # so we can wrap the response the same way.
            any_msg = any_pb2.Any()
            wrapped_in_any = False
            try:
                any_msg.ParseFromString(body)
            except Exception:
                any_msg = None
            if (any_msg is not None
                    and any_msg.type_url
                    and any_msg.Is(pb2.ActionCreatePreparedStatementRequest.DESCRIPTOR)):
                cmd = pb2.ActionCreatePreparedStatementRequest()
                any_msg.Unpack(cmd)
                wrapped_in_any = True
            else:
                cmd = pb2.ActionCreatePreparedStatementRequest()
                cmd.ParseFromString(body)
            sql = cmd.query
            handle = uuid.uuid4().bytes
            logger.info("create_prepared_statement", sql=sql[:200])
            with self._prepared_lock:
                self._prepared_stmts[handle] = sql
            result = pb2.ActionCreatePreparedStatementResult(
                prepared_statement_handle=handle,
            )
            # Clients that send Any-wrapped requests (e.g. ADBC Go driver)
            # expect the response wrapped in Any too.  Sending raw bytes
            # causes the Go protobuf parser to misinterpret the handle
            # bytes as a UTF-8 string field, breaking the connection.
            if wrapped_in_any:
                wrapper = any_pb2.Any()
                wrapper.Pack(result)
                yield flight.Result(wrapper.SerializeToString())
            else:
                yield flight.Result(result.SerializeToString())
            return

        if action.type == "ClosePreparedStatement":
            body = action.body.to_pybytes()
            # Unwrap Any if needed (same pattern as CreatePreparedStatement).
            any_msg = any_pb2.Any()
            wrapped_in_any = False
            try:
                any_msg.ParseFromString(body)
            except Exception:
                any_msg = None
            if (any_msg is not None
                    and any_msg.type_url
                    and any_msg.Is(pb2.ActionClosePreparedStatementRequest.DESCRIPTOR)):
                cmd = pb2.ActionClosePreparedStatementRequest()
                any_msg.Unpack(cmd)
                wrapped_in_any = True
            else:
                cmd = pb2.ActionClosePreparedStatementRequest()
                cmd.ParseFromString(body)
            with self._prepared_lock:
                self._prepared_stmts.pop(cmd.prepared_statement_handle, None)
            # ClosePreparedStatement has no structured result; respond
            # with empty bytes regardless of wrapping style.
            yield flight.Result(b"")
            return

        raise flight.FlightUnauthenticatedError(
            f"Unsupported action: {action.type}"
        )

    def _handle_prepared_statement_query(self, any_msg):
        """Execute a previously prepared statement."""
        cmd = pb2.CommandPreparedStatementQuery()
        any_msg.Unpack(cmd)
        handle = cmd.prepared_statement_handle
        with self._prepared_lock:
            sql = self._prepared_stmts.get(handle)
        if sql is None:
            raise flight.FlightServerError("Unknown prepared statement handle")
        # Delegate to statement query with the stored SQL
        logger.info("execute_prepared_query", sql=sql[:200])
        wrapped = any_pb2.Any()
        wrapped.Pack(pb2.CommandStatementQuery(query=sql))
        return self._handle_statement_query(wrapped)

    # ── Flight SQL: do_get ────────────────────────────────────────

    def do_get(self, context, ticket):
        ticket_msg = pb2.TicketStatementQuery()
        ticket_msg.ParseFromString(ticket.ticket)
        handle = ticket_msg.statement_handle

        with self._stash_lock:
            entry = self._stash.pop(handle, None)

        if entry is None:
            raise flight.FlightServerError("Unknown or expired statement handle")

        _schema, data, _ts = entry
        # Works for both pa.Table and pa.RecordBatchReader
        return flight.RecordBatchStream(data)

    # ── Stash helpers ─────────────────────────────────────────────

    def _stash_and_info(self, table: pa.Table) -> flight.FlightInfo:
        handle = uuid.uuid4().bytes
        with self._stash_lock:
            self._stash[handle] = (table.schema, table, time.monotonic())
            self._evict_expired()
        ticket_msg = pb2.TicketStatementQuery(statement_handle=handle)
        ticket = flight.Ticket(ticket_msg.SerializeToString())
        endpoint = flight.FlightEndpoint(ticket, [])
        return flight.FlightInfo(
            table.schema,
            descriptor=flight.FlightDescriptor.for_command(b""),
            endpoints=[endpoint],
            total_records=table.num_rows,
            total_bytes=table.nbytes,
        )

    def _stash_reader_and_info(self, reader: pa.RecordBatchReader) -> flight.FlightInfo:
        """Stash a streaming RecordBatchReader for later do_get."""
        handle = uuid.uuid4().bytes
        schema = reader.schema
        with self._stash_lock:
            self._stash[handle] = (schema, reader, time.monotonic())
            self._evict_expired()
        ticket_msg = pb2.TicketStatementQuery(statement_handle=handle)
        ticket = flight.Ticket(ticket_msg.SerializeToString())
        endpoint = flight.FlightEndpoint(ticket, [])
        return flight.FlightInfo(
            schema,
            descriptor=flight.FlightDescriptor.for_command(b""),
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    # ── Stash maintenance ─────────────────────────────────────────

    def _evict_expired(self):
        """Remove stash entries older than TTL. Must hold self._stash_lock."""
        now = time.monotonic()
        expired = [k for k, (_, _, ts) in self._stash.items()
                   if now - ts > _STASH_TTL]
        for k in expired:
            del self._stash[k]


class FlightSQLServer(_FlightSQLBase):
    """Flight SQL server backed by a pool of cached DuckDB connections.

    Queries are distributed round-robin across ``pool_size`` independent
    DuckDB connections, each protected by its own lock.  This allows
    concurrent panel queries (e.g. from Grafana) to execute in parallel
    instead of serializing through a single connection.

    Args:
        location: gRPC bind address (e.g. ``grpc://0.0.0.0:8815``).
        open_connection: Callable that returns a fresh DuckDB connection
            with all desired databases ATTACHed.
        snapshot_paths: File paths to monitor for mtime changes.  When
            any file's mtime changes, pool connections are rebuilt.
        memory_limit: DuckDB per-connection memory limit.
        threads: DuckDB thread count per connection.
        pool_size: Number of DuckDB connections in the pool.
    """

    def __init__(self, location: str, *,
                 open_connection: Callable,
                 snapshot_paths: tuple[str, ...] = (),
                 memory_limit: str = '8GB',
                 threads: int = 4,
                 pool_size: int = 4,
                 **kwargs):
        super().__init__(location, **kwargs)
        self._pool_size = max(1, pool_size)
        self._open_connection_fn = open_connection
        self._snapshot_paths = list(snapshot_paths)
        self._memory_limit = memory_limit
        self._threads = threads

        # Connection pool: each slot has its own connection + lock so
        # concurrent queries on different slots run in parallel.
        self._pool: list = [None] * self._pool_size
        self._pool_locks = [threading.Lock() for _ in range(self._pool_size)]
        self._pool_counter = 0
        self._pool_counter_lock = threading.Lock()

        self._snapshot_mtimes: dict[str, float] = {}
        self._mtime_check_interval: float = 60.0  # seconds between mtime checks

        # Query result cache: sql -> (pa.Table, monotonic_timestamp)
        self._query_cache: dict[str, tuple[pa.Table, float]] = {}
        self._query_cache_lock = threading.Lock()
        self._query_cache_ttl = _QUERY_CACHE_TTL

        # Background thread: check for snapshot changes and rebuild
        # proactively so queries never hit the cold-rebuild penalty.
        self._refresh_stop = threading.Event()
        self._refresh_thread = threading.Thread(
            target=self._background_refresh, daemon=True,
            name="flight-snapshot-refresh")
        self._refresh_thread.start()

    # ── Connection pool ────────────────────────────────────────────

    def _next_pool_idx(self) -> int:
        """Return the next pool index (round-robin)."""
        with self._pool_counter_lock:
            idx = self._pool_counter % self._pool_size
            self._pool_counter += 1
            return idx

    def _build_connection(self):
        """Create a new DuckDB connection with memory/thread settings."""
        conn = self._open_connection_fn()
        conn.execute(f"SET memory_limit = '{self._memory_limit}'")
        conn.execute(f"SET threads = {self._threads}")
        return conn

    def _execute_on_pool(self, fn):
        """Acquire a pool connection, run ``fn(conn)``, release.

        ``fn`` receives a DuckDB connection and must return its result
        before the lock is released (e.g. fetch all rows).
        """
        idx = self._next_pool_idx()
        with self._pool_locks[idx]:
            if self._pool[idx] is None:
                self._pool[idx] = self._build_connection()
                logger.info("pool_connection_created", pool_idx=idx)
            return fn(self._pool[idx])

    def _snapshots_changed(self) -> bool:
        for path in self._snapshot_paths:
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if self._snapshot_mtimes.get(path) != mtime:
                return True
        return False

    def _record_mtimes(self):
        for path in self._snapshot_paths:
            try:
                self._snapshot_mtimes[path] = os.path.getmtime(path)
            except OSError:
                pass

    def _background_refresh(self):
        """Periodically check snapshots and rebuild pool connections.

        Builds all connections immediately on startup, then checks for
        mtime changes every ``_mtime_check_interval`` seconds.  Each
        pool slot is rebuilt individually so other slots remain available
        for queries during the rebuild.
        """
        first = True
        while not self._refresh_stop.is_set():
            if not first:
                self._refresh_stop.wait(self._mtime_check_interval)
                if self._refresh_stop.is_set():
                    break
            first = False
            try:
                # Skip rebuild if snapshots haven't changed and pool is populated
                if (not self._snapshots_changed()
                        and all(c is not None for c in self._pool)):
                    continue
                # Rebuild each pool slot one at a time
                for idx in range(self._pool_size):
                    try:
                        new_conn = self._build_connection()
                    except Exception:
                        logger.warning("bg_refresh_connection_failed",
                                       pool_idx=idx, exc_info=True)
                        continue
                    with self._pool_locks[idx]:
                        old = self._pool[idx]
                        self._pool[idx] = new_conn
                    if old is not None:
                        try:
                            old.close()
                        except Exception:
                            pass
                self._record_mtimes()
                # Clear query cache — data has changed
                with self._query_cache_lock:
                    self._query_cache.clear()
                logger.info("pool_refreshed",
                            pool_size=self._pool_size,
                            snapshots=len(self._snapshot_paths))
            except Exception:
                logger.warning("bg_refresh_failed", exc_info=True)

    # ── Query handlers ────────────────────────────────────────────

    def _handle_statement_query(self, any_msg):
        cmd = pb2.CommandStatementQuery()
        any_msg.Unpack(cmd)
        sql = cmd.query.strip()
        if not sql:
            # Some clients (e.g. Grafana FlightSQL plugin) send empty SQL
            # as an initialization probe.  Return an empty table.
            table = pa.table({'ok': pa.array([1], type=pa.int32())})
            return self._stash_and_info(table)

        # Check query cache
        now = time.monotonic()
        with self._query_cache_lock:
            cached = self._query_cache.get(sql)
            if cached is not None:
                table, ts = cached
                if now - ts < self._query_cache_ttl:
                    logger.info("cache_hit", sql=sql[:200],
                                age_s=round(now - ts, 1))
                    return self._stash_and_info(table)
                # Expired — remove
                del self._query_cache[sql]

        logger.info("execute_query", sql=sql[:200])

        def run(conn):
            result = conn.execute(sql)
            return result.fetch_arrow_table()

        table = self._execute_on_pool(run)

        # Cache the result
        with self._query_cache_lock:
            self._query_cache[sql] = (table, time.monotonic())
            # Evict entries older than TTL
            expired = [k for k, (_, ts) in self._query_cache.items()
                       if now - ts >= self._query_cache_ttl]
            for k in expired:
                del self._query_cache[k]

        return self._stash_and_info(table)

    def _handle_get_tables(self, any_msg):
        cmd = pb2.CommandGetTables()
        any_msg.Unpack(cmd)
        include_schema = cmd.include_schema

        rows = self._execute_on_pool(
            lambda conn: conn.execute("SHOW ALL TABLES").fetchall())

        catalogs, schemas, names, types = [], [], [], []
        schema_bytes_list = []
        for row in rows:
            catalogs.append(row[0])
            schemas.append(row[1])
            names.append(row[2])
            types.append("TABLE")
            if include_schema:
                col_names = row[3]
                col_types = row[4]
                fields = []
                for cname, ctype in zip(col_names, col_types):
                    fields.append(pa.field(cname, _duckdb_type_to_arrow(ctype)))
                tbl_schema = pa.schema(fields)
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, tbl_schema)
                writer.close()
                schema_bytes_list.append(sink.getvalue().to_pybytes())

        arrays = [
            pa.array(catalogs, type=pa.utf8()),
            pa.array(schemas, type=pa.utf8()),
            pa.array(names, type=pa.utf8()),
            pa.array(types, type=pa.utf8()),
        ]
        fields = [
            pa.field("catalog_name", pa.utf8()),
            pa.field("db_schema_name", pa.utf8()),
            pa.field("table_name", pa.utf8()),
            pa.field("table_type", pa.utf8()),
        ]
        if include_schema:
            arrays.append(pa.array(schema_bytes_list, type=pa.binary()))
            fields.append(pa.field("table_schema", pa.binary()))

        table = pa.table(arrays, schema=pa.schema(fields))
        return self._stash_and_info(table)

    def _handle_get_db_schemas(self):
        rows = self._execute_on_pool(
            lambda conn: conn.execute(
                "SELECT catalog_name, schema_name "
                "FROM information_schema.schemata "
                "ORDER BY catalog_name, schema_name"
            ).fetchall())

        table = pa.table({
            "catalog_name": pa.array([r[0] for r in rows], type=pa.utf8()),
            "db_schema_name": pa.array([r[1] for r in rows], type=pa.utf8()),
        })
        return self._stash_and_info(table)

    def _handle_get_catalogs(self):
        rows = self._execute_on_pool(
            lambda conn: conn.execute(
                "SELECT DISTINCT catalog_name "
                "FROM information_schema.schemata "
                "ORDER BY catalog_name"
            ).fetchall())

        table = pa.table({
            "catalog_name": pa.array([r[0] for r in rows], type=pa.utf8()),
        })
        return self._stash_and_info(table)

    def _handle_get_sql_info(self, any_msg):
        """Return minimal SQL info metadata for ADBC client initialization."""
        # The SqlInfo response is a dense union table.  Construct an
        # empty table with the correct schema to satisfy the ADBC client.
        children = [
            pa.array([], type=pa.utf8()),
            pa.array([], type=pa.bool_()),
            pa.array([], type=pa.int64()),
            pa.array([], type=pa.int32()),
            pa.array([], type=pa.list_(pa.utf8())),
            pa.array([], type=pa.map_(pa.int32(), pa.list_(pa.int32()))),
        ]
        type_ids = pa.array([], type=pa.int8())
        offsets = pa.array([], type=pa.int32())
        union_arr = pa.UnionArray.from_dense(
            type_ids, offsets, children,
            field_names=["string_value", "bool_value", "bigint_value",
                         "int32_bitmask", "string_list",
                         "int32_to_int32_list_map"],
        )
        table = pa.table({
            "info_name": pa.array([], type=pa.uint32()),
            "value": union_arr,
        })
        return self._stash_and_info(table)


def _duckdb_type_to_arrow(type_str: str) -> pa.DataType:
    """Best-effort mapping from DuckDB type strings to Arrow types."""
    t = type_str.upper()
    if t in ('BIGINT', 'INT64'):
        return pa.int64()
    if t in ('INTEGER', 'INT32', 'INT'):
        return pa.int32()
    if t in ('SMALLINT', 'INT16'):
        return pa.int16()
    if t in ('TINYINT', 'INT8'):
        return pa.int8()
    if t in ('UBIGINT', 'UINT64'):
        return pa.uint64()
    if t in ('UINTEGER', 'UINT32'):
        return pa.uint32()
    if t in ('USMALLINT', 'UINT16'):
        return pa.uint16()
    if t in ('UTINYINT', 'UINT8'):
        return pa.uint8()
    if t in ('DOUBLE', 'FLOAT8'):
        return pa.float64()
    if t in ('FLOAT', 'REAL', 'FLOAT4'):
        return pa.float32()
    if t in ('BOOLEAN', 'BOOL'):
        return pa.bool_()
    if t in ('VARCHAR', 'TEXT', 'STRING'):
        return pa.utf8()
    if t == 'BLOB':
        return pa.binary()
    if t == 'DATE':
        return pa.date32()
    if t in ('TIMESTAMP', 'DATETIME'):
        return pa.timestamp('us')
    if t == 'TIMESTAMP WITH TIME ZONE':
        return pa.timestamp('us', tz='UTC')
    if t == 'UUID':
        return pa.utf8()
    if t == 'HUGEINT':
        return pa.large_utf8()
    return pa.utf8()
