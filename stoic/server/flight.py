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

    # ── Flight SQL: get_flight_info ───────────────────────────────

    def get_flight_info(self, context, descriptor):
        any_msg = any_pb2.Any()
        any_msg.ParseFromString(descriptor.command)

        if any_msg.Is(pb2.CommandStatementQuery.DESCRIPTOR):
            return self._handle_statement_query(any_msg)

        if any_msg.Is(pb2.CommandGetTables.DESCRIPTOR):
            return self._handle_get_tables(any_msg)

        if any_msg.Is(pb2.CommandGetDbSchemas.DESCRIPTOR):
            return self._handle_get_db_schemas()

        if any_msg.Is(pb2.CommandGetCatalogs.DESCRIPTOR):
            return self._handle_get_catalogs()

        raise flight.FlightUnauthenticatedError(
            f"Unsupported command: {any_msg.type_url}"
        )

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
    """Flight SQL server that queries a cached DuckDB connection.

    Args:
        location: gRPC bind address (e.g. ``grpc://0.0.0.0:8815``).
        open_connection: Callable that returns a fresh DuckDB connection
            with all desired databases ATTACHed.
        snapshot_paths: File paths to monitor for mtime changes.  When
            any file's mtime changes, the cached connection is rebuilt
            on the next query.
        memory_limit: DuckDB per-connection memory limit.
        threads: DuckDB thread count per connection.
    """

    def __init__(self, location: str, *,
                 open_connection: Callable,
                 snapshot_paths: tuple[str, ...] = (),
                 memory_limit: str = '8GB',
                 threads: int = 4,
                 **kwargs):
        super().__init__(location, **kwargs)
        self._open_connection_fn = open_connection
        self._snapshot_paths = list(snapshot_paths)
        self._memory_limit = memory_limit
        self._threads = threads
        self._conn = None
        self._conn_lock = threading.Lock()
        self._snapshot_mtimes: dict[str, float] = {}

    # ── Connection caching ────────────────────────────────────────

    def _get_connection(self):
        """Return cached connection, rebuilding only when snapshots change."""
        with self._conn_lock:
            return self._get_connection_unlocked()

    def _get_connection_unlocked(self):
        """Return cached connection.  Caller must hold ``_conn_lock``."""
        if self._conn is not None and not self._snapshots_changed():
            return self._conn
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = self._open_connection_fn()
        self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")
        self._conn.execute(f"SET threads = {self._threads}")
        self._record_mtimes()
        logger.info("connection_rebuilt",
                    snapshots=len(self._snapshot_paths))
        return self._conn

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

    # ── Query handlers ────────────────────────────────────────────

    def _handle_statement_query(self, any_msg):
        cmd = pb2.CommandStatementQuery()
        any_msg.Unpack(cmd)
        sql = cmd.query
        logger.info("execute_query", sql=sql[:200])

        # Hold _conn_lock for the entire execute→fetch cycle so that
        # concurrent queries don't invalidate each other's streams.
        with self._conn_lock:
            conn = self._get_connection_unlocked()
            result = conn.execute(sql)
            table = result.fetch_arrow_table()
        return self._stash_and_info(table)

    def _handle_get_tables(self, any_msg):
        cmd = pb2.CommandGetTables()
        any_msg.Unpack(cmd)
        include_schema = cmd.include_schema

        with self._conn_lock:
            conn = self._get_connection_unlocked()
            rows = conn.execute("SHOW ALL TABLES").fetchall()

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
        with self._conn_lock:
            conn = self._get_connection_unlocked()
            rows = conn.execute(
                "SELECT catalog_name, schema_name "
                "FROM information_schema.schemata "
                "ORDER BY catalog_name, schema_name"
            ).fetchall()

        table = pa.table({
            "catalog_name": pa.array([r[0] for r in rows], type=pa.utf8()),
            "db_schema_name": pa.array([r[1] for r in rows], type=pa.utf8()),
        })
        return self._stash_and_info(table)

    def _handle_get_catalogs(self):
        with self._conn_lock:
            conn = self._get_connection_unlocked()
            rows = conn.execute(
                "SELECT DISTINCT catalog_name "
                "FROM information_schema.schemata "
                "ORDER BY catalog_name"
            ).fetchall()

        table = pa.table({
            "catalog_name": pa.array([r[0] for r in rows], type=pa.utf8()),
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
