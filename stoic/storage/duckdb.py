"""
Generic DuckDB persistence with connection lifecycle management.

Handles connection open/close with retry on lock contention,
release/reconnect pattern for shared database files, and
PyArrow-based bulk insert helpers.
"""

import random
import time
from typing import Callable, Optional

import duckdb
import pyarrow as pa
import structlog

logger = structlog.get_logger()

# Retry configuration
MAX_RETRIES = 10
BASE_DELAY = 0.1   # 100 ms
MAX_DELAY = 5.0    # 5 s cap
JITTER = 0.5       # +/- 50%


class Store:
    """Generic DuckDB persistence with connection lifecycle management.

    Handles connection open/close with retry on lock contention.
    Use release() to close the connection between flush cycles,
    and reconnect() to reopen before the next operation.
    """

    def __init__(
        self,
        db_path: str,
        init_fn: Optional[Callable[[str], duckdb.DuckDBPyConnection]] = None,
        snapshot: bool = True,
    ):
        """Initialize the store.

        Args:
            db_path: Path to DuckDB database file.
            init_fn: Optional function to initialize the database.
                Called with db_path, should return a connection.
                If None, uses duckdb.connect() directly.
            snapshot: Whether to refresh snapshots on close/release.
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._snapshot = snapshot
        self._staging_created: set[str] = set()

        if init_fn is None:
            init_fn = lambda p: duckdb.connect(p)

        self.conn = self._connect_with_retry(init_fn, db_path)
        logger.info("store_initialized", db_path=db_path)

    @staticmethod
    def _connect_with_retry(
        init_fn: Callable[[str], duckdb.DuckDBPyConnection],
        db_path: str,
        max_retries: int = MAX_RETRIES,
        base_delay: float = BASE_DELAY,
        max_delay: float = MAX_DELAY,
    ) -> duckdb.DuckDBPyConnection:
        """Open DuckDB connection with retry on lock contention.

        When multiple processes share a database file, the connect or
        DDL inside init_fn may fail if another process holds the write
        lock. Retries with exponential backoff until the lock clears.
        """
        for attempt in range(1, max_retries + 1):
            try:
                return init_fn(db_path)
            except duckdb.IOException as exc:
                if 'lock' not in str(exc).lower():
                    raise
                if attempt == max_retries:
                    logger.error("db_lock_exhausted",
                                db_path=db_path, attempts=max_retries)
                    raise
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                jitter = delay * JITTER * (2 * random.random() - 1)
                sleep = delay + jitter
                logger.warning("db_lock_contention",
                               db_path=db_path,
                               attempt=attempt, max_retries=max_retries,
                               retry_delay=round(sleep, 3))
                time.sleep(sleep)

    def release(self) -> None:
        """Close DuckDB connection, releasing the file lock.

        Call after each flush cycle so other processes sharing the
        database file can acquire the lock. Use reconnect() to
        reopen before the next operation.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
        if self._snapshot:
            from stoic.storage.snapshot import refresh_snapshot
            refresh_snapshot(self.db_path)

    def reconnect(self) -> None:
        """Reopen DuckDB connection with retry on lock contention.

        Lightweight reconnect - just opens the file without re-running
        init DDL (tables already exist from __init__).
        """
        if self.conn is not None:
            return

        self.conn = self._connect_with_retry(
            lambda p: duckdb.connect(p),
            self.db_path,
            max_retries=60,
            base_delay=0.05,
            max_delay=2.0,
        )

    def write(self, fn: Callable, description: str = 'write'):
        """Execute a write operation with retry on DuckDB contention.

        Args:
            fn: Callable that performs SQL writes on self.conn and
                calls self.conn.commit(). Will be retried if
                DuckDB raises an IOException containing "lock" or
                a TransactionException (write-write conflict).
            description: Label for log messages on retry.

        Returns:
            Whatever fn returns.

        Raises:
            Exception: If all retries are exhausted.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return fn()
            except (duckdb.IOException, duckdb.TransactionException) as exc:
                exc_str = str(exc).lower()
                is_retryable = 'lock' in exc_str or 'conflict' in exc_str
                if not is_retryable:
                    raise
                if attempt == MAX_RETRIES:
                    logger.error("write_retry_exhausted",
                                op=description, attempts=MAX_RETRIES,
                                error=str(exc))
                    raise
                delay = min(BASE_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
                jitter = delay * JITTER * (2 * random.random() - 1)
                sleep = delay + jitter
                logger.debug("write_contention",
                             op=description,
                             attempt=attempt, max_retries=MAX_RETRIES,
                             retry_delay=round(sleep, 3))
                time.sleep(sleep)

    def insert_arrow(
        self,
        table_name: str,
        pa_table: pa.Table,
        on_conflict: Optional[str] = None,
        use_insert_or_replace: bool = False,
        staging: bool = False,
    ) -> None:
        """Bulk insert using PyArrow table.

        Args:
            table_name: Target table name.
            pa_table: PyArrow table with data to insert.
            on_conflict: Optional ON CONFLICT clause (e.g., "DO NOTHING"
                or "DO UPDATE SET col = EXCLUDED.col").
            use_insert_or_replace: If True, use INSERT OR REPLACE instead
                of INSERT ... ON CONFLICT.
            staging: If True, append to {table_name}_staging instead.
                The staging table has identical columns but no primary
                key, so inserts are fast bulk appends.  Call
                merge_staging() later to fold into the main table.
        """
        if pa_table.num_rows == 0:
            return

        if staging:
            self._insert_staging(table_name, pa_table)
            return

        temp_name = f'__insert_{table_name}'

        if use_insert_or_replace:
            insert_stmt = f'INSERT OR REPLACE INTO {table_name}'
        else:
            conflict_clause = f'ON CONFLICT {on_conflict}' if on_conflict else ''
            insert_stmt = f'INSERT INTO {table_name}'

        def _do():
            self.conn.register(temp_name, pa_table)
            try:
                cols = ', '.join(pa_table.column_names)
                if use_insert_or_replace:
                    self.conn.execute(f"""
                        {insert_stmt} ({cols})
                        SELECT {cols} FROM {temp_name}
                    """)
                else:
                    conflict_clause = f'ON CONFLICT {on_conflict}' if on_conflict else ''
                    self.conn.execute(f"""
                        INSERT INTO {table_name} ({cols})
                        SELECT {cols} FROM {temp_name}
                        {conflict_clause}
                    """)
                self.conn.commit()
            finally:
                self.conn.unregister(temp_name)

        self.write(_do, f'insert_{table_name}')

    # ------------------------------------------------------------------
    # Staging table helpers
    # ------------------------------------------------------------------

    def _insert_staging(self, table_name: str, pa_table: pa.Table) -> None:
        """Append rows to {table_name}_staging (no PK, no conflict)."""
        staging = f'{table_name}_staging'
        temp_name = f'__stage_{table_name}'

        def _do():
            if table_name not in self._staging_created:
                self.conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {staging} AS "
                    f"SELECT * FROM {table_name} WHERE FALSE"
                )
                self._staging_created.add(table_name)

            self.conn.register(temp_name, pa_table)
            try:
                cols = ', '.join(pa_table.column_names)
                self.conn.execute(f"""
                    INSERT INTO {staging} ({cols})
                    SELECT {cols} FROM {temp_name}
                """)
                self.conn.commit()
            finally:
                self.conn.unregister(temp_name)

        self.write(_do, f'stage_{table_name}')

    def merge_staging(
        self,
        table_name: str,
        merge_sql: str,
        *,
        max_merge_rows: int = 0,
        pre_aggregate_sql: str = '',
    ) -> int:
        """Merge {table_name}_staging into {table_name}, then drop it.

        Args:
            table_name: Main table name.
            merge_sql: SQL that reads from {table_name}_staging and
                writes into {table_name}.  Typically an INSERT...SELECT
                with GROUP BY and ON CONFLICT for pre-aggregation.
            max_merge_rows: If > 0, merge in batches of this size
                instead of all at once.  Prevents OOM on very large
                staging tables by renaming the staging table and
                processing chunks sequentially.
            pre_aggregate_sql: Optional SQL to collapse staging rows
                before batching.  Must read from {table_name}_staging_all
                and CREATE TABLE {table_name}_staging_agg.  When provided
                and the staging table exceeds max_merge_rows, this runs
                first so that batching operates on the (much smaller)
                aggregated result instead of raw rows.  Each unique key
                then hits ON CONFLICT at most once across the entire merge.

        Returns:
            Number of staging rows merged, or 0 if no staging table.
        """
        staging = f'{table_name}_staging'

        def _do():
            tables = [r[0] for r in
                      self.conn.execute("SHOW TABLES").fetchall()]

            staging_all = f'{staging}_all'
            staging_agg = f'{staging}_agg'

            # Recover stale leftovers from a previously-crashed merge.
            # Must run before anything else to handle:
            #   - Process crash during batched merge leaving _all/_agg
            #   - write() retry after a committed rename
            has_staging = staging in tables
            for stale in (staging_agg, staging_all):
                if stale not in tables:
                    continue
                if not has_staging:
                    # staging was renamed away; rename stale back
                    self.conn.execute(
                        f'ALTER TABLE {stale} RENAME TO {staging}')
                    self.conn.commit()
                    has_staging = True
                    logger.info("staging_recovered_rename",
                                table=table_name, stale_table=stale)
                else:
                    # Both exist; merge stale rows into staging
                    stale_n = self.conn.execute(
                        f'SELECT COUNT(*) FROM {stale}'
                    ).fetchone()[0]
                    if stale_n > 0:
                        self.conn.execute(
                            f'INSERT INTO {staging} SELECT * FROM {stale}')
                        logger.info("staging_recovered_stale",
                                    table=table_name, stale_table=stale,
                                    rows=stale_n)
                    self.conn.execute(f'DROP TABLE {stale}')
                    self.conn.commit()

            if not has_staging:
                return 0
            n = self.conn.execute(
                f'SELECT COUNT(*) FROM {staging}'
            ).fetchone()[0]
            if n == 0:
                self.conn.execute(f'DROP TABLE {staging}')
                self.conn.commit()
                self._staging_created.discard(table_name)
                return 0

            if max_merge_rows <= 0 or n <= max_merge_rows:
                # Single merge — original fast path
                self.conn.execute(merge_sql)
                self.conn.execute(f'DROP TABLE {staging}')
                self.conn.commit()
                self._staging_created.discard(table_name)
                return n

            # Batched merge: rename staging aside, process chunks
            self.conn.execute(
                f'ALTER TABLE {staging} RENAME TO {staging_all}')
            self.conn.commit()

            # Pre-aggregate if caller provided SQL
            if pre_aggregate_sql:
                logger.info("staging_pre_aggregate_start",
                            table=table_name, raw_rows=n)
                self.conn.execute(pre_aggregate_sql)
                self.conn.commit()
                agg_n = self.conn.execute(
                    f'SELECT COUNT(*) FROM {staging_agg}'
                ).fetchone()[0]
                self.conn.execute(f'DROP TABLE {staging_all}')
                self.conn.execute(
                    f'ALTER TABLE {staging_agg} RENAME TO {staging_all}')
                self.conn.commit()
                logger.info("staging_pre_aggregate_done",
                            table=table_name, raw_rows=n,
                            aggregated_rows=agg_n)

                # After aggregation the table may fit in one shot
                if agg_n <= max_merge_rows:
                    self.conn.execute(
                        f'ALTER TABLE {staging_all} RENAME TO {staging}')
                    self.conn.commit()
                    self.conn.execute(merge_sql)
                    self.conn.execute(f'DROP TABLE {staging}')
                    self.conn.commit()
                    self._staging_created.discard(table_name)
                    return n

                # Update count for progress logging
                n_remaining = agg_n
            else:
                n_remaining = n

            merged = 0
            while True:
                # Take a batch from the full staging table
                self.conn.execute(
                    f'CREATE TABLE {staging} AS '
                    f'SELECT * FROM {staging_all} '
                    f'LIMIT {max_merge_rows}')
                batch_n = self.conn.execute(
                    f'SELECT COUNT(*) FROM {staging}'
                ).fetchone()[0]
                if batch_n == 0:
                    self.conn.execute(f'DROP TABLE {staging}')
                    break

                # Run caller-provided merge SQL (reads {staging})
                self.conn.execute(merge_sql)
                merged += batch_n
                self.conn.execute(f'DROP TABLE {staging}')

                # Remove processed rows from staging_all
                self.conn.execute(
                    f'DELETE FROM {staging_all} WHERE rowid IN '
                    f'(SELECT rowid FROM {staging_all} '
                    f'LIMIT {max_merge_rows})')
                self.conn.commit()
                logger.info("staging_batch_merged",
                            table=table_name,
                            batch=batch_n, total_merged=merged,
                            remaining=n_remaining - merged)

            self.conn.execute(f'DROP TABLE IF EXISTS {staging_all}')
            self.conn.commit()
            self._staging_created.discard(table_name)
            return n

        result = self.write(_do, f'merge_staging_{table_name}')
        if result:
            logger.info("staging_merged", table=table_name, rows=result)
        return result or 0

    def staging_count(self, table_name: str) -> int:
        """Row count of {table_name}_staging, or 0 if it doesn't exist."""
        staging = f'{table_name}_staging'
        try:
            return self.conn.execute(
                f'SELECT COUNT(*) FROM {staging}'
            ).fetchone()[0]
        except duckdb.CatalogException:
            return 0

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("store_closed", db_path=self.db_path)
        if self._snapshot:
            from stoic.storage.snapshot import refresh_snapshot
            refresh_snapshot(self.db_path)


# Alias for protocol-aware imports
DuckDBStore = Store
