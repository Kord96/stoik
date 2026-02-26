"""ADBC Flight SQL connection pool.

Provides a bounded pool of ADBC connections to a Flight SQL server.
Used by the HTTP API facade to delegate all DuckDB queries to the
Flight SQL server, which already manages snapshot lifecycle, entity
views, and connection refresh.

Usage::

    pool = FlightPool('grpc://localhost:8815', size=4)
    pool.start()

    with pool.cursor() as cur:
        cur.execute("SELECT * FROM domain WHERE domain = 'example.com'")
        rows = cur.fetch_arrow_table().to_pylist()

    pool.stop()
"""

import logging
import os
import queue
import time
from contextlib import contextmanager

import adbc_driver_flightsql.dbapi as flight_dbapi

logger = logging.getLogger(__name__)

_DEFAULT_URL = 'grpc://localhost:8815'


class FlightPool:
    """Bounded pool of ADBC connections to a Flight SQL server."""

    def __init__(self, url: str | None = None, size: int = 4,
                 checkout_timeout: float = 30.0):
        self._url = url or os.environ.get('GRAPH_FLIGHT_URL', _DEFAULT_URL)
        self._size = size
        self._checkout_timeout = checkout_timeout
        self._pool: queue.Queue = queue.Queue(maxsize=size)
        self._started = False

    @property
    def url(self) -> str:
        return self._url

    def start(self):
        """Create initial connections and fill the pool."""
        for i in range(self._size):
            conn = self._new_connection()
            if conn is not None:
                self._pool.put(conn)
            else:
                logger.warning("Failed to create pool connection %d/%d",
                               i + 1, self._size)
        self._started = True
        logger.info("FlightPool started (%d connections to %s)",
                     self._pool.qsize(), self._url)

    def stop(self):
        """Drain and close all connections."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Exception:
                pass
        self._started = False
        logger.info("FlightPool stopped")

    def _new_connection(self):
        for attempt in range(3):
            try:
                return flight_dbapi.connect(self._url)
            except Exception:
                if attempt == 2:
                    logger.error("Failed to connect to Flight SQL at %s",
                                 self._url)
                    return None
                time.sleep(1.0 * (attempt + 1))

    @contextmanager
    def cursor(self):
        """Borrow a connection, yield a cursor, return to pool.

        If the cursor raises, the broken connection is replaced.
        """
        conn = None
        try:
            conn = self._pool.get(timeout=self._checkout_timeout)
        except queue.Empty:
            raise RuntimeError(
                "FlightPool exhausted — all connections in use")

        try:
            cur = conn.cursor()
            try:
                yield cur
            finally:
                cur.close()
        except Exception:
            # Connection may be broken — replace it
            try:
                conn.close()
            except Exception:
                pass
            conn = self._new_connection()
            raise
        finally:
            if conn is not None:
                self._pool.put(conn)

    def fetch_dicts(self, sql: str) -> list[dict]:
        """Execute SQL and return results as list of dicts."""
        with self.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
            return table.to_pylist()

    def fetch_one(self, sql: str) -> dict | None:
        """Execute SQL and return first row as dict, or None."""
        rows = self.fetch_dicts(sql)
        return rows[0] if rows else None
