"""Concurrent DuckDB read/write via XFS reflink snapshots.

After each consumer flush (when the DB file is closed and consistent),
``refresh_snapshot()`` creates a copy-on-write snapshot using
``cp --reflink=always``.  API readers open snapshots via
``open_snapshot()`` or ``open_graph()`` — neither side ever blocks the other.

Snapshot production is gated on SNAPSHOT_DIR existing.  If it doesn't,
``refresh_snapshot()`` silently returns False.
"""

import logging
import os
import subprocess
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

SNAPSHOT_DIR = '/opt/data/graph/snapshots'


def snapshot_path(db_path: str) -> str:
    """Map a DB path to its snapshot location.

    /opt/data/graph/node/domain.duckdb → /opt/data/graph/snapshots/domain.duckdb
    """
    return os.path.join(SNAPSHOT_DIR, os.path.basename(db_path))


def refresh_snapshot(db_path: str) -> bool:
    """Create a reflink snapshot of *db_path* in SNAPSHOT_DIR.

    Uses ``cp --reflink=always`` to a temporary file, then ``os.rename()``
    for an atomic swap.  Non-fatal on any failure (logs warning, returns
    False).  Silently skips if SNAPSHOT_DIR doesn't exist.
    """
    if not os.path.isdir(SNAPSHOT_DIR):
        return False

    if not os.path.isfile(db_path):
        return False

    dest = snapshot_path(db_path)
    tmp = dest + '.tmp'

    try:
        subprocess.run(
            ['cp', '--reflink=auto', db_path, tmp],
            check=True,
            capture_output=True,
        )
        os.rename(tmp, dest)
        return True
    except Exception:
        logger.warning("snapshot_failed: %s", db_path, exc_info=True)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False


def open_snapshot(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a read-only connection to the snapshot of *db_path*.

    Raises FileNotFoundError if the snapshot doesn't exist.
    """
    snap = snapshot_path(db_path)
    if not os.path.isfile(snap):
        raise FileNotFoundError(f"No snapshot for {db_path}: {snap}")
    return duckdb.connect(snap, read_only=True)


def open_graph(*db_paths: str) -> duckdb.DuckDBPyConnection:
    """ATTACH multiple snapshots into one in-memory connection.

    Each snapshot is attached under an alias derived from its filename
    stem (e.g. ``edge.duckdb`` → ``edge``).  Query tables as
    ``edge.association``, ``domain.domain``, etc.

    Raises FileNotFoundError if any snapshot is missing.
    """
    conn = duckdb.connect(':memory:')
    for db_path in db_paths:
        snap = snapshot_path(db_path)
        if not os.path.isfile(snap):
            conn.close()
            raise FileNotFoundError(f"No snapshot for {db_path}: {snap}")
        alias = Path(snap).stem
        conn.execute(
            f"ATTACH '{snap}' AS \"{alias}\" (READ_ONLY)"
        )
    return conn
