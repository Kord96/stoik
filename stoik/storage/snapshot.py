"""Concurrent DuckDB read/write via snapshots.

Supports two backends (controlled by SNAPSHOT_BACKEND env var):

- ``local`` (default): XFS reflink snapshots via ``cp --reflink=auto``.
  Readers open snapshots directly from SNAPSHOT_DIR.

- ``minio``: Export tables as Parquet, upload to MinIO (S3-compatible).
  Readers use DuckDB's ``read_parquet('s3://...')``.

Snapshot production is gated on SNAPSHOT_DIR existing (local) or
MINIO_ENDPOINT being set (minio).
"""

import logging
import os
import subprocess
import tempfile
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

SNAPSHOT_DIR = '/opt/data/graph/snapshots'
SNAPSHOT_BACKEND = os.environ.get('SNAPSHOT_BACKEND', 'local')  # 'local' or 'minio'

# MinIO configuration (used when SNAPSHOT_BACKEND='minio')
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio.prod.svc.cluster.local:9000')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'snapshots')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', '')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', '')
MINIO_USE_SSL = os.environ.get('MINIO_USE_SSL', 'false').lower() == 'true'


def configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure a DuckDB connection to read/write from MinIO."""
    try:
        conn.execute("INSTALL httpfs;")
    except Exception:
        pass  # Already installed
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute(f"SET s3_use_ssl={'true' if MINIO_USE_SSL else 'false'};")
    conn.execute("SET s3_url_style='path';")


def snapshot_path(db_path: str) -> str:
    """Map a DB path to its snapshot location (local backend).

    /opt/data/graph/node/domain.duckdb → /opt/data/graph/snapshots/domain.duckdb
    """
    return os.path.join(SNAPSHOT_DIR, os.path.basename(db_path))


def parquet_prefix(db_path: str) -> str:
    """Map a DB path to its MinIO prefix.

    /opt/data/graph/node/base_domain.duckdb → 's3://snapshots/base_domain/'
    """
    stem = Path(db_path).stem
    return f's3://{MINIO_BUCKET}/{stem}/'


def parquet_table_uri(db_path: str, table: str) -> str:
    """URI for a specific table's Parquet file in MinIO.

    >>> parquet_table_uri('/opt/data/graph/node/base_domain.duckdb', 'domain')
    's3://snapshots/base_domain/domain.parquet'
    """
    stem = Path(db_path).stem
    return f's3://{MINIO_BUCKET}/{stem}/{table}.parquet'


# ── Write path ──────────────────────────────────────────────────

def refresh_snapshot(db_path: str) -> bool:
    """Create a snapshot of *db_path*.

    Backend 'local': cp --reflink=auto to SNAPSHOT_DIR (atomic rename).
    Backend 'minio': export each table as Parquet to MinIO.
    """
    if SNAPSHOT_BACKEND == 'minio':
        return _refresh_minio(db_path)
    return _refresh_local(db_path)


def _refresh_local(db_path: str) -> bool:
    """Local reflink snapshot (original behavior)."""
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


def _refresh_minio(db_path: str) -> bool:
    """Export tables as Parquet to MinIO."""
    if not os.path.isfile(db_path):
        return False

    stem = Path(db_path).stem

    try:
        src = duckdb.connect(db_path, read_only=True)
        tables = [row[0] for row in src.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()]

        if not tables:
            src.close()
            return True

        for table in tables:
            uri = f's3://{MINIO_BUCKET}/{stem}/{table}.parquet'
            with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
                # Export to local temp file first (fast, no network)
                src.execute(
                    f'COPY (SELECT * FROM "{table}") '
                    f"TO '{tmp.name}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                # Upload to MinIO via DuckDB httpfs
                upload = duckdb.connect(':memory:')
                configure_s3(upload)
                upload.execute(
                    f"COPY (SELECT * FROM read_parquet('{tmp.name}')) "
                    f"TO '{uri}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                upload.close()

        src.close()
        logger.info("parquet_snapshot: %s (%d tables)", stem, len(tables))
        return True

    except Exception:
        logger.warning("parquet_snapshot_failed: %s", db_path, exc_info=True)
        return False


# ── Read path ───────────────────────────────────────────────────

def open_snapshot(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a read-only connection to the snapshot of *db_path*.

    Local: opens DuckDB file directly.
    MinIO: creates in-memory conn with views over Parquet files.
    """
    if SNAPSHOT_BACKEND == 'minio':
        return _open_snapshot_minio(db_path)
    return _open_snapshot_local(db_path)


def _open_snapshot_local(db_path: str) -> duckdb.DuckDBPyConnection:
    snap = snapshot_path(db_path)
    if not os.path.isfile(snap):
        raise FileNotFoundError(f"No snapshot for {db_path}: {snap}")
    return duckdb.connect(snap, read_only=True)


def _open_snapshot_minio(db_path: str) -> duckdb.DuckDBPyConnection:
    stem = Path(db_path).stem
    conn = duckdb.connect(':memory:')
    configure_s3(conn)

    # Discover tables by listing Parquet files in the prefix
    try:
        files = conn.execute(
            f"SELECT filename FROM glob('s3://{MINIO_BUCKET}/{stem}/*.parquet')"
        ).fetchall()
    except Exception:
        conn.close()
        raise FileNotFoundError(
            f"No Parquet snapshot for {db_path} in s3://{MINIO_BUCKET}/{stem}/"
        )

    if not files:
        conn.close()
        raise FileNotFoundError(
            f"No Parquet snapshot for {db_path} in s3://{MINIO_BUCKET}/{stem}/"
        )

    for (filepath,) in files:
        table = Path(filepath).stem  # 'domain.parquet' → 'domain'
        conn.execute(
            f'CREATE VIEW "{table}" AS '
            f"SELECT * FROM read_parquet('{filepath}')"
        )

    return conn


