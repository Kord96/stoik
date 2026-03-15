"""Backward-compat shim — real implementation in stoik.storage.snapshot."""

from stoik.storage.snapshot import (
    SNAPSHOT_DIR, snapshot_path, refresh_snapshot, open_snapshot,
    configure_s3, parquet_prefix, parquet_table_uri,
    SNAPSHOT_BACKEND, MINIO_BUCKET,
)

# Backward compat — open_graph moved to graphdb.lib.dbpool
def open_graph(*db_paths):
    """Deprecated: use graphdb.lib.dbpool.open_graph instead."""
    from stoik.storage.snapshot import open_snapshot, snapshot_path, SNAPSHOT_BACKEND, configure_s3, MINIO_BUCKET
    from pathlib import Path
    import duckdb

    conn = duckdb.connect(':memory:')
    if SNAPSHOT_BACKEND == 'minio':
        configure_s3(conn)
        for db_path in db_paths:
            stem = Path(db_path).stem
            files = conn.execute(
                f"SELECT filename FROM glob('s3://{MINIO_BUCKET}/{stem}/*.parquet')"
            ).fetchall()
            if not files:
                conn.close()
                raise FileNotFoundError(f"No Parquet snapshot for {db_path}")
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{stem}"')
            for (fp,) in files:
                table = Path(fp).stem
                conn.execute(f'CREATE VIEW "{stem}"."{table}" AS SELECT * FROM read_parquet(\'{fp}\')')
    else:
        import os
        for db_path in db_paths:
            snap = snapshot_path(db_path)
            if not os.path.isfile(snap):
                conn.close()
                raise FileNotFoundError(f"No snapshot for {db_path}: {snap}")
            alias = Path(snap).stem
            conn.execute(f"ATTACH '{snap}' AS \"{alias}\" (READ_ONLY)")
    return conn

__all__ = ['SNAPSHOT_DIR', 'snapshot_path', 'refresh_snapshot', 'open_snapshot', 'open_graph',
           'configure_s3', 'parquet_prefix', 'parquet_table_uri', 'SNAPSHOT_BACKEND', 'MINIO_BUCKET']
