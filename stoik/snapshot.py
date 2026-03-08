"""Backward-compat shim — real implementation in stoik.storage.snapshot."""

from stoik.storage.snapshot import (
    SNAPSHOT_DIR, snapshot_path, refresh_snapshot, open_snapshot, open_graph,
)

__all__ = ['SNAPSHOT_DIR', 'snapshot_path', 'refresh_snapshot', 'open_snapshot', 'open_graph']
