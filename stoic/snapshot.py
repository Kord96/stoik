"""Backward-compat shim — real implementation in stoic.storage.snapshot."""

from stoic.storage.snapshot import (
    SNAPSHOT_DIR, snapshot_path, refresh_snapshot, open_snapshot, open_graph,
)

__all__ = ['SNAPSHOT_DIR', 'snapshot_path', 'refresh_snapshot', 'open_snapshot', 'open_graph']
