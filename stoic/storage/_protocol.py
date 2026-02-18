"""Storage protocol — lifecycle contract for persistence backends."""

from typing import Callable, Protocol, runtime_checkable


@runtime_checkable
class Storage(Protocol):
    """Lifecycle contract for persistence backends.

    Concrete implementations: DuckDBStore (stoic.storage.duckdb).
    """

    db_path: str

    def release(self) -> None: ...
    def reconnect(self) -> None: ...
    def write(self, fn: Callable, description: str = 'write'): ...
    def merge_staging(self, table_name: str, merge_sql: str, *, max_merge_rows: int = 0, pre_aggregate_sql: str = '') -> int: ...
    def staging_count(self, table_name: str) -> int: ...
    def close(self) -> None: ...
