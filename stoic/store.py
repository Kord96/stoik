"""Backward-compat shim — real implementation in stoic.storage.duckdb."""

from stoic.storage.duckdb import Store, DuckDBStore, MAX_RETRIES, BASE_DELAY, MAX_DELAY, JITTER

__all__ = ['Store', 'DuckDBStore', 'MAX_RETRIES', 'BASE_DELAY', 'MAX_DELAY', 'JITTER']
