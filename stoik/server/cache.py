"""Generic two-tier entity cache: LRU (reactive) + warm (proactive).

The LRU tier caches responses to actual requests, evicting least-recently-used
entries when full.  Per-type limits prevent any single type from dominating.

The warm tier proactively loads recently-active entities for configured "hot"
types.  Which types are hot, their limits, and the SQL to warm them are
provided by the caller via a config dict.

Invalidation: bumping the generation counter causes stale entries to be
lazily evicted on next access.  The warm cache is rebuilt entirely on each
warm cycle.

Usage::

    from stoik.server.cache import EntityCache

    tiers = {
        'domain': {'warm': True, 'warm_limit': 8000, 'lru_limit': 15000},
        'host':   {'warm': False, 'warm_limit': 0,    'lru_limit': 5000},
    }

    cache = EntityCache(tiers)
    cache.get('domain', 'example.com')
    cache.put('domain', 'example.com', {'traffic': 100})
    cache.invalidate()
"""

import logging
import threading
from collections import OrderedDict
from typing import Callable

logger = logging.getLogger(__name__)


class _TypeLRU:
    """Bounded LRU dict for a single entity type."""

    __slots__ = ('_data', '_max')

    def __init__(self, max_size: int):
        self._data: OrderedDict[str, tuple] = OrderedDict()
        self._max = max_size

    def get(self, key: str, generation: int):
        entry = self._data.get(key)
        if entry is None:
            return None
        val, gen = entry
        if gen != generation:
            del self._data[key]
            return None
        self._data.move_to_end(key)
        return val

    def put(self, key: str, value, generation: int):
        self._data[key] = (value, generation)
        self._data.move_to_end(key)
        while len(self._data) > self._max:
            self._data.popitem(last=False)

    def __len__(self):
        return len(self._data)


class EntityCache:
    """Two-tier entity cache with generation-based invalidation.

    Args:
        tiers: ``{entity_type: {'warm': bool, 'warm_limit': int, 'lru_limit': int}}``
    """

    def __init__(self, tiers: dict[str, dict]):
        self._generation = 0
        self._lock = threading.Lock()
        self._tiers = tiers

        self._lru: dict[str, _TypeLRU] = {}
        for etype, cfg in tiers.items():
            self._lru[etype] = _TypeLRU(cfg.get('lru_limit', 1000))

        # Warm cache: entity_type -> {pk_value -> cached_value}
        self._warm: dict[str, dict[str, object]] = {}
        self._warm_generation = -1

    @property
    def generation(self) -> int:
        return self._generation

    def get(self, entity_type: str, key: str):
        """Look up a cached value. Returns None on miss."""
        gen = self._generation

        lru = self._lru.get(entity_type)
        if lru is not None:
            val = lru.get(key, gen)
            if val is not None:
                return val

        if self._warm_generation == gen:
            warm_type = self._warm.get(entity_type)
            if warm_type is not None:
                return warm_type.get(key)

        return None

    def put(self, entity_type: str, key: str, value):
        """Store a value in the LRU cache for the given type."""
        lru = self._lru.get(entity_type)
        if lru is None:
            # Unknown type — use a default LRU
            lru = _TypeLRU(1000)
            self._lru[entity_type] = lru
        lru.put(key, value, self._generation)

    def invalidate(self):
        """Bump generation, lazily invalidating all stale entries."""
        self._generation += 1
        logger.debug("Cache generation bumped to %d", self._generation)

    def set_warm(self, data: dict[str, dict[str, object]]):
        """Replace the warm cache with new data.

        Args:
            data: ``{entity_type: {pk_value: cached_value}}``
        """
        self._warm = data
        self._warm_generation = self._generation
        total = sum(len(v) for v in data.values())
        logger.info("Warm cache set: %d entries across %d types (gen=%d)",
                     total, len(data), self._generation)

    def stats(self) -> dict:
        """Return cache statistics."""
        return {
            'generation': self._generation,
            'lru': {et: len(lru) for et, lru in self._lru.items()},
            'warm': {et: len(entries) for et, entries in self._warm.items()},
        }
