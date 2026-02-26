"""Generic FastAPI application factory over Flight SQL.

Provides the HTTP facade layer: lifespan management (FlightPool + EntityCache),
background warm thread, Prometheus metrics, and dependency injection for routers.

Usage::

    from stoic.server.api import create_api, ApiConfig

    config = ApiConfig(
        flight_url='grpc://localhost:8815',
        pool_size=4,
        cache_tiers={'domain': {'warm': True, 'warm_limit': 8000, 'lru_limit': 15000}},
        warm_callback=my_warm_function,
    )
    app = create_api(config, routers=[my_router], title='My API')
"""

import logging
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Callable

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from prometheus_client import (
    Histogram, Counter, generate_latest, CONTENT_TYPE_LATEST,
)

from .cache import EntityCache
from .flight_pool import FlightPool

logger = logging.getLogger(__name__)

# ── Prometheus metrics ───────────────────────────────────────────────

_REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint', 'status'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
)

_REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status'],
)


def _normalize_path(path: str) -> str:
    """Collapse path parameters to templates for metric labels.

    /node/domain/gmail.com → /node/domain/{key}
    /node/subnet/1.2.3.0/24/neighbors → /node/subnet/{key}/neighbors
    """
    parts = path.strip('/').split('/')
    if not parts:
        return path

    # Known top-level routes
    if parts[0] == 'node' and len(parts) >= 3:
        # /node/{entity_type}/{key...}[/neighbors|/content]
        suffix = ''
        if parts[-1] in ('neighbors', 'content'):
            suffix = f'/{parts[-1]}'
            key_parts = parts[2:-1]
        else:
            key_parts = parts[2:]
        if key_parts:
            return f'/node/{parts[1]}/{{key}}{suffix}'
    elif parts[0] == 'messages' and len(parts) >= 3:
        return f'/messages/{parts[1]}/{{key}}'
    elif parts[0] == 'path' and len(parts) >= 3:
        return '/path/{src}/to/{tgt}'
    elif parts[0] == 'resolve':
        return f'/resolve/{parts[1]}' if len(parts) > 1 else '/resolve'

    return path


class _MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        duration = time.monotonic() - start

        endpoint = _normalize_path(request.url.path)
        status = str(response.status_code)
        method = request.method

        _REQUEST_DURATION.labels(method, endpoint, status).observe(duration)
        _REQUEST_COUNT.labels(method, endpoint, status).inc()

        return response


# ── Configuration ────────────────────────────────────────────────────


@dataclass
class ApiConfig:
    """Configuration for the HTTP API server."""

    # Server binding
    host: str = '0.0.0.0'
    port: int = 8443
    ssl_cert: str | None = None
    ssl_key: str | None = None

    # Flight SQL backend
    flight_url: str = 'grpc://localhost:8815'
    pool_size: int = 4

    # Cache configuration
    cache_tiers: dict = field(default_factory=dict)
    warm_interval: float = 30.0  # seconds between warm cache cycles

    # Warm callback: (FlightPool, EntityCache, dict[tiers]) -> None
    # Called periodically to populate the warm cache.
    warm_callback: Callable | None = None

    # Lifespan hooks: (app, config) -> None
    # Called during startup/shutdown for project-specific resources.
    on_startup: Callable | None = None
    on_shutdown: Callable | None = None


def get_pool(request: Request) -> FlightPool:
    """FastAPI dependency: get the FlightPool from app state."""
    return request.app.state.flight_pool


def get_cache(request: Request) -> EntityCache:
    """FastAPI dependency: get the EntityCache from app state."""
    return request.app.state.entity_cache


def _warm_loop(pool: FlightPool, cache: EntityCache,
               config: ApiConfig, stop: threading.Event):
    """Background thread: periodically refresh the warm cache."""
    if config.warm_callback is None:
        return
    while not stop.is_set():
        stop.wait(timeout=config.warm_interval)
        if stop.is_set():
            break
        try:
            config.warm_callback(pool, cache, config.cache_tiers)
        except Exception:
            logger.warning("Warm cache cycle failed", exc_info=True)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    config: ApiConfig = app.state.api_config
    pool = FlightPool(url=config.flight_url, size=config.pool_size)
    cache = EntityCache(config.cache_tiers)

    pool.start()
    app.state.flight_pool = pool
    app.state.entity_cache = cache

    # Start background warm thread
    stop_event = threading.Event()
    warm_thread = None
    if config.warm_callback is not None:
        warm_thread = threading.Thread(
            target=_warm_loop,
            args=(pool, cache, config, stop_event),
            daemon=True,
        )
        warm_thread.start()
        logger.info("Warm cache thread started (interval=%ss)",
                     config.warm_interval)

    # Project-specific startup hook
    if config.on_startup is not None:
        config.on_startup(app, config)

    try:
        yield
    finally:
        # Project-specific shutdown hook
        if config.on_shutdown is not None:
            try:
                config.on_shutdown(app, config)
            except Exception:
                logger.warning("Shutdown hook failed", exc_info=True)
        stop_event.set()
        if warm_thread is not None:
            warm_thread.join(timeout=10)
        pool.stop()
        logger.info("API server stopped")


def create_api(config: ApiConfig,
               routers: list | None = None,
               title: str = 'Flight SQL API') -> FastAPI:
    """Create a FastAPI app backed by a Flight SQL connection pool.

    Args:
        config: Server and cache configuration.
        routers: List of APIRouter instances to include.
        title: OpenAPI title.

    Returns:
        Configured FastAPI application (not yet running).
    """
    app = FastAPI(title=title, lifespan=_lifespan)
    app.state.api_config = config

    # Add Prometheus metrics middleware
    app.add_middleware(_MetricsMiddleware)

    # Add metrics endpoint
    @app.get('/metrics', include_in_schema=False)
    async def metrics():
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    if routers:
        for router in routers:
            app.include_router(router)

    return app
