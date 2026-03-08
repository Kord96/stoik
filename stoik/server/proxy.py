"""Flight SQL connection-pooling proxy.

Sits between N downstream ADBC/Flight SQL clients and an upstream
Flight SQL server (e.g. DuckDB-backed).  A bounded pool of
``pyarrow.flight.FlightClient`` instances serializes upstream access,
eliminating lock contention when many independent processes query
concurrently.

Usage::

    python -m stoik.server.proxy \\
        --upstream grpc://localhost:8815 \\
        --listen   grpc://0.0.0.0:8816 \\
        --pool-size 4
"""

import argparse
import queue
import threading

import pyarrow as pa
import pyarrow.flight as flight
import structlog
from google.protobuf import any_pb2
from flightsql import flightsql_pb2 as pb2

from stoik.server.flight import _FlightSQLBase

logger = structlog.get_logger()


class FlightSQLProxy(_FlightSQLBase):
    """Transparent Flight SQL proxy with upstream connection pooling.

    Args:
        location: gRPC bind address for downstream clients.
        upstream: gRPC URI of the upstream Flight SQL server.
        pool_size: Number of ``FlightClient`` instances in the pool.
        borrow_timeout: Seconds to wait for a pooled client before raising.
    """

    def __init__(self, location: str, *,
                 upstream: str,
                 pool_size: int = 4,
                 borrow_timeout: float = 60.0,
                 **kwargs):
        super().__init__(location, **kwargs)
        self._upstream = upstream
        self._pool_size = pool_size
        self._borrow_timeout = borrow_timeout
        self._pool: queue.Queue[flight.FlightClient] = queue.Queue(
            maxsize=pool_size,
        )
        self._pool_lock = threading.Lock()
        for _ in range(pool_size):
            self._pool.put(self._make_client())
        logger.info("proxy_pool_ready", upstream=upstream, pool_size=pool_size)

    def _make_client(self) -> flight.FlightClient:
        return flight.FlightClient(self._upstream)

    def _borrow(self) -> flight.FlightClient:
        """Borrow a FlightClient from the pool (blocks up to timeout)."""
        try:
            return self._pool.get(timeout=self._borrow_timeout)
        except queue.Empty:
            raise flight.FlightUnavailableError(
                "Upstream connection pool exhausted"
            )

    def _return(self, client: flight.FlightClient) -> None:
        """Return a healthy client to the pool."""
        self._pool.put(client)

    def _replace(self, dead: flight.FlightClient) -> None:
        """Discard a dead client and put a fresh one in the pool."""
        try:
            dead.close()
        except Exception:
            pass
        self._pool.put(self._make_client())

    # ── Upstream forwarding ───────────────────────────────────────

    def _forward_descriptor(self, descriptor: flight.FlightDescriptor) -> pa.Table:
        """Forward a FlightDescriptor to upstream, return materialized Table."""
        client = self._borrow()
        try:
            info = client.get_flight_info(descriptor)
            endpoint = info.endpoints[0]
            reader = client.do_get(endpoint.ticket)
            table = reader.read_all()
            self._return(client)
            return table
        except Exception:
            logger.exception("upstream_error")
            self._replace(client)
            raise

    def _forward_command(self, any_msg: any_pb2.Any) -> pa.Table:
        """Serialize a protobuf command into a descriptor and forward it."""
        descriptor = flight.FlightDescriptor.for_command(
            any_msg.SerializeToString(),
        )
        return self._forward_descriptor(descriptor)

    # ── Query handlers ────────────────────────────────────────────

    def _handle_statement_query(self, any_msg):
        cmd = pb2.CommandStatementQuery()
        any_msg.Unpack(cmd)
        logger.info("proxy_query", sql=cmd.query[:200])
        table = self._forward_command(any_msg)
        return self._stash_and_info(table)

    def _handle_get_tables(self, any_msg):
        table = self._forward_command(any_msg)
        return self._stash_and_info(table)

    def _handle_get_db_schemas(self):
        any_msg = any_pb2.Any()
        any_msg.Pack(pb2.CommandGetDbSchemas())
        table = self._forward_command(any_msg)
        return self._stash_and_info(table)

    def _handle_get_catalogs(self):
        any_msg = any_pb2.Any()
        any_msg.Pack(pb2.CommandGetCatalogs())
        table = self._forward_command(any_msg)
        return self._stash_and_info(table)


# ── CLI entrypoint ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Flight SQL connection-pooling proxy",
    )
    parser.add_argument(
        "--upstream", required=True,
        help="Upstream Flight SQL URI (e.g. grpc://localhost:8815)",
    )
    parser.add_argument(
        "--listen", default="grpc://0.0.0.0:8816",
        help="Listen address (default: grpc://0.0.0.0:8816)",
    )
    parser.add_argument(
        "--pool-size", type=int, default=4,
        help="Number of upstream connections (default: 4)",
    )
    args = parser.parse_args()

    server = FlightSQLProxy(
        args.listen,
        upstream=args.upstream,
        pool_size=args.pool_size,
    )
    logger.info("proxy_serving", listen=args.listen, upstream=args.upstream)
    server.serve()


if __name__ == "__main__":
    main()
