"""Server protocol — query server contract."""

from typing import Any, Protocol


class Server(Protocol):
    """Query server lifecycle."""

    def serve(self) -> None: ...
    def shutdown(self) -> None: ...


class ReadPool(Protocol):
    """Read-only connection pool for query servers."""

    def get_connection(self, scope: str) -> Any: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
