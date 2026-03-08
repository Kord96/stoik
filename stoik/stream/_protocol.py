"""Stream protocol — consumer contract for message sources."""

from typing import Any, Optional, Protocol


class Message(Protocol):
    """Single message from a stream source."""

    def topic(self) -> str: ...
    def value(self) -> Any: ...
    def error(self) -> Optional[Any]: ...


class Stream(Protocol):
    """Consumer contract for message sources.

    Concrete implementations: KafkaStream (stoik.stream.kafka).
    Deserialization is pushed into the stream plug — poll() returns
    pre-deserialized Message objects.
    """

    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, batch_size: int, timeout: float) -> list: ...
    def close(self) -> None: ...
