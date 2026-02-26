"""Kafka stream plug — wraps confluent_kafka.Consumer.

Deserialization is pushed into the stream plug so the generic loop
receives pre-deserialized Message.value() objects.

Includes automatic reconnection when the broker becomes unreachable
(e.g. librdkafka REQTMOUT loop after prolonged broker unavailability).
"""

import json
import time
from typing import Callable, Optional

import structlog

logger = structlog.get_logger()

# Number of consecutive errors before triggering reconnect
_RECONNECT_THRESHOLD = 10
# Seconds without any successful message before reconnect (when errors present)
_IDLE_RECONNECT_SECS = 120


class KafkaMessage:
    """Wraps confluent_kafka.Message with lazy pre-deserialized value."""

    __slots__ = ('_msg', '_deserialize', '_value', '_decoded')

    def __init__(self, raw_msg, deserialize_fn):
        self._msg = raw_msg
        self._deserialize = deserialize_fn
        self._value = None
        self._decoded = False

    def topic(self) -> str:
        return self._msg.topic()

    def value(self):
        if not self._decoded:
            self._value = self._deserialize(self._msg)
            self._decoded = True
        return self._value

    def error(self):
        return self._msg.error()

    def timestamp(self):
        return self._msg.timestamp()


class KafkaStream:
    """Kafka consumer that satisfies the Stream protocol.

    Handles deserialization (JSON / Avro / custom) internally.
    poll() filters out partition EOF and returns only data messages
    (plus real error messages).

    Automatically reconnects when consecutive errors exceed the
    threshold or when no successful messages arrive for too long
    while errors are present.
    """

    def __init__(
        self,
        broker: str,
        group_id: str,
        *,
        deserializer: str = 'json',
        schema_registry_url: Optional[str] = None,
        consumer_config: Optional[dict] = None,
        value_deserializer: Optional[Callable] = None,
    ):
        self._broker = broker
        self._group_id = group_id
        self._base_config = {
            'bootstrap.servers': broker,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'fetch.max.bytes': 52428800,       # 50 MB
            'max.partition.fetch.bytes': 10485760,  # 10 MB
            'fetch.wait.max.ms': 500,
            'max.poll.interval.ms': 1800000,   # 30 min
        }
        if consumer_config:
            self._base_config.update(consumer_config)

        self._deser_mode = deserializer
        self._schema_registry_url = schema_registry_url
        self._value_deserializer = value_deserializer
        self._topics: list[str] = []

        # Error tracking for reconnection
        self._consecutive_errors = 0
        self._last_success = time.monotonic()
        self._first_error_at = 0.0

        self._consumer = self._create_consumer()
        self._deserialize_fn = self._build_deserializer(
            deserializer, schema_registry_url, value_deserializer,
        )

    def _create_consumer(self):
        from confluent_kafka import Consumer
        return Consumer(self._base_config)

    @staticmethod
    def _build_deserializer(
        mode: str,
        schema_registry_url: Optional[str],
        value_deserializer: Optional[Callable],
    ) -> Callable:
        if mode == 'avro':
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import (
                SerializationContext, MessageField,
            )
            registry = SchemaRegistryClient({'url': schema_registry_url})
            avro_deser = AvroDeserializer(registry)

            def _avro(msg):
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                return avro_deser(msg.value(), ctx)
            return _avro

        if mode == 'custom' and value_deserializer is not None:
            def _custom(msg):
                return value_deserializer(msg.value())
            return _custom

        # Default: JSON
        def _json(msg):
            return json.loads(msg.value().decode('utf-8'))
        return _json

    def subscribe(self, topics: list[str]) -> None:
        self._topics = list(topics)
        self._consumer.subscribe(topics)
        logger.info("kafka_subscribed", topics=topics, group=self._group_id)

    def reconnect(self) -> None:
        """Close the current consumer and create a fresh one.

        Re-subscribes to the same topics. Committed offsets are preserved
        in the broker, so consumption resumes from where it left off.
        """
        logger.warning("kafka_reconnecting",
                        group=self._group_id,
                        consecutive_errors=self._consecutive_errors)
        try:
            self._consumer.close()
        except Exception:
            pass

        time.sleep(2)  # Brief pause before reconnecting

        self._consumer = self._create_consumer()
        self._deserialize_fn = self._build_deserializer(
            self._deser_mode, self._schema_registry_url,
            self._value_deserializer,
        )
        if self._topics:
            self._consumer.subscribe(self._topics)
            logger.info("kafka_resubscribed",
                        topics=self._topics, group=self._group_id)

        self._consecutive_errors = 0
        self._first_error_at = 0.0
        self._last_success = time.monotonic()

    def _check_reconnect_needed(self) -> bool:
        """Return True if error pattern warrants reconnection."""
        if self._consecutive_errors >= _RECONNECT_THRESHOLD:
            return True
        if (self._first_error_at > 0
                and time.monotonic() - self._first_error_at > _IDLE_RECONNECT_SECS):
            return True
        return False

    def poll(self, batch_size: int, timeout: float) -> list[KafkaMessage]:
        """Poll up to *batch_size* messages, filtering partition EOF."""
        from confluent_kafka import KafkaError

        try:
            raw_msgs = self._consumer.consume(batch_size, timeout=timeout)
        except Exception as exc:
            logger.error("kafka_consume_exception", error=str(exc),
                         group=self._group_id)
            self._consecutive_errors += 1
            if not self._first_error_at:
                self._first_error_at = time.monotonic()
            if self._check_reconnect_needed():
                self.reconnect()
            return []

        if not raw_msgs:
            return []

        result = []
        had_data = False
        for msg in raw_msgs:
            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                self._consecutive_errors += 1
                if not self._first_error_at:
                    self._first_error_at = time.monotonic()
                # Wrap real errors so the loop can inspect .error()
                result.append(KafkaMessage(msg, self._deserialize_fn))
            else:
                had_data = True
                result.append(KafkaMessage(msg, self._deserialize_fn))

        if had_data:
            self._consecutive_errors = 0
            self._first_error_at = 0.0
            self._last_success = time.monotonic()

        # Check if reconnect needed after processing batch
        if self._check_reconnect_needed():
            self.reconnect()
            return []  # Discard error-only batch, fresh poll on next call

        return result

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:
            pass
