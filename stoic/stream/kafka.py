"""Kafka stream plug — wraps confluent_kafka.Consumer.

Deserialization is pushed into the stream plug so the generic loop
receives pre-deserialized Message.value() objects.
"""

import json
from typing import Callable, Optional

import structlog

logger = structlog.get_logger()


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
        from confluent_kafka import Consumer

        config = {
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
            config.update(consumer_config)

        self._consumer = Consumer(config)
        self._group_id = group_id
        self._deserialize_fn = self._build_deserializer(
            deserializer, schema_registry_url, value_deserializer,
        )

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
        self._consumer.subscribe(topics)
        logger.info("kafka_subscribed", topics=topics, group=self._group_id)

    def poll(self, batch_size: int, timeout: float) -> list[KafkaMessage]:
        """Poll up to *batch_size* messages, filtering partition EOF."""
        from confluent_kafka import KafkaError

        raw_msgs = self._consumer.consume(batch_size, timeout=timeout)
        if not raw_msgs:
            return []

        result = []
        for msg in raw_msgs:
            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                # Wrap real errors so the loop can inspect .error()
                result.append(KafkaMessage(msg, self._deserialize_fn))
            else:
                result.append(KafkaMessage(msg, self._deserialize_fn))
        return result

    def close(self) -> None:
        self._consumer.close()
