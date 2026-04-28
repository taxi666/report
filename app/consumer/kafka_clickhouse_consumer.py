import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from app.processor import TrackingEventProcessor
from app.processor.tracking_event_processor import ProcessedTrackingEvent
from app.writer import ClickHouseWriter

logger = logging.getLogger(__name__)


class TrackingEventsConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        topic: str,
        group_id: str,
        processor: TrackingEventProcessor,
        writer: ClickHouseWriter,
        batch_size: int = 500,
        flush_interval_seconds: float = 1.0,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._processor = processor
        self._writer = writer
        self._batch_size = batch_size
        self._flush_interval_ms = max(1, int(flush_interval_seconds * 1000))
        self._auto_offset_reset = auto_offset_reset
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if self._consumer is not None:
            return

        await self._writer.init_schema()
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=False,
            key_deserializer=lambda value: value.decode("utf-8") if value else None,
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is None:
            return

        await self._consumer.stop()
        self._consumer = None

    async def run_forever(self) -> None:
        await self.start()
        try:
            while True:
                inserted = await self.poll_and_write_once()
                if inserted:
                    logger.info("Inserted %s tracking events into ClickHouse", inserted)
        finally:
            await self.stop()

    async def poll_and_write_once(self) -> int:
        if self._consumer is None:
            raise RuntimeError("TrackingEventsConsumer is not started")

        records = await self._consumer.getmany(
            timeout_ms=self._flush_interval_ms,
            max_records=self._batch_size,
        )
        if not records:
            return 0

        processed_events: list[ProcessedTrackingEvent] = []
        for messages in records.values():
            for message in messages:
                processed_event = self._process_message(message)
                if processed_event is not None:
                    processed_events.append(processed_event)

        if processed_events:
            await self._writer.insert_events(processed_events)

        await self._consumer.commit()
        return len(processed_events)

    def _process_message(
        self,
        message: ConsumerRecord[str | None, bytes],
    ) -> ProcessedTrackingEvent | None:
        try:
            payload = self._decode_value(message.value)
            return self._processor.process(
                payload,
                kafka_topic=message.topic,
                kafka_partition=message.partition,
                kafka_offset=message.offset,
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError, TypeError) as exc:
            logger.warning(
                "Skipping invalid Kafka message topic=%s partition=%s offset=%s error=%s",
                message.topic,
                message.partition,
                message.offset,
                exc,
            )
            return None

    @staticmethod
    def _decode_value(value: bytes) -> dict[str, Any]:
        decoded = json.loads(value.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise TypeError("Kafka message value must be a JSON object")
        return decoded
