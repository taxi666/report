from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata

from app.schemas.kafka import KafkaTrackingEvent


class KafkaProducerClient:
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topic: str = "tracking_events",
        client_id: str = "tracking-api",
        **producer_kwargs: Any,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._client_id = client_id
        self._producer_kwargs = producer_kwargs
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    @property
    def topic(self) -> str:
        return self._topic

    async def start(self) -> None:
        if self._started:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            value_serializer=self._serialize_value,
            key_serializer=self._serialize_key,
            **self._producer_kwargs,
        )
        await self._producer.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started or self._producer is None:
            return

        await self._producer.stop()
        self._producer = None
        self._started = False

    async def send_event(self, data: Mapping[str, Any]) -> RecordMetadata:
        if not self._started or self._producer is None:
            raise RuntimeError("KafkaProducerClient is not started")

        event = KafkaTrackingEvent.model_validate(dict(data))
        payload = event.model_dump(mode="json")

        return await self._producer.send_and_wait(
            self._topic,
            value=payload,
            key=event.user_id,
        )

    @staticmethod
    def _serialize_value(value: Mapping[str, Any]) -> bytes:
        return json.dumps(
            dict(value),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    @staticmethod
    def _serialize_key(value: str) -> bytes:
        return value.encode("utf-8")
