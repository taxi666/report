from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RawTrackingEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str
    user_id: str
    received_at: datetime
    request_path: str
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_id", "user_id", "request_path")
    @classmethod
    def validate_non_empty_string(cls, value: str) -> str:
        value = str(value).strip()
        if not value:
            raise ValueError("field cannot be empty")
        return value

    @field_validator("received_at")
    @classmethod
    def normalize_received_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class ProcessedTrackingEvent(BaseModel):
    event_id: str
    user_id: str
    received_at: datetime
    request_path: str
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    params_json: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TrackingEventProcessor:
    def process(
        self,
        payload: Dict[str, Any],
        *,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> ProcessedTrackingEvent:
        raw_event = RawTrackingEvent.model_validate(payload)
        params_json = json.dumps(
            raw_event.params,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

        return ProcessedTrackingEvent(
            event_id=raw_event.event_id,
            user_id=raw_event.user_id,
            received_at=raw_event.received_at,
            request_path=raw_event.request_path,
            client_ip=raw_event.client_ip,
            user_agent=raw_event.user_agent,
            params_json=params_json,
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
        )
