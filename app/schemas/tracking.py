from datetime import datetime, timezone
from uuid import uuid4
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TrackPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    params: dict[str, str | list[str]] = Field(
        ...,
        description="Arbitrary query parameters collected from /report.",
    )

    @field_validator("params")
    @classmethod
    def validate_params(
        cls,
        value: dict[str, str | list[str]],
    ) -> dict[str, str | list[str]]:
        if not value:
            raise ValueError("at least one query parameter is required")

        normalized: dict[str, str | list[str]] = {}
        for raw_key, raw_value in value.items():
            key = raw_key.strip()
            if not key:
                raise ValueError("query parameter name cannot be blank")

            if isinstance(raw_value, list):
                if not raw_value:
                    raise ValueError(f"query parameter '{key}' must have at least one value")
                normalized[key] = [str(item) for item in raw_value]
                continue

            normalized[key] = str(raw_value)

        return normalized

    def partition_key(self, fallback: str = "anonymous") -> str:
        for key in ("user_id", "uid", "device_id", "sid", "trace_id"):
            value = self.params.get(key)
            normalized = self._first_non_empty(value)
            if normalized:
                return normalized
        return fallback

    @staticmethod
    def _first_non_empty(value: str | list[str] | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, list):
            return next((item.strip() for item in value if item.strip()), None)
        value = value.strip()
        return value or None


class ReportKafkaEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    request_path: str
    client_ip: str | None = None
    user_agent: str | None = None
    params: dict[str, str | list[str]]


class TrackResponse(BaseModel):
    status: Literal["ok"] = "ok"
