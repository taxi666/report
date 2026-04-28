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

        user_id = normalized.get("user_id")
        if user_id is None:
            raise ValueError("query parameter 'user_id' is required")

        if isinstance(user_id, list):
            user_id = next((item.strip() for item in user_id if item.strip()), "")
        else:
            user_id = user_id.strip()

        if not user_id:
            raise ValueError("query parameter 'user_id' cannot be empty")

        return normalized

    @property
    def user_id(self) -> str:
        raw_user_id = self.params["user_id"]
        if isinstance(raw_user_id, list):
            return next(item for item in raw_user_id if item.strip()).strip()
        return raw_user_id.strip()


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
