from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class KafkaTrackingEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    user_id: str

    @field_validator("user_id", mode="before")
    @classmethod
    def validate_user_id(cls, value: Any) -> str:
        if value is None:
            raise ValueError("user_id is required")

        user_id = str(value).strip()
        if not user_id:
            raise ValueError("user_id cannot be empty")

        return user_id
