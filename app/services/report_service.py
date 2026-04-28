import logging

from fastapi import Request

from app.producer import KafkaProducerClient
from app.schemas.tracking import ReportKafkaEvent, TrackPayload

logger = logging.getLogger(__name__)


class ReportPublishError(Exception):
    pass


class ReportService:
    def __init__(self, producer: KafkaProducerClient) -> None:
        self._producer = producer

    async def publish_report(self, request: Request, payload: TrackPayload) -> None:
        client_ip = request.client.host if request.client else None
        event = ReportKafkaEvent(
            user_id=payload.partition_key(fallback=client_ip or "anonymous"),
            request_path=request.url.path,
            client_ip=client_ip,
            user_agent=request.headers.get("user-agent"),
            params=payload.params,
        )
        try:
            metadata = await self._producer.send_event(event.model_dump(mode="json"))
        except Exception as exc:
            raise ReportPublishError("failed to publish tracking event to Kafka") from exc

        logger.info(
            "Published tracking event event_id=%s topic=%s partition=%s offset=%s",
            event.event_id,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )
