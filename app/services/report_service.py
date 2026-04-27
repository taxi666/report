from fastapi import Request

from app.schemas.tracking import QueuedReportEvent, TrackPayload
from app.services.queue_service import EventQueueService


class ReportService:
    def __init__(self, queue_service: EventQueueService) -> None:
        self._queue_service = queue_service

    async def enqueue_report(self, request: Request, payload: TrackPayload) -> None:
        client_ip = request.client.host if request.client else None
        event = QueuedReportEvent(
            request_path=request.url.path,
            client_ip=client_ip,
            user_agent=request.headers.get("user-agent"),
            params=payload.params,
        )
        self._queue_service.enqueue_nowait(event)
