from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.deps import get_report_service, parse_report_query
from app.schemas.tracking import TrackPayload, TrackResponse
from app.services.queue_service import QueueBackpressureError
from app.services.report_service import ReportService

router = APIRouter()


@router.get("/report", response_model=TrackResponse)
async def report(
    request: Request,
    payload: Annotated[TrackPayload, Depends(parse_report_query)],
    report_service: Annotated[ReportService, Depends(get_report_service)],
) -> TrackResponse:
    try:
        await report_service.enqueue_report(request=request, payload=payload)
    except QueueBackpressureError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    return TrackResponse()
