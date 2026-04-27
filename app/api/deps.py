from collections import defaultdict

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from app.schemas.tracking import TrackPayload
from app.services.report_service import ReportService


def get_report_service(request: Request) -> ReportService:
    return request.app.state.report_service


def parse_report_query(request: Request) -> TrackPayload:
    grouped_params: dict[str, list[str]] = defaultdict(list)
    for key, value in request.query_params.multi_items():
        grouped_params[key].append(value)

    normalized_params = {
        key: values[0] if len(values) == 1 else values
        for key, values in grouped_params.items()
    }

    try:
        return TrackPayload(params=normalized_params)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc
