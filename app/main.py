from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes.report import router as report_router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.services.queue_service import EventQueueService
from app.services.report_service import ReportService


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    queue_service = EventQueueService(maxsize=settings.queue_maxsize)
    report_service = ReportService(queue_service=queue_service)

    app.state.queue_service = queue_service
    app.state.report_service = report_service

    if settings.queue_worker_enabled:
        await queue_service.start()

    try:
        yield
    finally:
        await queue_service.stop()


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(
        title="Tracking API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(report_router)
    return app


app = create_app()
