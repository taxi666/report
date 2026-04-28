from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes.report import router as report_router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.producer import KafkaProducerClient
from app.services.report_service import ReportService


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    producer = KafkaProducerClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_tracking_topic,
        client_id=settings.kafka_client_id,
    )
    report_service = ReportService(producer=producer)

    app.state.kafka_producer = producer
    app.state.report_service = report_service

    await producer.start()

    try:
        yield
    finally:
        await producer.stop()


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
