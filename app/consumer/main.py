import asyncio

from app.consumer import TrackingEventsConsumer
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.processor import TrackingEventProcessor
from app.writer import ClickHouseWriter


async def main() -> None:
    configure_logging()
    settings = get_settings()
    writer = ClickHouseWriter(
        host=settings.clickhouse_host,
        http_port=settings.clickhouse_http_port,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        table=settings.clickhouse_tracking_table,
        timeout_seconds=settings.clickhouse_connect_timeout_seconds,
    )
    consumer = TrackingEventsConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_tracking_topic,
        group_id=settings.kafka_consumer_group_id,
        processor=TrackingEventProcessor(),
        writer=writer,
        batch_size=settings.consumer_batch_size,
        flush_interval_seconds=settings.consumer_flush_interval_seconds,
        auto_offset_reset=settings.kafka_consumer_auto_offset_reset,
    )
    await consumer.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
