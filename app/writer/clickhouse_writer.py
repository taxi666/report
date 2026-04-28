import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.processor.tracking_event_processor import ProcessedTrackingEvent


class ClickHouseWriteError(Exception):
    pass


class ClickHouseWriter:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        http_port: int = 8123,
        user: str = "default",
        password: str = "",
        database: str = "analytics",
        table: str = "tracking_events",
        timeout_seconds: float = 5.0,
    ) -> None:
        self._base_url = f"http://{host}:{http_port}"
        self._user = user
        self._password = password
        self._database = database
        self._table = table
        self._timeout_seconds = timeout_seconds

    @property
    def table_name(self) -> str:
        return f"{self._database}.{self._table}"

    async def init_schema(self) -> None:
        await asyncio.to_thread(self._init_schema_sync)

    async def insert_events(self, events: Sequence[ProcessedTrackingEvent]) -> None:
        if not events:
            return
        await asyncio.to_thread(self._insert_events_sync, events)

    async def count_by_event_id(self, event_id: str) -> int:
        return await asyncio.to_thread(self._count_by_event_id_sync, event_id)

    async def count_by_param_value(self, param_name: str, param_value: str) -> int:
        return await asyncio.to_thread(
            self._count_by_param_value_sync,
            param_name,
            param_value,
        )

    def _init_schema_sync(self) -> None:
        self._post_query(f"CREATE DATABASE IF NOT EXISTS {self._database}")
        self._post_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                event_id String,
                user_id String,
                received_at DateTime64(3, 'UTC'),
                request_path String,
                client_ip Nullable(String),
                user_agent Nullable(String),
                params_json String,
                kafka_topic String,
                kafka_partition UInt32,
                kafka_offset UInt64,
                ingested_at DateTime64(3, 'UTC')
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(received_at)
            ORDER BY (received_at, user_id, event_id)
            """
        )

    def _insert_events_sync(self, events: Sequence[ProcessedTrackingEvent]) -> None:
        rows = [self._event_to_clickhouse_row(event) for event in events]
        body = "\n".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows
        )
        self._post_query(
            f"INSERT INTO {self.table_name} FORMAT JSONEachRow",
            body=body,
        )

    def _count_by_event_id_sync(self, event_id: str) -> int:
        escaped_event_id = event_id.replace("\\", "\\\\").replace("'", "\\'")
        response = self._post_query(
            f"SELECT count() FROM {self.table_name} WHERE event_id = '{escaped_event_id}'"
        )
        return int(response.strip() or "0")

    def _count_by_param_value_sync(self, param_name: str, param_value: str) -> int:
        escaped_param_name = param_name.replace("\\", "\\\\").replace("'", "\\'")
        escaped_param_value = param_value.replace("\\", "\\\\").replace("'", "\\'")
        response = self._post_query(
            f"""
            SELECT count()
            FROM {self.table_name}
            WHERE JSONExtractString(params_json, '{escaped_param_name}') = '{escaped_param_value}'
            """
        )
        return int(response.strip() or "0")

    def _event_to_clickhouse_row(self, event: ProcessedTrackingEvent) -> dict[str, object]:
        return {
            "event_id": event.event_id,
            "user_id": event.user_id,
            "received_at": self._format_clickhouse_datetime(event.received_at),
            "request_path": event.request_path,
            "client_ip": event.client_ip,
            "user_agent": event.user_agent,
            "params_json": event.params_json,
            "kafka_topic": event.kafka_topic,
            "kafka_partition": event.kafka_partition,
            "kafka_offset": event.kafka_offset,
            "ingested_at": self._format_clickhouse_datetime(event.ingested_at),
        }

    def _post_query(self, query: str, body: str | None = None) -> str:
        url = f"{self._base_url}/?{urlencode({'query': query})}"
        request = Request(
            url,
            data=body.encode("utf-8") if body is not None else b"",
            method="POST",
            headers=self._headers(),
        )

        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ClickHouseWriteError(f"ClickHouse HTTP error: {detail}") from exc
        except URLError as exc:
            raise ClickHouseWriteError(f"ClickHouse connection error: {exc}") from exc

    def _headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "X-ClickHouse-User": self._user,
        }
        if self._password:
            headers["X-ClickHouse-Key"] = self._password
        return headers

    @staticmethod
    def _format_clickhouse_datetime(value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
