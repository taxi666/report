import asyncio
import json
import os
import sys
from pathlib import Path
from time import monotonic
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import get_settings
from app.writer import ClickHouseWriter


REPORT_URL = os.getenv("REPORT_URL", "http://127.0.0.1:8000/report")


async def main() -> None:
    settings = get_settings()
    trace_id = str(uuid4())
    user_id = f"user-{trace_id[:8]}"
    query_params = {
        "user_id": user_id,
        "trace_id": trace_id,
        "prod": "pc_his",
        "from": "pc_web",
        "json": "1",
        "event_name": "e2e_clickhouse_test",
    }

    writer = ClickHouseWriter(
        host=settings.clickhouse_host,
        http_port=settings.clickhouse_http_port,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        table=settings.clickhouse_tracking_table,
        timeout_seconds=settings.clickhouse_connect_timeout_seconds,
    )
    await writer.init_schema()

    query_string = urlencode(query_params)
    with urlopen(f"{REPORT_URL}?{query_string}") as response:
        response_body = json.loads(response.read().decode("utf-8"))
        status_code = response.status

    deadline = monotonic() + 30
    count = 0
    while monotonic() < deadline:
        count = await writer.count_by_param_value("trace_id", trace_id)
        if count > 0:
            break
        await asyncio.sleep(1)

    if count <= 0:
        raise TimeoutError(f"trace_id={trace_id} was not written to ClickHouse")

    print(
        json.dumps(
            {
                "request": {
                    "status_code": status_code,
                    "body": response_body,
                    "query_params": query_params,
                },
                "clickhouse": {
                    "table": writer.table_name,
                    "trace_id": trace_id,
                    "matched_rows": count,
                },
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
