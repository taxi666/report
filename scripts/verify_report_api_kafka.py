import asyncio
import json
import os
import sys
from pathlib import Path
from time import monotonic
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import uuid4

from aiokafka import AIOKafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


TOPIC = "tracking_events"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
REPORT_URL = os.getenv("REPORT_URL", "http://127.0.0.1:8000/report")


async def main() -> None:
    trace_id = str(uuid4())
    user_id = f"user-{trace_id[:8]}"
    query_params = {
        "user_id": user_id,
        "trace_id": trace_id,
        "prod": "pc_his",
        "from": "pc_web",
        "json": "1",
    }

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"report-api-verifier-{trace_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda value: value.decode("utf-8") if value is not None else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value is not None else None,
    )

    await consumer.start()
    try:
        deadline = monotonic() + 10
        while not consumer.assignment():
            if monotonic() >= deadline:
                raise TimeoutError("consumer assignment timed out")
            await asyncio.sleep(0.2)

        query_string = urlencode(query_params)
        with urlopen(f"{REPORT_URL}?{query_string}") as response:
            body = response.read().decode("utf-8")
            status_code = response.status

        deadline = monotonic() + 20
        message = None
        while monotonic() < deadline:
            candidate = await asyncio.wait_for(consumer.getone(), timeout=5)
            candidate_trace_id = None
            if isinstance(candidate.value, dict):
                params = candidate.value.get("params") or {}
                if isinstance(params, dict):
                    candidate_trace_id = params.get("trace_id")

            if candidate_trace_id == trace_id:
                message = candidate
                break

        if message is None:
            raise TimeoutError(f"did not consume trace_id={trace_id} within timeout")

        print(
            json.dumps(
                {
                    "request": {
                        "status_code": status_code,
                        "body": json.loads(body),
                        "query_params": query_params,
                    },
                    "kafka_message": {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key,
                        "value": message.value,
                    },
                },
                ensure_ascii=False,
            )
        )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
