import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from uuid import uuid4

from aiokafka import AIOKafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.producer import KafkaProducerClient


TOPIC = "tracking_events"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"


async def main() -> None:
    event_id = str(uuid4())
    user_id = f"user-{event_id[:8]}"
    payload = {
        "event_id": event_id,
        "user_id": user_id,
        "event_name": "report_test",
        "source": "workspace_roundtrip_verification",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "properties": {
            "prod": "pc_his",
            "from": "pc_web",
            "json": "1",
        },
    }

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"tracking-verifier-{event_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda value: value.decode("utf-8") if value is not None else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value is not None else None,
    )

    producer = KafkaProducerClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        client_id="tracking-api-roundtrip-verifier",
    )

    await consumer.start()
    await producer.start()

    try:
        deadline = asyncio.get_running_loop().time() + 10
        while not consumer.assignment():
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("consumer assignment timed out")
            await asyncio.sleep(0.2)

        metadata = await producer.send_event(payload)
        deadline = monotonic() + 20
        message = None
        while monotonic() < deadline:
            candidate = await asyncio.wait_for(consumer.getone(), timeout=5)
            candidate_event_id = None
            if isinstance(candidate.value, dict):
                candidate_event_id = candidate.value.get("event_id")

            if candidate_event_id == event_id:
                message = candidate
                break

        if message is None:
            raise TimeoutError(f"did not consume event_id={event_id} within timeout")

        print(
            json.dumps(
                {
                    "produced": {
                        "topic": metadata.topic,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                        "key": user_id,
                        "payload": payload,
                    },
                    "consumed": {
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
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
