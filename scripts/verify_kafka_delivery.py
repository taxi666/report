import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.producer import KafkaProducerClient


async def main() -> None:
    payload = {
        "user_id": "user-1001",
        "event_name": "report_test",
        "source": "workspace_verification",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "properties": {
            "prod": "pc_his",
            "from": "pc_web",
            "json": "1",
        },
    }

    producer = KafkaProducerClient(
        bootstrap_servers="127.0.0.1:9092",
        topic="tracking_events",
        client_id="tracking-api-verifier",
    )

    await producer.start()
    try:
        metadata = await producer.send_event(payload)
    finally:
        await producer.stop()

    print(
        json.dumps(
            {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
                "payload": payload,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
