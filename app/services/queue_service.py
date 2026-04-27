import asyncio
import logging
from asyncio import Queue, QueueFull
from collections.abc import Awaitable, Callable
from contextlib import suppress

from app.schemas.tracking import QueuedReportEvent

logger = logging.getLogger(__name__)


class QueueBackpressureError(Exception):
    pass


class EventQueueService:
    def __init__(
        self,
        maxsize: int,
        handler: Callable[[QueuedReportEvent], Awaitable[None]] | None = None,
    ) -> None:
        self._queue: Queue[QueuedReportEvent] = Queue(maxsize=maxsize)
        self._handler = handler or self._default_handler
        self._worker_task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def size(self) -> int:
        return self._queue.qsize()

    def enqueue_nowait(self, event: QueuedReportEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except QueueFull as exc:
            raise QueueBackpressureError("event queue is full") from exc

    async def start(self) -> None:
        if self._started:
            return

        self._started = True
        self._worker_task = asyncio.create_task(
            self._drain_forever(),
            name="tracking-report-queue-worker",
        )

    async def stop(self) -> None:
        if self._worker_task is None:
            return

        self._worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._worker_task
        self._worker_task = None
        self._started = False

    async def _drain_forever(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                await self._handler(event)
            finally:
                self._queue.task_done()

    async def _default_handler(self, event: QueuedReportEvent) -> None:
        logger.info(
            "Consumed queued report event path=%s client_ip=%s keys=%s",
            event.request_path,
            event.client_ip,
            ",".join(event.params.keys()),
        )
