"""
Per-assistant async worker pool with dual-queue design, API rate-limiting semaphore,
dynamic flexible-worker scaling, and a broadcast lock.

Priority levels (lower value = processed first):
  BROADCAST = 1  – individual per-recipient sends during a broadcast
  USER      = 2  – outgoing auto-replies to individual users
  LOG       = 3  – forwarding user messages to the log chat

Architecture
------------
Two queues serve different worker types:

  _bc_queue  (asyncio.Queue)
      Holds one lightweight coroutine per broadcast recipient.
      Fed by the broadcast coordinator; drained exclusively by flexible workers.
      No mass Task creation — coroutines are cheap and sit idle until a worker
      picks them up.

  _msg_queue (asyncio.PriorityQueue)
      Holds USER and LOG tasks.  Drained by reserved workers (always) and
      by flexible workers when _bc_queue is empty.

Worker types:
  Reserved user workers  – always drain _msg_queue; never touch _bc_queue.
                           Guarantees bot responsiveness even during a broadcast.
  Flexible workers       – prefer _bc_queue; fall back to _msg_queue.
                           Scaled up/down dynamically by the scaler task.
"""

import asyncio
import enum
import logging
import time
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class Priority(int, enum.Enum):
    """Task priority.  Lower value = processed first."""

    BROADCAST = 1
    USER = 2
    LOG = 3


class WorkerPool:
    """
    Isolated async worker pool for a single assistant bot.

    Features
    --------
    - Dual-queue design: broadcast recipients feed a dedicated FIFO queue;
      user/log tasks share a priority queue.  Reserved workers exclusively
      drain the user/log queue, preventing message starvation during a
      broadcast.
    - Flexible workers prefer the broadcast queue and fall back to the
      user/log queue when idle.  They are dynamically scaled up when the
      broadcast queue is deep and scale down after SCALE_DOWN_IDLE seconds
      of inactivity.
    - ``api_sem`` semaphore caps total concurrent Telegram API calls.
    - ``broadcast_lock`` prevents two simultaneous broadcasts.
    """

    RESERVED_USER_WORKERS: int = 3   # always-on workers dedicated to user/log messages
    MAX_FLEXIBLE_WORKERS: int = 7    # flexible workers that prioritise broadcast items
    SCALE_UP_THRESHOLD: int = 5      # bc_queue depth that triggers a new flexible worker
    SCALE_DOWN_IDLE: float = 30.0    # idle seconds before a flexible worker exits
    API_CONCURRENCY: int = 8         # max simultaneous Telegram API calls

    def __init__(self, assistant_id: str) -> None:
        self.assistant_id = assistant_id
        # Broadcast items: (seq, coro) — one entry per recipient.
        self._bc_queue: asyncio.Queue[tuple[int, Coroutine[Any, Any, Any]]] = (
            asyncio.Queue()
        )
        # User/Log items: (priority_value, seq, coro).
        self._msg_queue: asyncio.PriorityQueue[
            tuple[int, int, Coroutine[Any, Any, Any]]
        ] = asyncio.PriorityQueue()
        # Acquired by callers around each Telegram API call (not around whole tasks).
        self.api_sem = asyncio.Semaphore(self.API_CONCURRENCY)
        # Must be held for the full duration of a broadcast.
        self.broadcast_lock = asyncio.Lock()
        self._reserved_workers: list[asyncio.Task[None]] = []
        self._flexible_workers: list[asyncio.Task[None]] = []
        self._scaler_task: asyncio.Task[None] | None = None
        self._running = False
        self._seq = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue_nowait(
        self,
        coro: Coroutine[Any, Any, Any],
        priority: Priority = Priority.USER,
    ) -> None:
        """Schedule *coro* immediately (non-blocking).

        BROADCAST tasks are placed on the dedicated broadcast queue; USER and
        LOG tasks go on the shared priority queue.  The *seq* counter provides
        FIFO ordering within the same priority level.
        """
        self._seq += 1
        if priority is Priority.BROADCAST:
            self._bc_queue.put_nowait((self._seq, coro))
        else:
            self._msg_queue.put_nowait((priority.value, self._seq, coro))

    def queue_depth(self) -> int:
        """Return the total number of tasks waiting across both queues."""
        return self._bc_queue.qsize() + self._msg_queue.qsize()

    def active_workers(self) -> int:
        """Return the number of worker tasks that have not yet finished."""
        return sum(1 for w in self._reserved_workers if not w.done()) + sum(
            1 for w in self._flexible_workers if not w.done()
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        for i in range(self.RESERVED_USER_WORKERS):
            self._add_reserved_worker(i)
        # Start one flexible worker immediately so broadcast items are
        # processed without waiting for the first scaler tick.
        self._add_flexible_worker()
        self._scaler_task = asyncio.create_task(
            self._scaler(), name=f"scaler-{self.assistant_id}"
        )
        logger.info(
            "WorkerPool[%s] started — reserved=%d, max_flexible=%d, api_concurrency=%d",
            self.assistant_id,
            self.RESERVED_USER_WORKERS,
            self.MAX_FLEXIBLE_WORKERS,
            self.API_CONCURRENCY,
        )

    async def stop(self) -> None:
        self._running = False
        if self._scaler_task and not self._scaler_task.done():
            self._scaler_task.cancel()
            try:
                await self._scaler_task
            except asyncio.CancelledError:
                pass
        all_workers = list(self._reserved_workers) + list(self._flexible_workers)
        for w in all_workers:
            w.cancel()
        if all_workers:
            await asyncio.gather(*all_workers, return_exceptions=True)
        self._reserved_workers.clear()
        self._flexible_workers.clear()
        logger.info("WorkerPool[%s] stopped", self.assistant_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_reserved_worker(self, wid: int) -> asyncio.Task[None]:
        task: asyncio.Task[None] = asyncio.create_task(
            self._user_worker(wid),
            name=f"worker-{self.assistant_id}-reserved-{wid}",
        )
        self._reserved_workers.append(task)
        return task

    def _add_flexible_worker(self) -> asyncio.Task[None]:
        wid = len(self._flexible_workers)
        task: asyncio.Task[None] = asyncio.create_task(
            self._flexible_worker(wid),
            name=f"worker-{self.assistant_id}-flex-{wid}",
        )
        self._flexible_workers.append(task)
        return task

    async def _user_worker(self, worker_id: int) -> None:
        """Drains only the user/log message queue.  Never touches broadcast items."""
        while self._running:
            try:
                item = await asyncio.wait_for(self._msg_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            _, _, coro = item
            try:
                await coro
            except Exception:
                logger.exception(
                    "WorkerPool[%s] reserved-worker-%d: unhandled exception",
                    self.assistant_id,
                    worker_id,
                )
            finally:
                self._msg_queue.task_done()

    async def _flexible_worker(self, worker_id: int) -> None:
        """Prefers broadcast queue; falls back to user/log queue when broadcast is idle.

        Checks _bc_queue non-blocking first on every iteration so it switches
        to broadcast work within one loop cycle after items arrive.  Falls back
        to a 1-second blocking wait on _msg_queue to avoid busy-spinning.
        Scales down after SCALE_DOWN_IDLE seconds of complete idleness.
        """
        idle_since: float | None = None
        while self._running:
            coro: Coroutine[Any, Any, Any] | None = None
            is_bc = False

            # 1. Prefer broadcast queue (non-blocking).
            try:
                _, coro = self._bc_queue.get_nowait()
                is_bc = True
            except asyncio.QueueEmpty:
                pass

            # 2. Fall back to user/log queue when no broadcast work is queued.
            if coro is None:
                try:
                    item = await asyncio.wait_for(self._msg_queue.get(), timeout=1.0)
                    _, _, coro = item
                except asyncio.TimeoutError:
                    # Both queues were empty; update the idle timer.
                    if idle_since is None:
                        idle_since = time.monotonic()
                    active_flex = sum(
                        1 for w in self._flexible_workers if not w.done()
                    )
                    if (
                        active_flex > 0
                        and time.monotonic() - idle_since >= self.SCALE_DOWN_IDLE
                    ):
                        logger.debug(
                            "WorkerPool[%s] flex-worker-%d scaling down after %.0fs idle",
                            self.assistant_id,
                            worker_id,
                            self.SCALE_DOWN_IDLE,
                        )
                        break
                    continue
                except asyncio.CancelledError:
                    break

            idle_since = None
            try:
                await coro
            except Exception:
                logger.exception(
                    "WorkerPool[%s] flex-worker-%d: unhandled exception",
                    self.assistant_id,
                    worker_id,
                )
            finally:
                if is_bc:
                    self._bc_queue.task_done()
                else:
                    self._msg_queue.task_done()

    async def _scaler(self) -> None:
        """Periodically add a flexible worker when the broadcast queue is deep."""
        while self._running:
            await asyncio.sleep(2)
            # Remove references to completed flexible workers.
            self._flexible_workers = [w for w in self._flexible_workers if not w.done()]
            bc_depth = self._bc_queue.qsize()
            active_flex = len(self._flexible_workers)
            if bc_depth >= self.SCALE_UP_THRESHOLD and active_flex < self.MAX_FLEXIBLE_WORKERS:
                self._add_flexible_worker()
                logger.debug(
                    "WorkerPool[%s] scaled up → %d flexible workers (bc queue depth=%d)",
                    self.assistant_id,
                    active_flex + 1,
                    bc_depth,
                )
