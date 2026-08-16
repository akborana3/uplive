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

from telethon.errors import FloodWaitError, PeerFloodError

logger = logging.getLogger(__name__)


class Priority(int, enum.Enum):
    """Task priority.  Lower value = processed first."""

    BROADCAST = 1
    USER = 2
    LOG = 3


class AdaptiveLimiter:
    """Self-tuning concurrency limiter for Telegram API calls, AIMD-style.

    Bot accounts (Bot API, ~30 msg/sec soft ceiling to distinct users) can
    take real concurrency, but the *exact* safe number varies by bot, load
    pattern, and what Telegram is doing that day. Rather than guess one fixed
    number, this creeps concurrency up slowly on clean send streaks and cuts
    it hard the moment Telegram signals it's unhappy (FloodWaitError /
    PeerFloodError), then eases back up again — the same idea TCP congestion
    control uses. Each assistant finds its own safe ceiling automatically.

    Drop-in replacement for ``asyncio.Semaphore`` at call sites: used as
    ``async with limiter:``. Success/failure are inferred from whether the
    wrapped block raised a flood-type error.
    """

    def __init__(self, initial: int, min_limit: int, max_limit: int, success_streak: int = 20) -> None:
        self._limit = initial
        self._min = min_limit
        self._max = max_limit
        self._success_streak_target = success_streak
        self._sem = asyncio.Semaphore(initial)
        self._resize_lock = asyncio.Lock()
        self._consecutive_success = 0

    @property
    def current_limit(self) -> int:
        return self._limit

    async def __aenter__(self) -> "AdaptiveLimiter":
        await self._sem.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self._sem.release()
        if exc_type is not None and issubclass(exc_type, (FloodWaitError, PeerFloodError)):
            await self._on_flood()
        elif exc_type is None:
            await self._on_success()
        return False  # never suppress the original exception

    async def _on_success(self) -> None:
        async with self._resize_lock:
            self._consecutive_success += 1
            if self._consecutive_success >= self._success_streak_target and self._limit < self._max:
                self._limit += 1
                self._sem.release()  # one extra permit in circulation = +1 capacity
                self._consecutive_success = 0

    async def _on_flood(self) -> None:
        async with self._resize_lock:
            self._consecutive_success = 0
            new_limit = max(self._min, self._limit // 2)
            if new_limit >= self._limit:
                return
            diff = self._limit - new_limit
            self._limit = new_limit
            logger.warning(
                "AdaptiveLimiter: flood signal received, cutting concurrency %d -> %d",
                self._limit + diff,
                self._limit,
            )
            for _ in range(diff):
                # Permanently remove one permit from circulation by acquiring
                # it and never releasing it back (until we grow again).
                asyncio.create_task(self._sem.acquire())


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

    RESERVED_USER_WORKERS: int = 5    # always-on workers dedicated to user/log messages
    MAX_FLEXIBLE_WORKERS: int = 10    # flexible workers that prioritise broadcast items
    SCALE_UP_THRESHOLD: int = 5       # bc_queue depth that triggers a new flexible worker
    SCALE_DOWN_IDLE: float = 30.0     # idle seconds before a flexible/extra worker exits

    # Extra (beyond RESERVED_USER_WORKERS) user/log workers, scaled dynamically
    # on msg_queue depth — same idle-scale-down pattern as flexible workers.
    MAX_EXTRA_USER_WORKERS: int = 15
    MSG_SCALE_UP_THRESHOLD: int = 25  # msg_queue depth that triggers an extra worker

    # AdaptiveLimiter bounds for concurrent Telegram API calls. These are bot
    # accounts (Bot API), not personal userbot sessions, so the ceiling can be
    # much higher than a user account's flood limits — 30/sec is roughly
    # Telegram's own soft ceiling for a bot messaging distinct users, so we
    # cap just under that and let AIMD find the real safe number per bot.
    API_CONCURRENCY_INITIAL: int = 12
    API_CONCURRENCY_MIN: int = 2
    API_CONCURRENCY_MAX: int = 28

    # Backpressure: past this many pending user/log items, non-essential LOG
    # (forward-to-owner) tasks are shed so the queue can't grow unbounded —
    # the actual user-facing reply (USER priority) is never dropped.
    MAX_MSG_QUEUE_FOR_LOG: int = 8000

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
        # Acquired by callers around each Telegram API call (not around whole
        # tasks). Self-tuning instead of a fixed cap — see AdaptiveLimiter.
        self.api_sem = AdaptiveLimiter(
            initial=self.API_CONCURRENCY_INITIAL,
            min_limit=self.API_CONCURRENCY_MIN,
            max_limit=self.API_CONCURRENCY_MAX,
        )
        # Must be held for the full duration of a broadcast.
        self.broadcast_lock = asyncio.Lock()
        self._reserved_workers: list[asyncio.Task[None]] = []
        self._extra_user_workers: list[asyncio.Task[None]] = []
        self._flexible_workers: list[asyncio.Task[None]] = []
        self._scaler_task: asyncio.Task[None] | None = None
        self._running = False
        self._seq = 0
        self._dropped_log_count = 0

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

        Backpressure: once the msg_queue is very deep, new LOG-priority items
        (the log-chat forward copy — not the user-facing reply) are dropped
        instead of queued, so a traffic storm can't grow memory unbounded.
        USER-priority replies are always queued regardless of depth.
        """
        if priority is Priority.LOG and self._msg_queue.qsize() >= self.MAX_MSG_QUEUE_FOR_LOG:
            coro.close()  # never-awaited coroutine must be closed to avoid a warning
            self._dropped_log_count += 1
            if self._dropped_log_count % 500 == 1:
                logger.warning(
                    "WorkerPool[%s] shedding LOG tasks under backpressure "
                    "(msg_queue depth=%d, dropped so far=%d)",
                    self.assistant_id,
                    self._msg_queue.qsize(),
                    self._dropped_log_count,
                )
            return

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
        return (
            sum(1 for w in self._reserved_workers if not w.done())
            + sum(1 for w in self._extra_user_workers if not w.done())
            + sum(1 for w in self._flexible_workers if not w.done())
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
            "WorkerPool[%s] started — reserved=%d, max_extra=%d, max_flexible=%d, "
            "api_concurrency=%d (adaptive, max=%d)",
            self.assistant_id,
            self.RESERVED_USER_WORKERS,
            self.MAX_EXTRA_USER_WORKERS,
            self.MAX_FLEXIBLE_WORKERS,
            self.api_sem.current_limit,
            self.API_CONCURRENCY_MAX,
        )

    async def stop(self) -> None:
        self._running = False
        if self._scaler_task and not self._scaler_task.done():
            self._scaler_task.cancel()
            try:
                await self._scaler_task
            except asyncio.CancelledError:
                pass
        all_workers = (
            list(self._reserved_workers)
            + list(self._extra_user_workers)
            + list(self._flexible_workers)
        )
        for w in all_workers:
            w.cancel()
        if all_workers:
            await asyncio.gather(*all_workers, return_exceptions=True)
        self._reserved_workers.clear()
        self._extra_user_workers.clear()
        self._flexible_workers.clear()
        logger.info("WorkerPool[%s] stopped", self.assistant_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_reserved_worker(self, wid: int) -> asyncio.Task[None]:
        task: asyncio.Task[None] = asyncio.create_task(
            self._user_worker(wid, scales_down=False),
            name=f"worker-{self.assistant_id}-reserved-{wid}",
        )
        self._reserved_workers.append(task)
        return task

    def _add_extra_user_worker(self) -> asyncio.Task[None]:
        wid = len(self._extra_user_workers)
        task: asyncio.Task[None] = asyncio.create_task(
            self._user_worker(wid, scales_down=True),
            name=f"worker-{self.assistant_id}-extra-{wid}",
        )
        self._extra_user_workers.append(task)
        return task

    def _add_flexible_worker(self) -> asyncio.Task[None]:
        wid = len(self._flexible_workers)
        task: asyncio.Task[None] = asyncio.create_task(
            self._flexible_worker(wid),
            name=f"worker-{self.assistant_id}-flex-{wid}",
        )
        self._flexible_workers.append(task)
        return task

    async def _user_worker(self, worker_id: int, scales_down: bool) -> None:
        """Drains only the user/log message queue.  Never touches broadcast items.

        Reserved workers (``scales_down=False``) run forever. Extra workers
        spun up by the scaler under load exit after ``SCALE_DOWN_IDLE``
        seconds of no work, same pattern as flexible broadcast workers.
        """
        idle_since: float | None = None
        while self._running:
            try:
                item = await asyncio.wait_for(self._msg_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                if scales_down:
                    if idle_since is None:
                        idle_since = time.monotonic()
                    elif time.monotonic() - idle_since >= self.SCALE_DOWN_IDLE:
                        logger.debug(
                            "WorkerPool[%s] extra-user-worker-%d scaling down after %.0fs idle",
                            self.assistant_id,
                            worker_id,
                            self.SCALE_DOWN_IDLE,
                        )
                        return
                continue
            except asyncio.CancelledError:
                break

            idle_since = None
            _, _, coro = item
            try:
                await coro
            except Exception:
                logger.exception(
                    "WorkerPool[%s] user-worker-%d: unhandled exception",
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
        """Periodically add workers when a queue is running deep.

        Broadcast side: flexible workers scale on ``_bc_queue`` depth (as
        before).  User/log side: extra user workers scale on ``_msg_queue``
        depth — this is what keeps replies flowing during a burst of
        messages instead of being stuck behind only the fixed reserved pool.
        """
        while self._running:
            await asyncio.sleep(2)

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

            self._extra_user_workers = [w for w in self._extra_user_workers if not w.done()]
            msg_depth = self._msg_queue.qsize()
            active_extra = len(self._extra_user_workers)
            if msg_depth >= self.MSG_SCALE_UP_THRESHOLD and active_extra < self.MAX_EXTRA_USER_WORKERS:
                self._add_extra_user_worker()
                logger.debug(
                    "WorkerPool[%s] scaled up → %d extra user workers (msg queue depth=%d)",
                    self.assistant_id,
                    active_extra + 1,
                    msg_depth,
                )
