import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from telethon import Button, TelegramClient, events
from telethon.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    InputUserDeactivatedError,
    PeerFloodError,
    UserIsBlockedError,
)
from telethon.tl.custom.message import Message

from app.config import Settings
from app.storage.hf_dataset import HFDataStore
from app.utils.media import send_payload, serialize_message
from app.worker import Priority, WorkerPool

logger = logging.getLogger(__name__)


class AssistantRuntime:
    MIN_ELAPSED_TIME_SECONDS = 0.1

    def __init__(
        self,
        settings: Settings,
        store: HFDataStore,
        assistant_id: str,
        session_path: str,
    ) -> None:
        self.settings = settings
        self.store = store
        self.assistant_id = assistant_id
        self.client = TelegramClient(session_path, settings.api_id, settings.api_hash)
        self.pending_actions: dict[int, dict[str, Any]] = {}
        # Each assistant has its own isolated worker pool.
        self._pool = WorkerPool(assistant_id)

    @property
    def data(self) -> dict[str, Any]:
        return self.store.get_data()["assistants"][self.assistant_id]

    def _admins(self) -> set[int]:
        admins = set(self.data.get("admins", []))
        admins.add(self.settings.super_admin_id)
        admins.add(self.data["owner_id"])
        return admins

    def _is_admin(self, user_id: int) -> bool:
        return user_id in self._admins()

    async def start(self) -> None:
        await self.client.start()
        await self._pool.start()
        self._register_handlers()

    async def stop(self) -> None:
        # Stop the pool first (drains / cancels in-flight tasks) then disconnect.
        await self._pool.stop()
        await self.client.disconnect()

    def _register_handlers(self) -> None:
        self.client.add_event_handler(self._on_new_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self._on_callback, events.CallbackQuery)

    def _ensure_user(self, user_id: int, premium: bool) -> dict[str, Any]:
        users = self.data.setdefault("users", {})
        user_key = str(user_id)
        now = datetime.now(timezone.utc).isoformat()
        if user_key not in users:
            users[user_key] = {
                "premium": premium,
                "blocked": False,
                "first_seen": now,
                "last_seen": now,
                "start_count": 0,
                "message_count": 0,
            }
        users[user_key]["premium"] = premium
        users[user_key]["last_seen"] = now
        return users[user_key]

    async def _log_user_message(self, event: events.NewMessage.Event, user_id: int) -> None:
        """Forward a user message to the log chat.

        Runs as a LOG-priority task in the worker pool so it never blocks the
        event handler.  Each Telegram API call is gated by ``api_sem``.
        """
        try:
            target = self.data.get("log_chat_id") or self.data["owner_id"]
            header = f"📩 Assistant {self.assistant_id}\nFrom: `{user_id}`"
            async with self._pool.api_sem:
                await self.client.send_message(target, header)
            async with self._pool.api_sem:
                forwarded = await event.message.forward_to(target)
            reply_map = self.data.setdefault("reply_map", {})
            reply_map[str(forwarded.id)] = user_id
            self.store.mark_dirty(self.assistant_id)
        except Exception:
            logger.exception(
                "Error logging message for assistant %s from user %s",
                self.assistant_id,
                user_id,
            )

    async def _apply_auto_reply(self, user_id: int, is_start: bool) -> None:
        """Send the configured auto-reply.

        Runs as a USER-priority task in the worker pool; gated by ``api_sem``.
        """
        payload = self.data.get("start_post") if is_start else self.data.get("setmsg")
        if payload:
            async with self._pool.api_sem:
                await send_payload(self.client, user_id, payload)

    async def _handle_admin_command(self, event: events.NewMessage.Event) -> bool:
        text = (event.raw_text or "").strip()
        user_id = event.sender_id or 0
        if not text.startswith("/"):
            return False

        parts = text.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if command == "/menu":
            await event.respond(
                "Assistant Menu",
                buttons=[
                    [Button.inline("SET START POST", data=f"asetstart:{self.assistant_id}")],
                    [Button.inline("SET MESSAGE", data=f"asetmsg:{self.assistant_id}")],
                    [Button.inline("STATS", data=f"astats:{self.assistant_id}")],
                    [Button.inline("BROADCAST", data=f"abroadcast:{self.assistant_id}")],
                ],
            )
            return True

        if command in {"/ban", "/unban", "/promote", "/demote"} and arg.isdigit():
            target = int(arg)
            if command == "/ban":
                self.data.setdefault("blocked_users", [])
                if target not in self.data["blocked_users"]:
                    self.data["blocked_users"].append(target)
                user_data = self._ensure_user(target, False)
                user_data["blocked"] = True
            elif command == "/unban":
                self.data["blocked_users"] = [x for x in self.data.get("blocked_users", []) if x != target]
                user_data = self._ensure_user(target, False)
                user_data["blocked"] = False
            elif command == "/promote":
                self.data.setdefault("admins", [])
                if target not in self.data["admins"]:
                    self.data["admins"].append(target)
            elif command == "/demote":
                self.data["admins"] = [x for x in self.data.get("admins", []) if x != target]
            self.store.mark_dirty(self.assistant_id)
            await event.reply("Updated.")
            return True

        if command in {"/ban", "/unban", "/promote", "/demote"}:
            await event.reply("Usage: /ban <id>, /unban <id>, /promote <id>, /demote <id>")
            return True

        return False

    async def _handle_reply_bridge(self, event: events.NewMessage.Event) -> bool:
        if not event.is_reply:
            return False
        if event.chat_id != (self.data.get("log_chat_id") or self.data["owner_id"]):
            return False

        reply = await event.get_reply_message()
        if not reply:
            return False

        user_id = self.data.get("reply_map", {}).get(str(reply.id))
        if not user_id:
            return False

        async with self._pool.api_sem:
            payload = await serialize_message(self.client, event.message)
        async with self._pool.api_sem:
            await send_payload(self.client, user_id, payload)
        return True

    async def _handle_pending_action(self, event: events.NewMessage.Event) -> bool:
        action = self.pending_actions.get(event.sender_id or 0)
        if not action:
            return False

        if (event.raw_text or "").strip().lower() == "cancel":
            self.pending_actions.pop(event.sender_id or 0, None)
            await event.reply("Cancelled.")
            return True

        if action["type"] in {"set_start", "set_msg"}:
            async with self._pool.api_sem:
                payload = await serialize_message(self.client, event.message)
            if action["type"] == "set_start":
                self.data["start_post"] = payload
            else:
                self.data["setmsg"] = payload
            self.store.mark_dirty(self.assistant_id)
            self.pending_actions.pop(event.sender_id or 0, None)
            await event.reply("Saved.")
            return True

        if action["type"] == "broadcast_prepare":
            async with self._pool.api_sem:
                payload = await serialize_message(self.client, event.message)
            self.pending_actions[event.sender_id or 0] = {"type": "broadcast_confirm", "payload": payload}
            await event.reply(
                "Are you sure you want to broadcast to all users?",
                buttons=[
                    [Button.inline("YES", data=f"abcyes:{self.assistant_id}")],
                    [Button.inline("CANCEL", data=f"abcancel:{self.assistant_id}")],
                ],
            )
            return True

        return False

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        if event.sender_id is None:
            return

        self.data["last_active_at"] = datetime.now(timezone.utc).isoformat()
        self.store.mark_dirty(self.assistant_id)

        if self._is_admin(event.sender_id):
            if await self._handle_pending_action(event):
                return
            if await self._handle_admin_command(event):
                return
            await self._handle_reply_bridge(event)
            return

        if not event.is_private:
            return

        if event.sender_id in self.data.get("blocked_users", []):
            return

        sender = await event.get_sender()
        premium = bool(getattr(sender, "premium", False))
        user_data = self._ensure_user(event.sender_id, premium)
        user_data["message_count"] += 1
        self.data.setdefault("stats", {"total_starts": 0, "total_messages": 0})
        self.data["stats"]["total_messages"] += 1

        is_start = (event.raw_text or "").strip().startswith("/start")
        if is_start:
            user_data["start_count"] += 1
            self.data["stats"]["total_starts"] += 1

        self.store.mark_dirty(self.assistant_id)

        # Hand off to the priority worker pool so the event handler returns
        # immediately.  USER replies are processed before LOG forwarding.
        self._pool.enqueue_nowait(
            self._apply_auto_reply(event.sender_id, is_start), Priority.USER
        )
        self._pool.enqueue_nowait(
            self._log_user_message(event, event.sender_id), Priority.LOG
        )

    def _stats_text(self) -> str:
        users = self.data.get("users", {})
        total = len(users)
        premium = sum(1 for u in users.values() if u.get("premium"))
        blocked = len(self.data.get("blocked_users", []))
        non_premium = total - premium
        admins = sorted(self._admins())
        stats = self.data.get("stats", {})
        return (
            f"Total users: {total}\n"
            f"Premium users: {premium}\n"
            f"Non-premium users: {non_premium}\n"
            f"Blocked users: {blocked}\n"
            f"Total admins: {len(admins)}\n"
            f"Admin IDs: {', '.join(map(str, admins))}\n"
            f"Total /start count: {stats.get('total_starts', 0)}\n"
            f"Total messages count: {stats.get('total_messages', 0)}\n"
            f"Worker pool — queue: {self._pool.queue_depth()} "
            f"| active workers: {self._pool.active_workers()}"
        )

    async def _broadcast_progress(
        self,
        status_msg: Message,
        counters: dict[str, int],
        total: int,
        started: float,
    ) -> None:
        """Periodically edit the status message with broadcast progress."""
        last_pct = -1
        while True:
            await asyncio.sleep(3)
            done = counters["success"] + counters["failed"]
            pct = int(done / total * 100) if total else 100
            if pct != last_pct:
                last_pct = pct
                elapsed = max(time.time() - started, self.MIN_ELAPSED_TIME_SECONDS)
                speed = done / elapsed
                remaining = total - done
                eta = int(remaining / speed) if speed > 0 else 0
                try:
                    await status_msg.edit(
                        f"{pct}% completed\n"
                        f"Sent: {done}/{total}\n"
                        f"ETA: {eta}s\n"
                        f"Speed: {speed:.2f} msg/sec"
                    )
                except Exception:
                    pass

    async def _broadcast(self, admin_id: int, payload: dict[str, Any], status_msg: Message) -> None:
        """Queue-based broadcast: one coroutine per recipient, processed by the pool.

        Instead of spawning thousands of Tasks via ``asyncio.gather``, a single
        lightweight coroutine is enqueued per recipient into the dedicated
        broadcast queue.  Workers drain it concurrently, gated by ``api_sem``.
        Memory usage stays bounded because coroutines (unlike Tasks) are cheap
        and sit dormant in the queue until a worker picks them up.

        A ``remaining`` counter decremented in each coroutine's ``finally``
        block fires an ``asyncio.Event`` when all sends have finished, at which
        point the summary is written and ``broadcast_lock`` is released.
        """
        async with self._pool.broadcast_lock:
            blocked_users = set(self.data.get("blocked_users", []))
            users = [
                int(k)
                for k in self.data.get("users", {})
                if int(k) not in blocked_users
            ]
            total = len(users)
            if total == 0:
                await status_msg.edit("No users to broadcast.")
                return

            counters: dict[str, int] = {"success": 0, "failed": 0, "blocked": 0}
            # List wrapper allows mutation from the nested coroutine without
            # nonlocal — safe because asyncio is single-threaded.
            remaining = [total]
            done_event = asyncio.Event()
            started = time.time()

            async def send_one(user_id: int) -> None:
                try:
                    async with self._pool.api_sem:
                        await send_payload(self.client, user_id, payload)
                    counters["success"] += 1
                except FloodWaitError as e:
                    await asyncio.sleep(max(1, int(e.seconds)))
                    try:
                        async with self._pool.api_sem:
                            await send_payload(self.client, user_id, payload)
                        counters["success"] += 1
                    except Exception:
                        counters["failed"] += 1
                except (UserIsBlockedError, InputUserDeactivatedError, ChatWriteForbiddenError):
                    counters["blocked"] += 1
                    counters["failed"] += 1
                    if user_id not in self.data.setdefault("blocked_users", []):
                        self.data["blocked_users"].append(user_id)
                        self.store.mark_dirty(self.assistant_id)
                except PeerFloodError:
                    await asyncio.sleep(2)
                    counters["failed"] += 1
                except Exception:
                    counters["failed"] += 1
                finally:
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        done_event.set()

            # Enqueue one coroutine per recipient — no mass Task creation.
            for uid in users:
                self._pool.enqueue_nowait(send_one(uid), Priority.BROADCAST)

            progress_task = asyncio.create_task(
                self._broadcast_progress(status_msg, counters, total, started)
            )
            try:
                await done_event.wait()
            finally:
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass

            duration = int(time.time() - started)
            self.store.mark_dirty(self.assistant_id)
            await status_msg.edit(
                "Broadcast Completed ✅\n\n"
                f"Total Users: {total}\n"
                f"Sent: {counters['success']}\n"
                f"Failed: {counters['failed']}\n"
                f"Blocked: {counters['blocked']}\n"
                f"Time Taken: {duration} seconds"
            )

    async def _on_callback(self, event: events.CallbackQuery.Event) -> None:
        sender_id = event.sender_id or 0
        if not self._is_admin(sender_id):
            await event.answer("Not allowed", alert=True)
            return

        cb_data = (event.data or b"").decode("utf-8")

        if cb_data == f"acancel:{self.assistant_id}":
            self.pending_actions.pop(sender_id, None)
            await event.edit("Cancelled.")
            return

        if cb_data == f"asetstart:{self.assistant_id}":
            self.pending_actions[sender_id] = {"type": "set_start"}
            await event.edit(
                "Send STARTPOST message now, or press Cancel.",
                buttons=[[Button.inline("❌ Cancel", data=f"acancel:{self.assistant_id}")]],
            )
            return

        if cb_data == f"asetmsg:{self.assistant_id}":
            self.pending_actions[sender_id] = {"type": "set_msg"}
            await event.edit(
                "Send SETMSG message now, or press Cancel.",
                buttons=[[Button.inline("❌ Cancel", data=f"acancel:{self.assistant_id}")]],
            )
            return

        if cb_data == f"astats:{self.assistant_id}":
            await event.edit("Processing...")
            await event.edit(self._stats_text())
            return

        if cb_data == f"abroadcast:{self.assistant_id}":
            self.pending_actions[sender_id] = {"type": "broadcast_prepare"}
            await event.edit(
                "Send broadcast message now (text/media), or press Cancel.",
                buttons=[[Button.inline("❌ Cancel", data=f"acancel:{self.assistant_id}")]],
            )
            return

        if cb_data == f"abcancel:{self.assistant_id}":
            self.pending_actions.pop(sender_id, None)
            await event.edit("Broadcast cancelled.")
            return

        if cb_data == f"abcyes:{self.assistant_id}":
            action = self.pending_actions.get(sender_id)
            if not action or action.get("type") != "broadcast_confirm":
                await event.answer("No pending broadcast", alert=True)
                return
            payload = action["payload"]
            self.pending_actions.pop(sender_id, None)
            msg = await event.edit("Broadcast started...")
            # Run the broadcast coordinator as a plain background task — it is
            # not a pool item itself, but it enqueues individual per-recipient
            # coroutines into the broadcast queue.
            asyncio.create_task(
                self._broadcast(sender_id, payload, msg),
                name=f"broadcast-{self.assistant_id}-{sender_id}",
            )
            return
