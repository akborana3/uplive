import base64
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from telethon import Button, TelegramClient, events

from app.config import Settings
from app.session_manager import SessionManager
from app.storage.hf_dataset import HFDataStore


class MasterController:
    def __init__(self, settings: Settings, store: HFDataStore, sessions: SessionManager) -> None:
        self.settings = settings
        self.store = store
        self.sessions = sessions
        self.client = TelegramClient(settings.master_session_file, settings.api_id, settings.api_hash)
        self.pending_connect: dict[int, dict[str, Any]] = {}

    def _master_data(self) -> dict[str, Any]:
        return self.store.get_data()["master"]

    def _is_admin(self, user_id: int) -> bool:
        if user_id == self.settings.super_admin_id:
            return True
        return user_id in self._master_data().get("admins", [])

    def _is_banned(self, user_id: int) -> bool:
        return user_id in self._master_data().get("banned", [])

    async def start(self) -> None:
        await self.client.start()
        self.client.add_event_handler(self._on_new_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self._on_callback, events.CallbackQuery)

    async def run(self) -> None:
        await self.client.run_until_disconnected()

    async def stop(self) -> None:
        await self.client.disconnect()

    def _main_menu(self) -> list[list[Button]]:
        return [
            [Button.inline("CONNECT BOT", data="m_connect")],
            [Button.inline("MY ALL BOTS", data="m_mybots")],
            [Button.inline("DISCONNECT", data="m_disconnect")],
            [Button.inline("ADMIN PANEL", data="m_admin")],
        ]

    def _ensure_master_user(self, user_id: int) -> None:
        users = self._master_data().setdefault("users", {})
        users.setdefault(str(user_id), {"joined_at": datetime.now(timezone.utc).isoformat()})
        self.store.mark_dirty()

    def _bot_display_name(self, aid: str, data: dict[str, Any]) -> str:
        """Return @username when available, otherwise fall back to the numeric ID."""
        username = data.get("bot_username", "")
        return f"@{username}" if username else f"Bot {aid}"

    async def _show_my_bots(self, event: events.common.EventCommon, owner_id: int) -> None:
        assistants = self.store.get_data().get("assistants", {})
        mine = [
            (aid, data)
            for aid, data in assistants.items()
            if data.get("owner_id") == owner_id or self._is_admin(owner_id)
        ]
        if not mine:
            await event.reply("No assistant bots connected.")
            return
        buttons = [
            [Button.inline(self._bot_display_name(aid, data), data=f"m_bstat:{aid}")]
            for aid, data in mine
        ]
        await event.reply("Your assistant bots:", buttons=buttons)

    def _assistant_stats(self, aid: str, data: dict[str, Any]) -> str:
        users = data.get("users", {})
        total = len(users)
        premium = sum(1 for u in users.values() if u.get("premium"))
        blocked = len(data.get("blocked_users", []))
        non_premium = total - premium
        admin_count = self._assistant_admin_count(data)
        display = self._bot_display_name(aid, data)
        return (
            f"Assistant: {display}\n"
            f"Total users: {total}\n"
            f"Premium users: {premium}\n"
            f"Non-premium users: {non_premium}\n"
            f"Blocked users: {blocked}\n"
            f"Total /start count: {data.get('stats', {}).get('total_starts', 0)}\n"
            f"Total messages received: {data.get('stats', {}).get('total_messages', 0)}\n"
            f"Admin count: {admin_count}\n"
            f"Creation date: {data.get('created_at', '-')}\n"
            f"Last active time: {data.get('last_active_at', '-')}"
        )

    def _assistant_admin_count(self, data: dict[str, Any]) -> int:
        admins = set(data.get("admins", []))
        admins.add(self.settings.super_admin_id)
        owner_id = data.get("owner_id")
        if owner_id is not None:
            admins.add(owner_id)
        return len(admins)

    async def _admin_panel(self, event: events.common.EventCommon) -> None:
        assistants = self.store.get_data().get("assistants", {})
        total_users = len(self._master_data().get("users", {}))
        text = (
            f"Admin Panel\n"
            f"Total master users: {total_users}\n"
            f"Total assistant bots: {len(assistants)}"
        )
        buttons = [[Button.inline("GLOBAL SYNC", data="m_sync")]]
        for aid, data in assistants.items():
            label = self._bot_display_name(aid, data)
            buttons.append([Button.inline(label, data=f"m_adminbot:{aid}")])
        await event.reply(text, buttons=buttons)

    async def _activate_assistant(self, user_id: int) -> None:
        pending = self.pending_connect.get(user_id)
        if not pending:
            return
        mode = pending.get("mode", "connect")
        assistant_id = pending["assistant_id"]
        assistants = self.store.get_data().setdefault("assistants", {})
        if mode == "replace":
            old_id = pending["target_assistant_id"]
            existing = assistants.get(old_id)
            if not existing:
                self.pending_connect.pop(user_id, None)
                return
            existing["session_b64"] = pending["session_b64"]
            existing["bot_username"] = pending.get("bot_username", existing.get("bot_username", ""))
            existing["last_active_at"] = datetime.now(timezone.utc).isoformat()
            await self.sessions.stop_assistant(old_id)
            await self.sessions.start_assistant(old_id, existing)
            self.pending_connect.pop(user_id, None)
            self.store.mark_dirty(old_id)
            return
        assistants[assistant_id] = {
            "assistant_id": assistant_id,
            "bot_username": pending.get("bot_username", ""),
            "owner_id": user_id,
            "session_b64": pending["session_b64"],
            "log_chat_id": pending.get("log_chat_id"),
            "users": {},
            "admins": [self.settings.super_admin_id],
            "blocked_users": [],
            "reply_map": {},
            "stats": {"total_starts": 0, "total_messages": 0},
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_active_at": datetime.now(timezone.utc).isoformat(),
            "start_post": None,
            "setmsg": None,
        }
        # Mark main dirty (assistant_ids list grows) and assistant dirty (new file).
        self.store.mark_dirty()
        self.store.mark_dirty(assistant_id)
        await self.sessions.start_assistant(assistant_id, assistants[assistant_id])
        self.pending_connect.pop(user_id, None)

    async def _handle_connect_upload(self, event: events.NewMessage.Event) -> bool:
        pending = self.pending_connect.get(event.sender_id or 0)
        if not pending or pending.get("step") != "upload":
            return False

        if not event.file or not (event.file.name or "").endswith(".session"):
            await event.reply("Please upload a valid .session file.")
            return True

        with TemporaryDirectory() as td:
            session_path = Path(td) / (event.file.name or "assistant.session")
            await event.download_media(file=session_path)
            test_client = TelegramClient(str(session_path), self.settings.api_id, self.settings.api_hash)
            try:
                await test_client.connect()
                me = await test_client.get_me()
                if not me:
                    raise ValueError("Invalid session")
                pending["assistant_id"] = str(me.id)
                pending["bot_username"] = me.username or ""
                if pending.get("mode") == "replace" and pending.get("target_assistant_id") != str(me.id):
                    await event.reply("Session user ID does not match selected assistant bot.")
                    return True
                pending["session_b64"] = base64.b64encode(session_path.read_bytes()).decode("utf-8")
                if pending.get("mode") == "replace":
                    pending["step"] = "done"
                else:
                    pending["step"] = "log_chat"
            finally:
                await test_client.disconnect()

        if pending.get("mode") == "replace":
            await self._activate_assistant(event.sender_id or 0)
            await event.reply("Assistant session replaced and restarted.")
            return True

        await event.reply(
            "Add this assistant bot to a group and make it admin for logs.\n"
            "Send log group ID now or press SKIP.",
            buttons=[
                [Button.inline("SKIP", data="m_skiplog")],
                [Button.inline("❌ Cancel", data="m_cancel")],
            ],
        )
        return True

    async def _handle_connect_log_chat(self, event: events.NewMessage.Event) -> bool:
        pending = self.pending_connect.get(event.sender_id or 0)
        if not pending or pending.get("step") != "log_chat":
            return False

        text = (event.raw_text or "").strip()
        if text.lstrip("-").isdigit():
            pending["log_chat_id"] = int(text)
            await self._activate_assistant(event.sender_id or 0)
            await event.reply("Assistant connected and activated.")
            return True

        await event.reply("Send a valid numeric chat ID or click SKIP.")
        return True

    async def _handle_admin_commands(self, event: events.NewMessage.Event) -> bool:
        text = (event.raw_text or "").strip()
        if not text.startswith("/"):
            return False

        if not self._is_admin(event.sender_id or 0):
            return False

        parts = text.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if command in {"/ban", "/unban", "/promote", "/demote"}:
            if not arg.isdigit():
                await event.reply("Usage: /ban <id>, /unban <id>, /promote <id>, /demote <id>")
                return True
            uid = int(arg)
            if command == "/ban":
                if uid not in self._master_data().setdefault("banned", []):
                    self._master_data()["banned"].append(uid)
            elif command == "/unban":
                self._master_data()["banned"] = [x for x in self._master_data().get("banned", []) if x != uid]
            elif command == "/promote":
                self._master_data().setdefault("admins", [])
                if uid not in self._master_data()["admins"]:
                    self._master_data()["admins"].append(uid)
            elif command == "/demote":
                self._master_data()["admins"] = [x for x in self._master_data().get("admins", []) if x != uid]
            self.store.mark_dirty()
            await event.reply("Updated.")
            return True

        return False

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        user_id = event.sender_id or 0
        if user_id == 0 or not event.is_private:
            return

        self._ensure_master_user(user_id)
        self._master_data().setdefault("stats", {"total_starts": 0, "total_messages": 0})
        self._master_data()["stats"]["total_messages"] += 1
        self.store.mark_dirty()

        if self._is_banned(user_id):
            await event.reply("You are banned from using this bot.")
            return

        if await self._handle_admin_commands(event):
            return

        if await self._handle_connect_upload(event):
            return
        if await self._handle_connect_log_chat(event):
            return

        text = (event.raw_text or "").strip().lower()
        if text.startswith("/start"):
            self._master_data()["stats"]["total_starts"] += 1
            self.store.mark_dirty()
            await event.reply("Master Control Panel", buttons=self._main_menu())

    async def _on_callback(self, event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id or 0
        cb_data = (event.data or b"").decode("utf-8")

        if self._is_banned(user_id):
            await event.answer("Banned", alert=True)
            return

        if cb_data == "m_connect":
            self.pending_connect[user_id] = {"step": "upload", "mode": "connect"}
            await event.edit(
                "Upload your .session file now.",
                buttons=[[Button.inline("❌ Cancel", data="m_cancel")]],
            )
            return

        if cb_data == "m_cancel":
            self.pending_connect.pop(user_id, None)
            await event.edit("Cancelled.", buttons=self._main_menu())
            return

        if cb_data == "m_skiplog":
            pending = self.pending_connect.get(user_id)
            if pending and pending.get("step") == "log_chat":
                pending["log_chat_id"] = None
                await self._activate_assistant(user_id)
                await event.edit("Assistant connected. Logs will be sent to owner DM.")
            else:
                await event.answer("No pending connect flow", alert=True)
            return

        if cb_data == "m_mybots":
            await event.edit("Loading your bots...")
            await self._show_my_bots(event, user_id)
            return

        if cb_data.startswith("m_bstat:"):
            aid = cb_data.split(":", 1)[1]
            bot_data = self.store.get_data().get("assistants", {}).get(aid)
            if not bot_data:
                await event.answer("Not found", alert=True)
                return
            if bot_data.get("owner_id") != user_id and not self._is_admin(user_id):
                await event.answer("Not allowed", alert=True)
                return
            await event.edit(self._assistant_stats(aid, bot_data))
            return

        if cb_data == "m_disconnect":
            assistants = self.store.get_data().get("assistants", {})
            mine = [
                (aid, item)
                for aid, item in assistants.items()
                if item.get("owner_id") == user_id or self._is_admin(user_id)
            ]
            if not mine:
                await event.edit("No assistant bots to disconnect.")
                return
            buttons = [
                [Button.inline(f"Disconnect {self._bot_display_name(aid, item)}", data=f"m_disco:{aid}")]
                for aid, item in mine
            ]
            await event.edit("Select bot to disconnect:", buttons=buttons)
            return

        if cb_data.startswith("m_disco:"):
            aid = cb_data.split(":", 1)[1]
            assistants = self.store.get_data().get("assistants", {})
            bot_data = assistants.get(aid)
            if not bot_data:
                await event.answer("Not found", alert=True)
                return
            if bot_data.get("owner_id") != user_id and not self._is_admin(user_id):
                await event.answer("Not allowed", alert=True)
                return
            display = self._bot_display_name(aid, bot_data)
            await self.sessions.stop_assistant(aid)
            await self.store.delete_assistant_data(aid)
            await event.edit(f"Assistant {display} disconnected and removed.")
            return

        if cb_data == "m_admin":
            if not self._is_admin(user_id):
                await event.answer("Admin only", alert=True)
                return
            await self._admin_panel(event)
            return

        if cb_data == "m_sync":
            if not self._is_admin(user_id):
                await event.answer("Admin only", alert=True)
                return
            await event.edit("Syncing...")
            await self.store.sync(force=True)
            await event.edit("HF dataset sync complete.")
            return

        if cb_data.startswith("m_adminbot:"):
            if not self._is_admin(user_id):
                await event.answer("Admin only", alert=True)
                return
            aid = cb_data.split(":", 1)[1]
            bot_data = self.store.get_data().get("assistants", {}).get(aid)
            if not bot_data:
                await event.answer("Not found", alert=True)
                return
            text = (
                f"Owner: {bot_data.get('owner_id')}\n\n"
                f"{self._assistant_stats(aid, bot_data)}"
            )
            await event.edit(
                text,
                buttons=[
                    [Button.inline("Disconnect bot", data=f"m_adisco:{aid}")],
                    [Button.inline("Wipe all data", data=f"m_awipe:{aid}")],
                    [Button.inline("Upload/Replace session", data=f"m_areplace:{aid}")],
                ],
            )
            return

        if cb_data.startswith("m_adisco:"):
            if not self._is_admin(user_id):
                await event.answer("Admin only", alert=True)
                return
            aid = cb_data.split(":", 1)[1]
            await self.sessions.stop_assistant(aid)
            await self.store.delete_assistant_data(aid)
            await event.edit("Disconnected.")
            return

        if cb_data.startswith("m_awipe:"):
            if not self._is_admin(user_id):
                await event.answer("Admin only", alert=True)
                return
            aid = cb_data.split(":", 1)[1]
            data_ref = self.store.get_data().get("assistants", {}).get(aid)
            if data_ref:
                data_ref["users"] = {}
                data_ref["blocked_users"] = []
                data_ref["reply_map"] = {}
                data_ref["stats"] = {"total_starts": 0, "total_messages": 0}
                self.store.mark_dirty(aid)
            await event.edit("Assistant data wiped.")
            return

        if cb_data.startswith("m_areplace:"):
            if not self._is_admin(user_id):
                await event.answer("Admin only", alert=True)
                return
            aid = cb_data.split(":", 1)[1]
            if aid not in self.store.get_data().get("assistants", {}):
                await event.answer("Not found", alert=True)
                return
            self.pending_connect[user_id] = {
                "step": "upload",
                "mode": "replace",
                "target_assistant_id": aid,
            }
            await event.edit(
                f"Upload replacement .session for assistant {aid}.",
                buttons=[[Button.inline("❌ Cancel", data="m_cancel")]],
            )
            return
