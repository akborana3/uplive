import asyncio
import copy
import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError

from app.config import Settings
from app.utils.stats_agg import empty_stats_agg

logger = logging.getLogger(__name__)

# Bump whenever the per-user record shape changes in a way old records can't
# just be lazily upgraded from (v2 added dc_id / lang_code / stats_agg).
# Bumping this wipes ``users`` / ``stats`` / ``stats_agg`` / ``reply_map`` for
# the master bot and every assistant on next boot -- sessions, owner/admin
# ids and blocked lists are preserved so nothing needs to be reconnected.
SCHEMA_VERSION = 2


class HFDataStore:
    # New split-file layout
    MAIN_FILE = "main_db.json"
    # Legacy single-file path used for one-time migration
    LEGACY_FILE = "database.json"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.api = HfApi(token=settings.hf_token)
        self.data: dict[str, Any] = self._default_data()
        self._lock = asyncio.Lock()
        self._main_dirty = False
        self._assistant_dirty: dict[str, bool] = {}
        self._auto_sync_task: asyncio.Task | None = None

    @staticmethod
    def _default_data() -> dict[str, Any]:
        return {
            "version": 1,
            "schema_version": SCHEMA_VERSION,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "master": {
                "users": {},
                "admins": [],
                "banned": [],
                "stats": {"total_starts": 0, "total_messages": 0},
                "stats_agg": empty_stats_agg(),
                "session_b64": "",
            },
            "assistants": {},
        }

    def _migrate_schema_if_needed(self) -> None:
        """One-time reset of per-user stats data when SCHEMA_VERSION bumps.

        Only ``users``/``stats``/``stats_agg``/``reply_map`` are cleared —
        session_b64, owner_id, log_chat_id, admins and blocked_users are all
        left untouched, so no bot needs to be reconnected because of this.
        """
        current = self.data.get("schema_version", 1)
        if current >= SCHEMA_VERSION:
            return

        logger.warning(
            "DB schema %s -> %s: clearing users/stats/stats_agg/reply_map "
            "for master + %d assistant(s). Sessions and admin/owner config "
            "are preserved.",
            current,
            SCHEMA_VERSION,
            len(self.data.get("assistants", {})),
        )

        scopes = [self.data.setdefault("master", {})]
        scopes.extend(self.data.get("assistants", {}).values())
        for scope in scopes:
            scope["users"] = {}
            scope["stats"] = {"total_starts": 0, "total_messages": 0}
            scope["stats_agg"] = empty_stats_agg()
            scope["reply_map"] = {}

        self.data["schema_version"] = SCHEMA_VERSION
        self._main_dirty = True
        for aid in self.data.get("assistants", {}):
            self._assistant_dirty[aid] = True

    async def initialize(self) -> None:
        await self._ensure_repo()
        await self.load()

    async def _ensure_repo(self) -> None:
        def _create() -> None:
            self.api.create_repo(
                repo_id=self.settings.hf_repo_id,
                repo_type="dataset",
                exist_ok=True,
                token=self.settings.hf_token,
            )

        await asyncio.to_thread(_create)

    def _assistant_filename(self, assistant_id: str) -> str:
        return f"assistant_{assistant_id}.json"

    async def _download_json(self, filename: str) -> dict[str, Any] | None:
        """Download and parse a JSON file from HF. Returns None if not found."""
        try:
            local = await asyncio.to_thread(
                hf_hub_download,
                repo_id=self.settings.hf_repo_id,
                repo_type="dataset",
                filename=filename,
                token=self.settings.hf_token,
            )
            with open(local, "r", encoding="utf-8") as f:
                return json.load(f)
        except (EntryNotFoundError, RepositoryNotFoundError, FileNotFoundError):
            return None

    async def load(self) -> None:
        async with self._lock:
            main_data = await self._download_json(self.MAIN_FILE)

            if main_data is None:
                # Attempt one-time migration from legacy single file
                legacy = await self._download_json(self.LEGACY_FILE)
                if legacy is not None:
                    logger.info("Migrating from legacy %s to split-file format.", self.LEGACY_FILE)
                    self.data = legacy
                    self.data.setdefault("assistants", {})
                    self.data.setdefault("schema_version", 1)
                    self._main_dirty = True
                    for aid in self.data.get("assistants", {}):
                        self._assistant_dirty[aid] = True
                else:
                    self.data = self._default_data()
                    self._main_dirty = True
                self._migrate_schema_if_needed()
                await self._sync_unlocked()
                return

            self.data = {
                "version": main_data.get("version", 1),
                "schema_version": main_data.get("schema_version", 1),
                "updated_at": main_data.get("updated_at", datetime.now(timezone.utc).isoformat()),
                "master": main_data.get("master", self._default_data()["master"]),
                "assistants": {},
            }

            for aid in main_data.get("assistant_ids", []):
                a_data = await self._download_json(self._assistant_filename(aid))
                if a_data is not None:
                    self.data["assistants"][aid] = a_data
                else:
                    logger.warning("Assistant file for %s not found in HF; skipping.", aid)
                    # Stale ID – update main_db on the next sync to remove it.
                    self._main_dirty = True

            self._migrate_schema_if_needed()
            if self._is_dirty():
                await self._sync_unlocked()

    def get_data(self) -> dict[str, Any]:
        return self.data

    def get_snapshot(self) -> dict[str, Any]:
        return copy.deepcopy(self.data)

    def mark_dirty(self, assistant_id: str | None = None) -> None:
        """Mark data dirty for the next sync.

        Pass *assistant_id* to mark only that assistant's file dirty; omit
        (or pass ``None``) to mark the main database file dirty.
        """
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
        if assistant_id is not None:
            self._assistant_dirty[assistant_id] = True
        else:
            self._main_dirty = True

    def _is_dirty(self) -> bool:
        return self._main_dirty or bool(self._assistant_dirty)

    async def delete_assistant_data(self, assistant_id: str) -> None:
        """Remove an assistant from memory and delete its HF file.

        The main database is marked dirty so that ``assistant_ids`` is updated
        on the next sync.  Any pending dirty flag for the assistant is cleared
        to avoid re-uploading a file we just deleted.
        """
        self.data.get("assistants", {}).pop(assistant_id, None)
        self._assistant_dirty.pop(assistant_id, None)
        self._main_dirty = True  # assistant_ids list shrinks

        def _delete() -> None:
            try:
                self.api.delete_file(
                    path_in_repo=self._assistant_filename(assistant_id),
                    repo_id=self.settings.hf_repo_id,
                    repo_type="dataset",
                    token=self.settings.hf_token,
                )
            except Exception:
                logger.warning("Could not delete HF file for assistant %s (may not exist).", assistant_id)

        await asyncio.to_thread(_delete)

    async def _upload_json(self, filename: str, payload: dict[str, Any]) -> None:
        # Snapshot on the event loop first (cheap object copy) so the actual
        # json.dump + upload can run entirely in a worker thread without
        # racing against concurrent mutations to self.data happening on the
        # loop while serialization is in flight — dict mutation isn't safe to
        # interleave with json.dump iterating the same structure.
        snapshot = copy.deepcopy(payload)

        def _dump_and_upload() -> None:
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json", encoding="utf-8") as tf:
                json.dump(snapshot, tf, ensure_ascii=False, indent=2)
                temp_path = tf.name
            try:
                self.api.upload_file(
                    path_or_fileobj=temp_path,
                    path_in_repo=filename,
                    repo_id=self.settings.hf_repo_id,
                    repo_type="dataset",
                    token=self.settings.hf_token,
                    commit_message=f"sync {filename}",
                )
            finally:
                Path(temp_path).unlink(missing_ok=True)

        await asyncio.to_thread(_dump_and_upload)

    async def _sync_unlocked(self) -> None:
        """Upload all dirty files. Caller must hold ``self._lock``."""
        if self._main_dirty:
            main_payload = {
                "version": self.data.get("version", 1),
                "schema_version": self.data.get("schema_version", SCHEMA_VERSION),
                "updated_at": self.data.get("updated_at"),
                "master": self.data.get("master", {}),
                # Keep track of which per-assistant files exist
                "assistant_ids": list(self.data.get("assistants", {}).keys()),
            }
            await self._upload_json(self.MAIN_FILE, main_payload)
            self._main_dirty = False

        for aid, dirty in list(self._assistant_dirty.items()):
            if dirty:
                a_data = self.data.get("assistants", {}).get(aid)
                if a_data is not None:
                    await self._upload_json(self._assistant_filename(aid), a_data)
                self._assistant_dirty[aid] = False

    async def sync(self, force: bool = False) -> None:
        async with self._lock:
            if not force and not self._is_dirty():
                return
            if force:
                self._main_dirty = True
                for aid in self.data.get("assistants", {}):
                    self._assistant_dirty[aid] = True
            await self._sync_unlocked()

    async def start_auto_sync(self) -> None:
        if self._auto_sync_task and not self._auto_sync_task.done():
            return

        async def _runner() -> None:
            while True:
                await asyncio.sleep(self.settings.auto_sync_interval)
                try:
                    await self.sync()
                except Exception:
                    logger.exception("Auto-sync failed for repo %s", self.settings.hf_repo_id)

        self._auto_sync_task = asyncio.create_task(_runner())

    async def stop_auto_sync(self) -> None:
        if self._auto_sync_task and not self._auto_sync_task.done():
            self._auto_sync_task.cancel()
            try:
                await self._auto_sync_task
            except asyncio.CancelledError:
                pass
