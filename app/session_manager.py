import asyncio
import base64
import logging
from pathlib import Path
from typing import Any

from app.bots.assistant import AssistantRuntime
from app.config import Settings
from app.storage.hf_dataset import HFDataStore


class SessionManager:
    def __init__(self, settings: Settings, store: HFDataStore) -> None:
        self.settings = settings
        self.store = store
        self.base_dir = Path("sessions")
        self.base_dir.mkdir(exist_ok=True)
        self._assistants: dict[str, AssistantRuntime] = {}

    def running_ids(self) -> list[str]:
        return list(self._assistants.keys())

    def get_runtime(self, assistant_id: str) -> AssistantRuntime | None:
        return self._assistants.get(assistant_id)

    async def start_assistant(self, assistant_id: str, assistant_data: dict[str, Any]) -> None:
        if assistant_id in self._assistants:
            return

        session_b64 = assistant_data.get("session_b64", "")
        session_path = self.base_dir / f"{assistant_id}.session"
        if session_b64:
            try:
                session_path.write_bytes(base64.b64decode(session_b64.encode("utf-8")))
            except Exception as exc:
                raise ValueError(f"Corrupted session data for assistant {assistant_id}") from exc

        runtime = AssistantRuntime(self.settings, self.store, assistant_id, str(session_path))
        await runtime.start()
        self._assistants[assistant_id] = runtime

    async def stop_assistant(self, assistant_id: str) -> None:
        runtime = self._assistants.pop(assistant_id, None)
        if runtime:
            await runtime.stop()
        (self.base_dir / f"{assistant_id}.session").unlink(missing_ok=True)

    async def load_all(self) -> None:
        assistants = self.store.get_data().get("assistants", {})
        for assistant_id, data in assistants.items():
            try:
                await self.start_assistant(assistant_id, data)
            except Exception:
                logging.exception("Failed to start assistant %s", assistant_id)

    async def shutdown(self) -> None:
        for assistant_id in list(self._assistants.keys()):
            await self.stop_assistant(assistant_id)
