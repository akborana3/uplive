import base64
import io
from typing import Any

from telethon import TelegramClient
from telethon.tl.custom.message import Message


async def serialize_message(client: TelegramClient, message: Message) -> dict[str, Any]:
    msg_file = getattr(message, "file", None)
    payload: dict[str, Any] = {
        "text": message.raw_text or "",
        "caption": message.text or "",
    }
    if message.media:
        raw = await client.download_media(message, file=bytes)
        if raw:
            payload["media_b64"] = base64.b64encode(raw).decode("utf-8")
            payload["mime_type"] = getattr(msg_file, "mime_type", "application/octet-stream")
            payload["name"] = getattr(msg_file, "name", "file")
    return payload


async def send_payload(client: TelegramClient, chat_id: int, payload: dict[str, Any]) -> Any:
    media_b64 = payload.get("media_b64")
    if media_b64:
        data = base64.b64decode(media_b64)
        bio = io.BytesIO(data)
        bio.name = payload.get("name") or "file"
        return await client.send_file(chat_id, bio, caption=payload.get("caption") or payload.get("text") or "")
    return await client.send_message(chat_id, payload.get("text") or payload.get("caption") or "")
