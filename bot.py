"""Bridge: forwards messages from one MAX group to a Telegram forum topic.

Runs as a userbot on your MAX account (PyMax library) and posts through a Telegram bot.
Only the chat set in MAX_CHAT_ID is forwarded; every other chat is ignored.
"""
import asyncio
import base64
import html
import logging
import os
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from pymax import Client, Message
from pymax.types.domain import (
    AudioAttachment,
    FileAttachment,
    PhotoAttachment,
    ShareAttachment,
    StickerAttachment,
    VideoAttachment,
)

load_dotenv()

MAX_PHONE = os.environ["MAX_PHONE"]
MAX_CHAT_ID = int(os.environ["MAX_CHAT_ID"]) if os.getenv("MAX_CHAT_ID") else None
TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]
TG_THREAD_ID = os.getenv("TG_THREAD_ID")  # forum topic id; empty = General topic

TG_API = f"https://api.telegram.org/bot{TG_TOKEN}"
TG_UPLOAD_LIMIT = 50 * 1024 * 1024  # Bot API upload limit
CAPTION_LIMIT = 1024

SESSION_DIR = Path("session")
SESSION_FILE = SESSION_DIR / "session.db"
# On hosting there is no console for the SMS code, so the saved session comes from an env var.
if os.getenv("MAX_SESSION") and not SESSION_FILE.exists():
    SESSION_DIR.mkdir(exist_ok=True)
    SESSION_FILE.write_bytes(base64.b64decode(os.environ["MAX_SESSION"]))

log = logging.getLogger("bridge")
client = Client(phone=MAX_PHONE, work_dir=str(SESSION_DIR))
http: aiohttp.ClientSession | None = None
send_lock = asyncio.Lock()


async def tg_call(method, fields, file=None):
    """file = (form field, file name, bytes)."""
    fields = {**fields, "chat_id": TG_CHAT_ID}
    if TG_THREAD_ID:
        fields["message_thread_id"] = TG_THREAD_ID
    for attempt in range(5):
        form = aiohttp.FormData()
        for key, value in fields.items():
            form.add_field(key, str(value))
        if file:
            form.add_field(file[0], file[2], filename=file[1])
        try:
            async with http.post(f"{TG_API}/{method}", data=form) as r:
                body = await r.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            log.warning("Telegram %s: %s, retrying", method, e)
            await asyncio.sleep(2 ** attempt)
            continue
        if r.status == 429:
            await asyncio.sleep(body.get("parameters", {}).get("retry_after", 5))
            continue
        if not body.get("ok"):
            raise RuntimeError(f"Telegram {method} failed: {body}")
        return body
    raise RuntimeError(f"Telegram {method} failed after retries")


async def send_text(text):
    await tg_call("sendMessage", {"text": text, "parse_mode": "HTML"})


async def send_long_text(header, raw_text):
    """Telegram limits a message to 4096 chars, so long text goes in several parts."""
    chunk = 3500  # leaves room for the header and for HTML escaping
    parts = [raw_text[i : i + chunk] for i in range(0, len(raw_text), chunk)] or [""]
    for part in parts:
        await send_text(f"{header}\n{html.escape(part)}" if part else header)


async def download(url):
    async with http.get(url) as r:
        r.raise_for_status()
        if int(r.headers.get("Content-Length") or 0) > TG_UPLOAD_LIMIT:
            return None
        data = await r.read()
    return data if len(data) <= TG_UPLOAD_LIMIT else None


async def send_media(method, field, name, url, caption):
    content = await download(url)
    if content is None:
        await send_text(f"{caption}\n[файл больше 50 МБ]\n{html.escape(url)}")
        return
    await tg_call(method, {"caption": caption, "parse_mode": "HTML"}, (field, name, content))


async def send_attachment(message, att, caption):
    if isinstance(att, PhotoAttachment):
        await send_media("sendPhoto", "photo", "photo.jpg", att.base_url, caption)
    elif isinstance(att, FileAttachment):
        info = await client.get_file_by_id(chat_id=message.chat_id, message_id=message.id, file_id=att.file_id)
        if info is None:
            await send_text(f"{caption}\n[файл: {html.escape(att.name)}]")
            return
        await send_media("sendDocument", "document", att.name, info.url, caption)
    elif isinstance(att, VideoAttachment):
        info = await client.get_video_by_id(chat_id=message.chat_id, message_id=message.id, video_id=att.video_id)
        if info is None or not info.url:
            await send_text(f"{caption}\n[видео]")
            return
        await send_media("sendVideo", "video", "video.mp4", info.url, caption)
    elif isinstance(att, AudioAttachment) and att.url:
        await send_media("sendAudio", "audio", "audio.mp3", att.url, caption)
    elif isinstance(att, StickerAttachment):
        await send_media("sendDocument", "document", "sticker.webp", att.url, caption)
    elif isinstance(att, ShareAttachment) and att.url:
        await send_text(f"{caption}\n{html.escape(att.url)}")
    else:
        await send_text(f"{caption}\n[вложение: {html.escape(str(getattr(att, 'type', '?')))}]")


async def author_name(user_id):
    if user_id is None:
        return "Группа"
    user = await client.get_user(user_id)
    if user and user.names:
        n = user.names[0]
        return n.name or " ".join(filter(None, [n.first_name, n.last_name])) or str(user_id)
    return str(user_id)


@client.on_start()
async def on_start(client: Client) -> None:
    log.info("Вход выполнен. Ваши чаты в MAX (id — название):")
    for chat in await client.fetch_chats() or []:
        log.info("  %s — %s", chat.id, chat.title)
    if MAX_CHAT_ID is None:
        log.warning("MAX_CHAT_ID не задан: бот ничего не пересылает. Впишите id нужной группы в .env")


@client.on_message()
async def on_message(message: Message, client: Client) -> None:
    if MAX_CHAT_ID is None or message.chat_id != MAX_CHAT_ID:
        return
    # PyMax runs handlers concurrently; the lock keeps Telegram order equal to MAX order.
    async with send_lock:
        try:
            await forward(message)
        except Exception:
            log.exception("Не удалось переслать сообщение %s", message.id)


async def forward(message: Message) -> None:
    header = f"<b>{html.escape(await author_name(message.sender))}</b>"
    raw_text = message.text or ""
    full = f"{header}\n{html.escape(raw_text)}" if raw_text else header
    attachments = list(message.attaches)

    if attachments and len(full) <= CAPTION_LIMIT:
        # Text fits into the first attachment's caption.
        first, attachments = attachments[0], attachments[1:]
        try:
            await send_attachment(message, first, full)
        except Exception:
            log.exception("Не удалось переслать вложение в сообщении %s", message.id)
            await send_text(f"{full}\n[вложение не удалось переслать]")
    elif raw_text or not attachments:
        await send_long_text(header, raw_text)
    for att in attachments:
        try:
            await send_attachment(message, att, header)
        except Exception:
            log.exception("Не удалось переслать вложение в сообщении %s", message.id)
            await send_text(f"{header}\n[вложение не удалось переслать]")


async def main():
    global http
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as http:
        await client.start()


if __name__ == "__main__":
    asyncio.run(main())
