import asyncio
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.types import BufferedInputFile

# =========================
# ENV (Railway)
# =========================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
YANDEX_TOKEN = os.getenv("YANDEX_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

missing = [k for k, v in {
    "TG_BOT_TOKEN": TG_BOT_TOKEN,
    "YANDEX_TOKEN": YANDEX_TOKEN,
    "TG_CHAT_ID": TG_CHAT_ID
}.items() if not v]

if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

TG_CHAT_ID = int(TG_CHAT_ID)

# =========================
# SETTINGS
# =========================
POLL_INTERVAL = 60
SEND_DELAY_SEC = 0.7
LAST_LIMIT = 80                 # чуть больше хвост, чтобы не пропускать
SKIP_EXISTING_ON_START = True
DB_PATH = "state.db"

# Сколько времени (сек) ждать скачивания файла с Диска
DOWNLOAD_TIMEOUT = 60

YANDEX_LAST_UPLOADED = "https://cloud-api.yandex.net/v1/disk/resources/last-uploaded"
YANDEX_API = "https://cloud-api.yandex.net/v1/disk/resources"

# =========================
# TAGS BY FOLDER (ищем по вхождению)
# =========================
TAG_BY_FOLDER = {
    "/palcevolevf/": "#Palcevo",
    "/gvardjd/": "#Gvardeyskiy",
    "/palcevotropa/": "#Palcevo2",
    "/shluz/": "#Shluz",
}

def tag_for_path(path: str) -> str:
    p = (path or "").lower()
    for folder, tag in TAG_BY_FOLDER.items():
        if folder in p:
            return tag
    return "#Photo"

# =========================
# DB
# =========================
CREATE_SENT_SQL = """
CREATE TABLE IF NOT EXISTS sent_files (
    path TEXT PRIMARY KEY,
    modified TEXT
);
"""

CREATE_META_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

async def init_db():
    db = await aiosqlite.connect(DB_PATH)
    await db.execute(CREATE_SENT_SQL)
    await db.execute(CREATE_META_SQL)
    await db.commit()
    return db

async def meta_get(db: aiosqlite.Connection, key: str) -> Optional[str]:
    async with db.execute("SELECT value FROM meta WHERE key=?", (key,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None

async def meta_set(db: aiosqlite.Connection, key: str, value: str):
    await db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, value))
    await db.commit()

async def is_sent(db: aiosqlite.Connection, path: str) -> bool:
    async with db.execute("SELECT 1 FROM sent_files WHERE path=?", (path,)) as cur:
        return (await cur.fetchone()) is not None

async def mark_sent(db: aiosqlite.Connection, path: str, modified: str):
    await db.execute("INSERT OR IGNORE INTO sent_files(path, modified) VALUES(?,?)", (path, modified))
    await db.commit()

# =========================
# HELPERS
# =========================
def parse_iso(iso_date: str) -> Optional[datetime]:
    if not iso_date:
        return None
    try:
        return datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    except Exception:
        return None

from datetime import timedelta

def now_str_local() -> str:
    # UTC + 3 часа (МСК)
    return (datetime.utcnow() + timedelta(hours=3)).strftime("%d.%m.%Y %H:%M")

# =========================
# Yandex Client
# =========================
class YandexDiskClient:
    def __init__(self, token: str):
        self.token = token

    @property
    def headers(self):
        return {"Authorization": f"OAuth {self.token}"}

    async def last_uploaded_images(self, session: aiohttp.ClientSession, limit: int = 80) -> List[Dict[str, Any]]:
        params = {
            "limit": limit,
            "fields": "items.path,items.name,items.modified,items.mime_type,items.type"
        }
        async with session.get(YANDEX_LAST_UPLOADED, headers=self.headers, params=params, timeout=30) as r:
            r.raise_for_status()
            data = await r.json()

        items = data.get("items", [])
        images = []

        for it in items:
            if it.get("type") != "file":
                continue
            name = (it.get("name") or "").lower()
            mime = (it.get("mime_type") or "").lower()
            if mime.startswith("image/") or name.endswith((".jpg", ".jpeg", ".png", ".webp")):
                images.append(it)

        images.sort(key=lambda x: x.get("modified", ""))
        return images

    async def get_download_url(self, session: aiohttp.ClientSession, file_path: str) -> str:
        params = {"path": file_path}
        async with session.get(f"{YANDEX_API}/download", headers=self.headers, params=params, timeout=30) as r:
            r.raise_for_status()
            data = await r.json()
        return data["href"]

    async def download_bytes(self, session: aiohttp.ClientSession, href: str) -> bytes:
        # Telegram иногда не может сам скачать по URL, поэтому скачиваем мы и отправляем байтами
        async with session.get(href, timeout=DOWNLOAD_TIMEOUT) as r:
            r.raise_for_status()
            return await r.read()

# =========================
# Bootstrap (anti-spam)
# =========================
async def bootstrap_cursor_if_needed(db: aiosqlite.Connection, ydx: YandexDiskClient):
    cur = await meta_get(db, "cursor_modified")
    if cur is not None:
        return

    if not SKIP_EXISTING_ON_START:
        await meta_set(db, "cursor_modified", "1970-01-01T00:00:00+00:00")
        return

    async with aiohttp.ClientSession() as session:
        items = await ydx.last_uploaded_images(session, limit=LAST_LIMIT)
        max_dt = None
        for it in items:
            dt = parse_iso(it.get("modified", ""))
            if dt and (max_dt is None or dt > max_dt):
                max_dt = dt

        if max_dt is None:
            max_dt = datetime.now(timezone.utc)

        await meta_set(db, "cursor_modified", max_dt.isoformat())
        print(f"BOOTSTRAP: cursor set to {max_dt.isoformat()}")

# =========================
# MAIN LOOP
# =========================
async def poll_and_forward(bot: Bot, ydx: YandexDiskClient, db: aiosqlite.Connection):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                cursor_iso = await meta_get(db, "cursor_modified") or "1970-01-01T00:00:00+00:00"
                cursor_dt = parse_iso(cursor_iso) or datetime(1970, 1, 1, tzinfo=timezone.utc)

                files = await ydx.last_uploaded_images(session, limit=LAST_LIMIT)
                newest_seen = cursor_dt

                for f in files:
                    path = f["path"]
                    modified = f.get("modified", "")
                    dt = parse_iso(modified)

                    if not dt or dt <= cursor_dt:
                        continue

                    # дубль — пропускаем
                    if await is_sent(db, path):
                        newest_seen = max(newest_seen, dt)
                        continue

                    href = await ydx.get_download_url(session, path)

                    # скачиваем байты
                    data = await ydx.download_bytes(session, href)

                    tag = tag_for_path(path)
                    caption = f"{tag} • {now_str_local()}"  # дата отправки, а не modified

                    filename = f.get("name") or "photo.jpg"
                    photo = BufferedInputFile(data, filename=filename)

                    await bot.send_photo(
                        chat_id=TG_CHAT_ID,
                        photo=photo,
                        caption=caption
                    )

                    await mark_sent(db, path, modified)
                    newest_seen = max(newest_seen, dt)
                    await asyncio.sleep(SEND_DELAY_SEC)

                if newest_seen > cursor_dt:
                    await meta_set(db, "cursor_modified", newest_seen.isoformat())

            except Exception as e:
                print("ERROR:", repr(e))

            await asyncio.sleep(POLL_INTERVAL)

# =========================
# RUN
# =========================
async def main():
    bot = Bot(token=TG_BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(F.text == "/id")
    async def cmd_id(message: Message):
        await message.answer(f"chat_id = {message.chat.id}")

    db = await init_db()
    ydx = YandexDiskClient(YANDEX_TOKEN)

    await bootstrap_cursor_if_needed(db, ydx)

    asyncio.create_task(poll_and_forward(bot, ydx, db))

    print("БОТ ЗАПУЩЕН ✅")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())