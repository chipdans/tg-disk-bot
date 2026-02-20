import asyncio
import os
from typing import List, Dict, Any
from datetime import datetime

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message

# =========================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (Railway)
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
# НАСТРОЙКИ
# =========================
POLL_INTERVAL = 60  # проверка раз в минуту
SEND_DELAY_SEC = 0.7
LAST_LIMIT = 20  # сколько последних файлов проверять
DB_PATH = "state.db"

YANDEX_LAST_UPLOADED = "https://cloud-api.yandex.net/v1/disk/resources/last-uploaded"
YANDEX_API = "https://cloud-api.yandex.net/v1/disk/resources"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sent_files (
    path TEXT PRIMARY KEY,
    modified TEXT
);
"""

# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================

def format_date(iso_date: str) -> str:
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso_date


class YandexDiskClient:
    def __init__(self, token: str):
        self.token = token

    @property
    def headers(self):
        return {"Authorization": f"OAuth {self.token}"}

    async def last_uploaded_images(self, session: aiohttp.ClientSession, limit: int = 50) -> List[Dict[str, Any]]:
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


async def init_db():
    db = await aiosqlite.connect(DB_PATH)
    await db.execute(CREATE_TABLE_SQL)
    await db.commit()
    return db


async def is_sent(db: aiosqlite.Connection, path: str) -> bool:
    async with db.execute("SELECT 1 FROM sent_files WHERE path = ?", (path,)) as cur:
        return (await cur.fetchone()) is not None


async def mark_sent(db: aiosqlite.Connection, path: str, modified: str):
    await db.execute("INSERT OR IGNORE INTO sent_files(path, modified) VALUES(?, ?)", (path, modified))
    await db.commit()


# =========================
# ОСНОВНОЙ ЦИКЛ
# =========================

async def poll_and_forward(bot: Bot, ydx: YandexDiskClient, db: aiosqlite.Connection):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                files = await ydx.last_uploaded_images(session, limit=LAST_LIMIT)

                for f in files:
                    path = f["path"]
                    modified = f.get("modified", "")

                    if await is_sent(db, path):
                        continue

                    download_url = await ydx.get_download_url(session, path)

                    caption = (
                        f"📸 {f.get('name', '')}\n"
                        f"📁 {path}\n"
                        f"📅 {format_date(modified)}"
                    )

                    await bot.send_photo(
                        chat_id=TG_CHAT_ID,
                        photo=download_url,
                        caption=caption
                    )

                    await mark_sent(db, path, modified)
                    await asyncio.sleep(SEND_DELAY_SEC)

            except Exception as e:
                print("ERROR:", repr(e))

            await asyncio.sleep(POLL_INTERVAL)


# =========================
# ЗАПУСК
# =========================

async def main():
    bot = Bot(token=TG_BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(F.text == "/id")
    async def cmd_id(message: Message):
        await message.answer(f"chat_id = {message.chat.id}")

    db = await init_db()
    ydx = YandexDiskClient(YANDEX_TOKEN)

    asyncio.create_task(poll_and_forward(bot, ydx, db))

    print("БОТ ЗАПУЩЕН ✅")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())