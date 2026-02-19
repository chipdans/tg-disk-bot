import asyncio
from typing import List, Dict, Any
from datetime import datetime
import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
import os

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


FOLDERS = [
    "/Gvardjd",
    "/palcevolevf",
    "/Palcevotropa",
    "/shluz",
]

POLL_INTERVAL = 30  # секунд между проверками
DB_PATH = "state.db"

# True  -> при первом запуске отметит все текущие фото как "уже отправленные" и не будет спамить старыми
# False -> отправит ВСЕ фото, которые уже лежат в папках
SKIP_EXISTING_ON_FIRST_RUN = True

# Пауза между отправками (анти-флуд)
SEND_DELAY_SEC = 0.6
# =========================

YANDEX_API = "https://cloud-api.yandex.net/v1/disk/resources"

CREATE_TABLE_SQL = """
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


def format_date(iso_date: str) -> str:
    """
    Преобразует дату из формата Яндекс API (ISO 8601)
    в формат: 19.02.2026 14:37
    """
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

    async def list_images(self, session: aiohttp.ClientSession, folder_path: str) -> List[Dict[str, Any]]:
        params = {
            "path": folder_path,
            "limit": 1000,
            "fields": "_embedded.items.path,_embedded.items.type,_embedded.items.name,_embedded.items.mime_type,_embedded.items.modified"
        }
        async with session.get(YANDEX_API, headers=self.headers, params=params, timeout=30) as r:
            r.raise_for_status()
            data = await r.json()

        items = data.get("_embedded", {}).get("items", [])
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


async def init_db(db_path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(db_path)
    await db.execute(CREATE_TABLE_SQL)
    await db.execute(CREATE_META_SQL)
    await db.commit()
    return db


async def meta_get(db: aiosqlite.Connection, key: str) -> str | None:
    async with db.execute("SELECT value FROM meta WHERE key=?", (key,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None


async def meta_set(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))
    await db.commit()


async def is_sent(db: aiosqlite.Connection, path: str) -> bool:
    async with db.execute("SELECT 1 FROM sent_files WHERE path = ?", (path,)) as cur:
        return (await cur.fetchone()) is not None


async def mark_sent(db: aiosqlite.Connection, path: str, modified: str) -> None:
    await db.execute("INSERT OR IGNORE INTO sent_files(path, modified) VALUES(?, ?)", (path, modified))
    await db.commit()


async def bootstrap_if_needed(db: aiosqlite.Connection, ydx: YandexDiskClient) -> None:
    """При первом запуске (если включено) отмечает все текущие фото как 'уже отправленные'."""
    boot = await meta_get(db, "bootstrapped")
    if boot == "1":
        return

    if not SKIP_EXISTING_ON_FIRST_RUN:
        await meta_set(db, "bootstrapped", "1")
        return

    print("Первый запуск: пропускаю уже существующие фото (не отправляю старые).")
    async with aiohttp.ClientSession() as session:
        for folder in FOLDERS:
            try:
                files = await ydx.list_images(session, folder)
                for f in files:
                    await mark_sent(db, f["path"], f.get("modified", ""))
            except Exception as e:
                print(f"BOOTSTRAP ERROR for {folder}:", repr(e))

    await meta_set(db, "bootstrapped", "1")
    print("Готово: текущие файлы помечены, дальше будут отправляться только новые.")


async def poll_and_forward(bot: Bot, ydx: YandexDiskClient, db: aiosqlite.Connection):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                for folder in FOLDERS:
                    files = await ydx.list_images(session, folder)

                    for f in files:
                        path = f["path"]
                        modified = f.get("modified", "")

                        if await is_sent(db, path):
                            continue

                        download_url = await ydx.get_download_url(session, path)

                        date_str = format_date(modified)
                        caption = (
                            f"📸 {f.get('name', '')}\n"
                            f"📁 {folder}\n"
                            f"📅 {date_str}"
                        )

                        await bot.send_photo(chat_id=TG_CHAT_ID, photo=download_url, caption=caption)

                        await mark_sent(db, path, modified)
                        await asyncio.sleep(SEND_DELAY_SEC)

            except Exception as e:
                print("ERROR:", repr(e))

            await asyncio.sleep(POLL_INTERVAL)
            

async def main():
    bot = Bot(token=TG_BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(F.text == "/id")
    async def cmd_id(message: Message):
        await message.answer(f"chat_id = {message.chat.id}")

    db = await init_db(DB_PATH)
    ydx = YandexDiskClient(YANDEX_TOKEN)

    await bootstrap_if_needed(db, ydx)

    asyncio.create_task(poll_and_forward(bot, ydx, db))

    print("БОТ ЗАПУЩЕН ✅")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
