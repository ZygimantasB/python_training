import asyncio
import os
import re

from pathlib import Path

from decouple import config
from telethon import TelegramClient, functions
from telethon.errors.rpcerrorlist import FloodWaitError

api_id = config("telegram_api_id", cast=int)
api_hash = config("telegram_api_hash")

session_name = os.getenv("TELEGRAM_SESSION", "session_name")
base_download_dir = Path(os.getenv("TELEGRAM_DOWNLOAD_DIR", "telegram_download")).resolve()
include_groups = os.getenv("TELEGRAM_INCLUDE_GROUPS", "0").strip() in {"1", "true", "yes", "on"}

client = TelegramClient(session_name, api_id, api_hash)

def _safe_name(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "unnamed"


async def _load_downloaded_set(folder: Path) -> set[int]:
    idx_file = folder / ".download_ids.txt"
    if not idx_file.exists():
        return set()
    try:
        with idx_file.open("r", encoding='utf-8') as file:
            return {int(line.strip()) for line in file if line.strip().isdigit()}
    except Exception as e:
        return set()

def _append_downloaded_id(folder: Path, mid: int) -> None:
    idx_file = folder / ".download_ids.txt"
    with idx_file.open('a', encoding='utf-8') as file:
        file.write(f"{mid}\n")


async def download_from_dialog(entity, dialog_name: str) -> None:
    channel_dir = base_download_dir / _safe_name(dialog_name)
    channel_dir.mkdir(parents=True, exist_ok=True)

    downloaded = await _load_downloaded_set(channel_dir)

    async def progress(current: int, total: int):
        if total:
            pct = current / total
            print(f"[{dialog_name} Downloaded {current}/{total} bytes ({pct:.2f})%", end='\r')

    async for message in client.iter_messages(entity):
        if not message.file:
            continue
        if message.id in downloaded:
            continue
        try:
            path = await client.download_media(
                message,
                file=str(channel_dir),
                progress_callback=progress,
            )
            if path:
                print()  # newline after progress\r
                _append_downloaded_id(channel_dir, message.id)
        except FloodWaitError as fw:
            # Respect Telegram rate limits
            wait = int(getattr(fw, "seconds", 5))
            await asyncio.sleep(wait)


async def main():
    pass

if __name__ == "__main__":
    asyncio.run(main())

