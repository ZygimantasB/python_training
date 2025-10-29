import asyncio
import os
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

async def _load_download_set(folder: Path) -> set[int]:
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


async def main():
    pass

if __name__ == "__main__":
    asyncio.run(main())
sfasdagsd