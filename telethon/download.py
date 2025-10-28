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

client = TelegramClient(session_name, api_id, api_hash)

async def main():
    print()