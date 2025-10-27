from decouple import config

from telethon import TelegramClient

with TelegramClient('session_name', api_id=config('telegram_api_id'), api_hash=config('telegram_api_hash')) as client:
    client.loop.run_until_complete(client.connect())