from dotenv import load_dotenv
load_dotenv()

import os

from decouple import config

from telethon import TelegramClient

api_id = os.getenv('telegram_api_id')
api_hash = os.getenv('telegram_api_hash')

try:
    with TelegramClient('session_name', api_id=api_id, api_hash=api_hash) as client:
        client.loop.run_until_complete(client.connect())

except Exception as e:
    print(f"Error: {e}")
