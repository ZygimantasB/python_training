import logging
import os
import asyncio
import re

import polars as pl

from decouple import config
from telethon import TelegramClient, events
from typing import List, Dict

logger = logging.getLogger(__name__)

class TelegramScrapper:

    def __init__(self, api_id: int, api_hash: str, session_name: str, target_channel: str, download_folder: str) -> None:
        self.client = TelegramClient(session=session_name, api_id=api_id, api_hash=api_hash)
        self.target_channel = target_channel
        self.download_folder = download_folder
        self.pass_regex = r'```\s*([^\s`]+)\s*```'
        self.messages = []

    def _parse_message(self, message, channel_name, channel_id) -> Dict[str, str]:
        pass_match = re.search(self.pass_regex, message.text) if message.text else None

        return {
            'channel_name': channel_name,
            'channel_id': channel_id,
            'message_id': message.id,
            'sender': message.sender.username if message.sender else None,
            'message_text': message.text,
            'message_raw_text': message.raw_text,
            'message_date': message.date.isoformat() if message.date else None,
            'file_name': message.file.name if message.file else None,
            'file_size': message.file.size if message.file else None,
            'pass_match': pass_match.group(1) if pass_match else None,
        }

    async def fetch_messages (self, limit: int=100) -> None:
        async with self.client:
            me = await self.client.get_me()

            async for dialog in self.client.iter_dialogs():
                if dialog.title == self.target_channel:

                    async for message in self.client.iter_messages(dialog.entity, limit=limit):
                        parse_data = self._parse_message(message, dialog.name, dialog.id)
                        self.messages.append(parse_data)
                    return


    def save_to_csv(self, file_name: str = 'telegram_messages_test.csv'):
        save_path = os.path.join(self.download_folder, file_name)
        pl.DataFrame(self.messages).write_csv(save_path)

    async def run(self):
        await self.fetch_messages(limit=100)
        self.save_to_csv()


if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete()

    api_id = config('TELEGRAM_API_ID')
    api_hash = config('TELEGRAM_APP_API_HASH')
    session_name = config('TELEGRAM_SESSION')
    target_channel = config('CHANNEL_NAME')

    scraper = TelegramScrapper(
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        target_channel=target_channel,
        download_folder='telegram_download',
    )

    asyncio.run(scraper.run())

